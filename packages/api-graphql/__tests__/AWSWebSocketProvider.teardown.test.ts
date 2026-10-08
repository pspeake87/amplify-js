// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * A teardown must not wait for something that cannot arrive, and it must not
 * destroy the only thing that can settle its own wait. Both halves of that are
 * why 45 unsubscribes froze for ever when realtime was down.
 */
import * as constants from '../src/Providers/constants';
import { ConnectionState as CS } from '../src/types/PubSub';

import { replaceConstant } from './helpers';
import {
	ApiListener,
	Harness,
	connectOneSubscription,
	delay,
	lambdaOptions,
	listenApi,
	makeProvider,
	settle,
} from './realtimeHarness';

jest.mock('@aws-amplify/core/internals/aws-client-utils', () => {
	const original = jest.requireActual(
		'@aws-amplify/core/internals/aws-client-utils',
	);

	return {
		...original,
		signRequest: () => ({
			method: 'test',
			headers: { test: 'test' },
			url: new URL('http://example/'),
		}),
	};
});

describe('AWSWebSocketProvider teardown', () => {
	let harness: Harness;
	let api: ApiListener;

	const stopSkips = () => api.of('subscriptionStopSkipped');

	const seed = (id: string, patch: Record<string, unknown>) => {
		harness.map().set(id, {
			observer: { next: jest.fn(), error: jest.fn(), complete: jest.fn() },
			query: 'subscription Op { onCreateThing { id } }',
			variables: {},
			subscriptionState: constants.SUBSCRIPTION_STATUS.PENDING,
			...patch,
		});

		return id;
	};

	/** A live link with one subscription whose start was sent but never acked. */
	const connectWithoutAck = async () => {
		const subscription = harness.provider
			.subscribe(lambdaOptions())
			.subscribe({ next: () => {}, error: () => {} });
		await settle();
		harness.socket.open();
		await settle();
		harness.socket.connectionAck();
		await settle();

		return { subscription, id: harness.socket.startFrames[0].id as string };
	};

	beforeEach(() => {
		Object.defineProperty(constants, 'MAX_DELAY_MS', { value: 100 });
		Object.defineProperty(constants, 'RECONNECT_DELAY', { value: 100_000 });
		Object.defineProperty(constants, 'CONNECT_SOCKET_TIMEOUT', { value: 20 });

		api = listenApi();
		harness = makeProvider();
	});

	afterEach(async () => {
		await harness.provider.close();
		api.stop();
		jest.restoreAllMocks();
	});

	test('unsubscribing while the link is down resolves at once, with reason never-connected', async () => {
		const setTimeoutSpy = jest.spyOn(global, 'setTimeout');
		const id = seed('sub-down', {});
		expect(harness.link()).toBeUndefined();

		await (harness.provider as any)._cleanupSubscription(id, undefined);

		expect(stopSkips()).toHaveLength(1);
		expect(stopSkips()[0].data.reason).toBe('never-connected');
		expect(harness.map().size).toBe(0);
		// No wait was entered at all: with no link there is no possible settler, so
		// a wait here is a hang, not a safety margin.
		expect(
			setTimeoutSpy.mock.calls.some(
				([, ms]) => ms === constants.SUBSCRIPTION_STOP_WAIT_TIMEOUT,
			),
		).toBe(false);
	});

	test('the unsubscribe wait has a deadline when a start is in flight on the live link', async () => {
		await replaceConstant('SUBSCRIPTION_STOP_WAIT_TIMEOUT', 20, async () => {
			const { subscription } = await connectWithoutAck();

			subscription.unsubscribe();
			await delay(40);
		});

		expect(stopSkips()).toHaveLength(1);
		expect(stopSkips()[0].data.reason).toBe('wait-timeout');
		expect(harness.map().size).toBe(0);
	});

	test('the reconnect observer is removed after the teardown', async () => {
		const { subscription } = await connectOneSubscription(harness);
		const monitor = (harness.provider as any).reconnectionMonitor;
		expect(monitor.reconnectObservers).toHaveLength(1);

		subscription.unsubscribe();
		await settle();

		// Pruned through the rxjs teardown, so the array cannot grow by one entry
		// per subscription per DataStore restart.
		expect(monitor.reconnectObservers).toHaveLength(0);
		expect(harness.map().size).toBe(0);
	});

	test('a replay keeps a pending unsubscribe callbacks, so the teardown still settles', async () => {
		const { subscription, id } = await connectWithoutAck();

		// A teardown parks on the start that is in flight.
		subscription.unsubscribe();
		await settle();
		expect(harness.map().get(id).subscriptionReadyCallback).toBeDefined();
		expect(harness.map().get(id).subscriptionFailedCallback).toBeDefined();

		// The link dies and the replay rewrites this entry.
		harness.socket.serverClose();
		await settle();
		(harness.provider as any).reconnectionMonitor._triggerReconnect();
		await settle();

		expect(harness.map().get(id).subscriptionReadyCallback).toBeDefined();
		expect(harness.map().get(id).subscriptionFailedCallback).toBeDefined();

		// The replay's attempt fails, which is what settles the parked teardown.
		await delay(40);
		expect(stopSkips()).toHaveLength(1);
		expect(stopSkips()[0].data.reason).toBe('connect-failed');
		expect(harness.map().size).toBe(0);
	});

	test('the unsubscribe wait keeps the start-ack timer', async () => {
		await connectOneSubscription(harness);
		const ackTimer = setTimeout(() => {}, 100_000);
		const id = seed('sub-pending', {
			startedGen: harness.gen(),
			startAckTimeoutId: ackTimer,
		});

		// Not awaited: the wait is meant to be parked here.
		void (harness.provider as any)._waitForSubscriptionToBeConnected(id);

		// Replacing the entry instead of merging left this timer orphaned, to fire
		// 15 s later against whatever link was current by then.
		expect(harness.map().get(id).startAckTimeoutId).toBe(ackTimer);
		expect(harness.map().get(id).subscriptionReadyCallback).toBeDefined();
		clearTimeout(ackTimer);
	});

	test('_closeSocketIfRequired finishes within the close timeout when a send is still buffered', async () => {
		await replaceConstant('SOCKET_CLOSE_TIMEOUT', 20, async () => {
			await connectOneSubscription(harness);
			const socket = harness.socket;
			socket.bufferedAmount = 1;
			harness.map().clear();

			(harness.provider as any)._closeSocketIfRequired();
			// The link stays live while the buffer drains: nothing has closed yet.
			expect(api.states()[api.states().length - 1]).toBe(CS.Connected);
			expect(harness.link().phase).toBe('live');

			await delay(40);

			// One grace period, then the close happens anyway. Rescheduling for ever
			// while bufferedAmount stayed above zero meant the teardown never
			// completed.
			expect(api.states()[api.states().length - 1]).toBe(CS.Disconnected);
			expect(harness.link()).toBeUndefined();
			expect(socket.closedWith).toEqual([3001]);
		});
	});

	test('a subscription that arrives during the close grace starts on the open socket', async () => {
		await replaceConstant('SOCKET_CLOSE_TIMEOUT', 20, async () => {
			const { subscription } = await connectOneSubscription(harness);
			const socket = harness.socket;
			socket.bufferedAmount = 1;
			subscription.unsubscribe();
			await delay(2);
			(harness.provider as any)._closeSocketIfRequired();
			expect(harness.map().size).toBe(0);

			// The real path, not a map write: a 'closing' phase parked this start
			// in promiseArray, and the abandoned close never released it, so the
			// subscription stayed PENDING on a link the app believed healthy.
			harness.provider.subscribe(lambdaOptions()).subscribe({ error: () => {} });
			await settle();

			expect(harness.promiseArray()).toHaveLength(0);
			expect(socket.startFrames).toHaveLength(2);

			await delay(40);

			// The grace ended with a subscription present: nothing closed.
			expect(harness.link().phase).toBe('live');
			expect(socket.closedWith).toEqual([]);
			expect(api.states()[api.states().length - 1]).toBe(CS.Connected);
		});
	});

	test('a start queued behind an attempt is not sent once its unsubscribe gave up', async () => {
		await replaceConstant('SUBSCRIPTION_STOP_WAIT_TIMEOUT', 20, async () => {
			// Two subscriptions queued behind one attempt whose header stage is
			// slow: the host answers only when the test says so.
			let answerHeaders!: (headers: Record<string, string>) => void;
			const options = lambdaOptions({
				additionalHeaders: () =>
					new Promise<Record<string, string>>(resolve => {
						answerHeaders = resolve;
					}),
			});
			const a = harness.provider.subscribe(options).subscribe({ error: () => {} });
			harness.provider.subscribe(options).subscribe({ error: () => {} });
			await settle();
			expect(harness.map().size).toBe(2);

			// A's teardown runs out of patience while the attempt still runs, and
			// deletes A's entry. B keeps the attempt alive.
			a.unsubscribe();
			await delay(40);
			expect(stopSkips()[0].data.reason).toBe('wait-timeout');
			expect(harness.map().size).toBe(1);

			// The attempt goes live and releases every queued start.
			answerHeaders({ Authorization: 'a-token' });
			await settle();
			harness.socket.open();
			await settle();
			harness.socket.connectionAck();
			await settle();

			// Only B's start is sent. A start for A would be a server-side
			// subscription that no GQL_STOP ever ends.
			const [bId] = [...harness.map().keys()];
			expect(harness.socket.startFrames.map(frame => frame.id)).toEqual([bId]);
		});
	});

	test('close() resolves without waiting for the socket onclose', async () => {
		await connectOneSubscription(harness);
		const socket = harness.socket;
		// A dying socket that never answers.
		socket.closeHangs = true;

		await harness.provider.close();

		expect(socket.closedWith).toEqual([3001]);
		expect(api.states()[api.states().length - 1]).toBe(CS.Disconnected);
		expect(harness.link()).toBeUndefined();
		expect(harness.map().size).toBe(0);
	});
});
