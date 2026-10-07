// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * One rule: a socket, a timer or a server message may touch shared state only
 * while it belongs to the current link. Everything else is a no-op.
 *
 * And one state is reserved: CONNECTION_FAILED halts reconnection and disables
 * network monitoring, so only a deliberate shutdown may record it.
 */
import * as constants from '../src/Providers/constants';
import { MESSAGE_TYPES, REALTIME_CONNECTION_FAILURE } from '../src/Providers/constants';
import { CONNECTION_CHANGE } from '../src/utils/ConnectionStateMonitor';
import { ConnectionState as CS } from '../src/types/PubSub';

import { replaceConstant } from './helpers';
import {
	ApiListener,
	Harness,
	TestSocket,
	connectOneSubscription,
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

describe('AWSWebSocketProvider link generations', () => {
	let harness: Harness;
	let api: ApiListener;

	/**
	 * Starts one attempt and keeps its outcome handled, so a test can abandon it
	 * without jest seeing an unhandled rejection.
	 */
	const startAttempt = (): Promise<{ value?: any; error?: any }> =>
		((harness.provider as any).startAttempt(lambdaOptions()) as Promise<any>)
			.then(
				value => ({ value }),
				error => ({ error }),
			);

	/**
	 * One attempt with an open socket that is never acked. The attempt promise is
	 * handed back wrapped, so awaiting this does not await the attempt.
	 */
	const openWithoutAck = async () => {
		const attempt = startAttempt();
		await settle();
		harness.socket.open();
		await settle();

		return { attempt };
	};

	/** The reconnect round, without waiting out RECONNECT_DELAY. */
	const replay = async () => {
		(harness.provider as any).reconnectionMonitor._triggerReconnect();
		await settle();
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

	test('a discarded attempt keeps no armed deadline, and its late failure records nothing', async () => {
		await openWithoutAck();
		const linkA = harness.link();
		expect(linkA.phase).toBe('handshake');
		expect(linkA.phaseTimer).toBeDefined();

		// Only a settlement can abandon an attempt, and it disarms the attempt's
		// own deadline on the way out.
		(harness.provider as any).failAttempt(linkA.gen, new Error('superseded'));
		expect(linkA.phaseTimer).toBeUndefined();
		expect(linkA.budgetTimer).toBeUndefined();

		// Attempt B becomes the live link.
		await connectOneSubscription(harness);
		expect(api.states()[api.states().length - 1]).toBe(CS.Connected);

		const statesBefore = api.states().length;
		const failuresBefore = api.of(REALTIME_CONNECTION_FAILURE).length;

		// A's deadline, had it still been armed, would land here.
		(harness.provider as any).failAttempt(
			linkA.gen,
			new Error('A handshake deadline'),
		);

		expect(api.states()).toHaveLength(statesBefore);
		expect(api.of(REALTIME_CONNECTION_FAILURE)).toHaveLength(failuresBefore);
		expect(harness.link().phase).toBe('live');
	});

	test('CONNECTION_FAILED is recorded only by close()', async () => {
		const record = jest.spyOn(
			(harness.provider as any).connectionStateMonitor,
			'record',
		);
		const recordedFailed = () =>
			record.mock.calls.filter(
				([change]) => change === CONNECTION_CHANGE.CONNECTION_FAILED,
			);

		// the open deadline
		await expect(
			(harness.provider as any).startAttempt(lambdaOptions()),
		).rejects.toBeDefined();

		// the handshake deadline
		await replaceConstant('CONNECTION_INIT_TIMEOUT', 20, async () => {
			const { attempt } = await openWithoutAck();
			expect((await attempt).error).toBeDefined();
		});

		// a live link that stops answering, and a start that is never acked
		const { id } = await connectOneSubscription(harness);
		const liveGen = harness.gen();
		(harness.provider as any).keepAliveTimestamp = 0;
		(harness.provider as any).keepAliveHeartbeat(liveGen, 1000);
		await settle();

		await connectOneSubscription(harness);
		(harness.provider as any)._timeoutStartSubscriptionAck(harness.gen(), id);
		await settle();

		expect(recordedFailed()).toHaveLength(0);

		await harness.provider.close();
		expect(recordedFailed()).toHaveLength(1);
	});

	test('a handshake deadline on the current attempt publishes ConnectionDisrupted, not Disconnected', async () => {
		const before = api.states().length;

		await replaceConstant('CONNECTION_INIT_TIMEOUT', 20, async () => {
			const { attempt } = await openWithoutAck();
			expect((await attempt).error).toBeDefined();
		});

		const states = api.states().slice(before);
		expect(states).toContain(CS.ConnectionDisrupted);
		// Disconnected would halt the reconnect monitor and disable network
		// monitoring: a missed ack would end realtime for the life of the page.
		expect(states).not.toContain(CS.Disconnected);
		expect(api.of(REALTIME_CONNECTION_FAILURE)[0].data.reason).toBe(
			'handshake-deadline',
		);
	});

	test('a stale socket cannot speak, and its late close leaves the live link alone', async () => {
		await connectOneSubscription(harness);
		const staleSocket = harness.socket;
		const staleOnClose = staleSocket.onclose;
		const staleGen = harness.gen();

		staleSocket.serverClose();
		await settle();
		await replay();
		harness.socket.open();
		await settle();
		harness.socket.connectionAck();
		await settle();
		harness.socket.startAck(harness.socket.startFrames[0].id);
		await settle();

		expect(harness.gen()).not.toBe(staleGen);
		expect(api.states()[api.states().length - 1]).toBe(CS.Connected);

		const statesBefore = api.states().length;
		const keepAlive = (harness.provider as any).keepAliveHeartbeatIntervalId;
		expect(keepAlive).toBeDefined();

		// The discarded socket has no handlers at all.
		expect(staleSocket.handlers).toEqual([null, null, null, null]);
		// And even the handler it had records nothing.
		staleOnClose?.({ reason: 'late close' });

		expect(api.states()).toHaveLength(statesBefore);
		expect((harness.provider as any).keepAliveHeartbeatIntervalId).toBe(
			keepAlive,
		);
		expect(harness.socket.closedWith).toEqual([]);
		expect(harness.link().phase).toBe('live');
	});

	test('a stale keep-alive tick records nothing', async () => {
		await connectOneSubscription(harness);
		const statesBefore = api.states().length;

		(harness.provider as any).keepAliveTimestamp = 0;
		(harness.provider as any).keepAliveHeartbeat(harness.gen() - 1, 1000);
		await settle();

		expect(api.states()).toHaveLength(statesBefore);
		expect(harness.link().phase).toBe('live');
	});

	test('onopen of a stale attempt does not become the live socket', async () => {
		startAttempt();
		await settle();
		const socketA = harness.socket;
		const onOpenA = socketA.onopen;
		const genA = harness.gen();

		(harness.provider as any).failAttempt(genA, new Error('superseded'));

		const attemptB = startAttempt();
		await settle();
		const socketB = harness.socket;
		expect(socketB).not.toBe(socketA);

		// A's socket answers late.
		socketA.readyState = TestSocket.OPEN;
		onOpenA?.({});

		expect((harness.provider as any).awsRealTimeSocket).not.toBe(socketA);
		expect(socketA.closedWith.length).toBeGreaterThan(0);

		socketB.open();
		await settle();
		socketB.connectionAck();
		await attemptB;
		expect((harness.provider as any).awsRealTimeSocket).toBe(socketB);
	});

	test('a replay does not re-send a start for a subscription the live link already started', async () => {
		await connectOneSubscription(harness);
		const socket = harness.socket;
		expect(socket.startFrames).toHaveLength(1);

		await replay();
		await settle();

		// AppSync scopes subscription ids per connection and answers a repeat with
		// DuplicatedOperationError.
		expect(socket.startFrames).toHaveLength(1);
		expect(harness.sockets).toHaveLength(1);
	});

	test('a replay does not reset a CONNECTED entry to PENDING', async () => {
		const { id } = await connectOneSubscription(harness);
		expect(harness.map().get(id).subscriptionState).toBe(
			constants.SUBSCRIPTION_STATUS.CONNECTED,
		);

		await replay();
		await settle();

		// A reset would strand the entry: PENDING with no ack timer and no start
		// in flight, which also freezes any unsubscribe that waits on it.
		expect(harness.map().get(id).subscriptionState).toBe(
			constants.SUBSCRIPTION_STATUS.CONNECTED,
		);
		expect(harness.map().get(id).startedGen).toBe(harness.gen());
	});

	test('a stale start-ack timer does not lose the live link', async () => {
		const { id } = await connectOneSubscription(harness);
		const statesBefore = api.states().length;

		(harness.provider as any)._timeoutStartSubscriptionAck(
			harness.gen() - 1,
			id,
		);
		await settle();

		expect(api.states()).toHaveLength(statesBefore);
		expect(harness.link().phase).toBe('live');
		expect(harness.map().get(id).subscriptionState).toBe(
			constants.SUBSCRIPTION_STATUS.CONNECTED,
		);
	});

	test('a start-ack timeout on the live link closes it, so the replay lands on a new socket', async () => {
		const { id } = await connectOneSubscription(harness);
		const socket = harness.socket;

		(harness.provider as any)._timeoutStartSubscriptionAck(harness.gen(), id);
		await settle();

		expect(api.states()[api.states().length - 1]).toBe(CS.ConnectionDisrupted);
		expect(socket.closedWith).toEqual([3001]);

		await replay();
		expect(harness.sockets).toHaveLength(2);
		expect(
			harness.sockets[0].sent.filter(
				frame => frame.type === MESSAGE_TYPES.GQL_START,
			),
		).toHaveLength(1);
	});
});
