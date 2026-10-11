// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * Rule R4 of the subscription-ownership design: a start the service refuses
 * with a retryable reason is retried on a fresh link, not dropped for the
 * life of the page. The entry is kept, the link is lost, one episode failure
 * carries the reason, and the replay re-sends the start with its original
 * id. Only a reason no replay can fix (a malformed query, a disabled
 * operation) errors the observable.
 *
 * Rule R7: a host can wait for the teardowns in progress, so a round it
 * starts next never overlaps the previous one on the server.
 */
import * as constants from '../src/Providers/constants';
import {
	MESSAGE_TYPES,
	REALTIME_CONNECTION_FAILURE,
} from '../src/Providers/constants';
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

const AUTH_REFUSAL = {
	errorType: 'UnauthorizedException',
	message: 'You are not authorized to make this call.',
};
const CAP_REFUSAL = {
	errorType: 'MaxSubscriptionsReachedError',
	message: 'Max number of 200 subscriptions reached',
};
const VALIDATION_REFUSAL = {
	errorType: 'BadRequestException',
	errorCode: 400,
	message: "Validation error of type FieldUndefined: Field 'x' is undefined",
};

describe('AWSWebSocketProvider refused starts', () => {
	let harness: Harness;
	let api: ApiListener;

	const failures = () => api.of(REALTIME_CONNECTION_FAILURE);
	const stopSkips = () => api.of('subscriptionStopSkipped');
	const lastState = () => api.states()[api.states().length - 1];

	/** The reconnect round, without waiting out RECONNECT_DELAY. */
	const replay = async () => {
		(harness.provider as any).reconnectionMonitor._triggerReconnect();
		await settle();
	};

	/** The service refuses one start on the current socket. */
	const refuse = (id: string, error: Record<string, unknown>) => {
		harness.socket.serverMessage({
			type: MESSAGE_TYPES.GQL_ERROR,
			id,
			payload: { errors: [error] },
		});
	};

	/**
	 * A live link with one start sent and not yet acked: the state a refusal
	 * answers.
	 */
	const startWithoutAck = async () => {
		const errorSpy = jest.fn();
		const nextSpy = jest.fn();
		const subscription = harness.provider
			.subscribe(lambdaOptions())
			.subscribe({ next: nextSpy, error: errorSpy });
		await settle();
		harness.socket.open();
		await settle();
		harness.socket.connectionAck();
		await settle();
		const id = harness.socket.startFrames[0]?.id as string;

		return { subscription, id, errorSpy, nextSpy };
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

	test('an auth refusal keeps the entry, loses the link, and reports an auth failure', async () => {
		const { id, errorSpy } = await startWithoutAck();
		const socket = harness.socket;

		refuse(id, AUTH_REFUSAL);
		await settle();

		expect(errorSpy).not.toHaveBeenCalled();
		expect(harness.map().size).toBe(1);
		expect(harness.map().get(id).subscriptionState).toBe(
			constants.SUBSCRIPTION_STATUS.FAILED,
		);
		expect(harness.link()).toBeUndefined();
		expect(socket.closedWith).toHaveLength(1);
		expect(lastState()).toBe(CS.ConnectionDisrupted);

		expect(failures()).toHaveLength(1);
		const failure = failures()[0].data;
		expect(failure.reason).toBe('auth-rejected');
		expect(failure.isAuthFailure).toBe(true);
		expect(failure.diagnostics.phase).toBe('live');
		expect(failure.diagnostics.consecutiveFailures).toBe(1);
	});

	test('the replay after a refusal re-sends the start with its original id on a new socket, and the ack connects it', async () => {
		const { id, errorSpy, nextSpy } = await startWithoutAck();
		refuse(id, AUTH_REFUSAL);
		await settle();

		await replay();
		expect(harness.sockets).toHaveLength(2);
		harness.socket.open();
		await settle();
		harness.socket.connectionAck();
		await settle();
		expect(harness.socket.startFrames.map(frame => frame.id)).toEqual([id]);

		harness.socket.startAck(id);
		await settle();
		expect(harness.map().get(id).subscriptionState).toBe(
			constants.SUBSCRIPTION_STATUS.CONNECTED,
		);
		expect(lastState()).toBe(CS.Connected);
		expect(errorSpy).not.toHaveBeenCalled();

		// The subscription delivers on the new link.
		harness.socket.serverMessage({
			type: MESSAGE_TYPES.DATA,
			id,
			payload: { data: { onCreateThing: { id: '1' } } },
		});
		expect(nextSpy).toHaveBeenCalledTimes(1);
	});

	test('the cap error, sent as one object rather than a list, is a retryable refusal with reason start-refused', async () => {
		const { id, errorSpy } = await startWithoutAck();

		harness.socket.serverMessage({
			type: MESSAGE_TYPES.GQL_ERROR,
			id,
			payload: { errors: CAP_REFUSAL },
		});
		await settle();

		expect(errorSpy).not.toHaveBeenCalled();
		expect(harness.map().size).toBe(1);
		expect(harness.link()).toBeUndefined();
		expect(failures()).toHaveLength(1);
		expect(failures()[0].data.reason).toBe('start-refused');
		expect(failures()[0].data.isAuthFailure).toBe(false);
		expect(failures()[0].data.message).toContain(
			'MaxSubscriptionsReachedError',
		);
	});

	test('a refusal with no error type at all is retryable', async () => {
		const { id, errorSpy } = await startWithoutAck();

		harness.socket.serverMessage({
			type: MESSAGE_TYPES.GQL_ERROR,
			id,
			payload: { errors: [{ message: 'something went wrong' }] },
		});
		await settle();

		expect(errorSpy).not.toHaveBeenCalled();
		expect(harness.map().size).toBe(1);
		expect(harness.link()).toBeUndefined();
		expect(failures()[0].data.reason).toBe('start-refused');
	});

	test.each([
		['BadRequestException with code 400', VALIDATION_REFUSAL],
		['ValidationError', { errorType: 'ValidationError', message: 'bad' }],
		['OperationDisabled', { errorType: 'OperationDisabled', message: 'off' }],
		[
			'a validation message with no type',
			{ message: 'Validation error of type UnknownType' },
		],
	])(
		'a non-retryable refusal (%s) errors the observable, removes the entry, and keeps the link',
		async (_name, error) => {
			const { id, errorSpy } = await startWithoutAck();
			const socket = harness.socket;

			refuse(id, error);
			await settle();

			expect(errorSpy).toHaveBeenCalledTimes(1);
			expect(errorSpy.mock.calls[0][0].errors[0].message).toContain(
				'Connection failed',
			);
			// rxjs ran the teardown: the entry is gone, and no GQL_STOP was sent
			// for a start the server never registered.
			expect(harness.map().size).toBe(0);
			expect(
				socket.sent.filter(f => f.type === MESSAGE_TYPES.GQL_STOP),
			).toHaveLength(0);
			expect(harness.link()?.phase).toBe('live');
			expect(failures()).toHaveLength(0);
		},
	);

	test('a refusal while an unsubscribe waits for the ack settles the wait, and no GQL_STOP is sent', async () => {
		await replaceConstant(
			'SUBSCRIPTION_STOP_WAIT_TIMEOUT',
			10_000,
			async () => {
				const { id, subscription, errorSpy } = await startWithoutAck();
				const socket = harness.socket;

				subscription.unsubscribe();
				await settle();
				expect(harness.map().size).toBe(1); // parked on the ack

				refuse(id, AUTH_REFUSAL);
				await settle();

				expect(harness.map().size).toBe(0);
				expect(
					socket.sent.filter(f => f.type === MESSAGE_TYPES.GQL_STOP),
				).toHaveLength(0);
				expect(stopSkips()).toHaveLength(1);
				expect(stopSkips()[0].data.reason).toBe('connect-failed');
				expect(errorSpy).not.toHaveBeenCalled();
			},
		);
	});

	test('one episode failure per link loss, however many refusals the round had', async () => {
		const errorSpy = jest.fn();
		harness.provider.subscribe(lambdaOptions()).subscribe({ error: errorSpy });
		harness.provider.subscribe(lambdaOptions()).subscribe({ error: errorSpy });
		await settle();
		harness.socket.open();
		await settle();
		harness.socket.connectionAck();
		await settle();
		const [a, b] = harness.socket.startFrames.map(frame => frame.id as string);
		const socket = harness.socket;

		// The test socket keeps delivering after close(); a browser socket does
		// not. Either way the second refusal must change nothing.
		refuse(a, AUTH_REFUSAL);
		refuse(b, AUTH_REFUSAL);
		await settle();

		expect(failures()).toHaveLength(1);
		expect(socket.closedWith).toHaveLength(1);
		expect(harness.map().size).toBe(2);
		expect(errorSpy).not.toHaveBeenCalled();
	});

	test('a refusal for a start of an earlier link is not a refusal of this link', async () => {
		const { id } = await startWithoutAck();
		refuse(id, AUTH_REFUSAL);
		await settle();
		await replay();
		harness.socket.open();
		await settle();
		harness.socket.connectionAck();
		await settle();
		const failuresBefore = failures().length;

		// A late frame from the first socket: discarded by the generation guard.
		harness.sockets[0].serverMessage({
			type: MESSAGE_TYPES.GQL_ERROR,
			id,
			payload: { errors: [AUTH_REFUSAL] },
		});
		await settle();

		expect(failures()).toHaveLength(failuresBefore);
		expect(harness.link()?.phase).toBe('live');
		expect(harness.map().get(id).subscriptionState).toBe(
			constants.SUBSCRIPTION_STATUS.PENDING,
		);
	});

	test('an error on a CONNECTED subscription still errors the observable (the service ended it)', async () => {
		const { id, errorSpy } = await connectOneSubscription(harness);

		refuse(id, { errorType: 'SomethingEnded', message: 'ended' });
		await settle();

		expect(errorSpy).toHaveBeenCalledTimes(1);
		expect(harness.map().size).toBe(0);
		expect(failures()).toHaveLength(0);
	});

	test('a working ack after a refusal episode closes the episode with realtimeRecovered once it was reported', async () => {
		let now = 1_000_000;
		jest.spyOn(Date, 'now').mockImplementation(() => now);
		const { id } = await startWithoutAck();
		refuse(id, AUTH_REFUSAL);
		await settle();

		// The attempt about 65 s later: old enough to report.
		now += 65_000;
		await replay();
		harness.socket.open();
		await settle();
		harness.socket.connectionAck();
		await settle();
		refuse(id, AUTH_REFUSAL);
		await settle();
		expect(failures()).toHaveLength(2);
		expect(failures()[1].data.report).toBe('open');
		expect(failures()[1].data.isAuthFailure).toBe(true);

		// The service accepts the token again.
		await replay();
		harness.socket.open();
		await settle();
		harness.socket.connectionAck();
		await settle();
		harness.socket.startAck(id);
		await settle();
		expect(api.of('realtimeRecovered')).toHaveLength(1);
		expect(lastState()).toBe(CS.Connected);
	});

	describe('settleTeardowns', () => {
		test('resolves at once when nothing is being torn down', async () => {
			await connectOneSubscription(harness);
			const started = Date.now();
			await harness.provider.settleTeardowns();
			expect(Date.now() - started).toBeLessThan(50);
		});

		test('resolves after the teardown in progress has sent its GQL_STOP', async () => {
			const { subscription, id } = await connectOneSubscription(harness);
			const socket = harness.socket;

			subscription.unsubscribe();
			await harness.provider.settleTeardowns();

			expect(
				socket.sent
					.filter(f => f.type === MESSAGE_TYPES.GQL_STOP)
					.map(f => f.id),
			).toEqual([id]);
			expect(harness.map().size).toBe(0);
		});

		test('resolves only when a parked teardown gives up its ack wait', async () => {
			await replaceConstant('SUBSCRIPTION_STOP_WAIT_TIMEOUT', 30, async () => {
				const { subscription } = await startWithoutAck();
				subscription.unsubscribe();
				await settle();

				let settled = false;
				const settling = harness.provider.settleTeardowns().then(() => {
					settled = true;
				});
				await delay(10);
				expect(settled).toBe(false);
				await settling;
				expect(settled).toBe(true);
				expect(harness.map().size).toBe(0);
				expect(stopSkips()[0].data.reason).toBe('wait-timeout');
			});
		});
	});
});
