// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * Invariant I1: every connect attempt ends in a recorded outcome. No
 * failure-prone or unbounded work runs before the attempt is recorded, every
 * wait has a deadline, and the deadline settles the whole chain it owns.
 *
 * Invariant I2: one failure episode, one signal.
 */
import * as constants from '../src/Providers/constants';
import { ConnectionState as CS } from '../src/types/PubSub';
import { REALTIME_CONNECTION_FAILURE } from '../src/Providers/constants';

import { replaceConstant } from './helpers';
import {
	ApiListener,
	Harness,
	connectOneSubscription,
	lambdaOptions,
	listenApi,
	makeProvider,
	settle,
	delay,
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

describe('AWSWebSocketProvider connect attempt', () => {
	let harness: Harness;
	let api: ApiListener;

	const failures = () => api.of(REALTIME_CONNECTION_FAILURE);
	const lastFailure = () => failures()[failures().length - 1]?.data;

	/** One attempt whose socket never opens, settled by the open deadline. */
	const failOneAttempt = async (options = lambdaOptions()) => {
		await expect(
			(harness.provider as any).startAttempt(options),
		).rejects.toBeDefined();
	};

	beforeEach(() => {
		// One connect try per attempt: jitteredBackoff refuses a delay above the
		// maximum, so the first failure is final.
		Object.defineProperty(constants, 'MAX_DELAY_MS', { value: 100 });
		// Nothing in this file wants a reconnect replay in the middle of a test.
		Object.defineProperty(constants, 'RECONNECT_DELAY', { value: 100_000 });
		// A socket that is never answered must not hold the test for 15 s.
		Object.defineProperty(constants, 'CONNECT_SOCKET_TIMEOUT', { value: 20 });

		api = listenApi();
		harness = makeProvider();
	});

	afterEach(async () => {
		await harness.provider.close();
		api.stop();
		jest.restoreAllMocks();
	});

	test('records OPENING_CONNECTION before the header stage', async () => {
		let statesWhenTokenRead: string[] = [];

		await expect(
			(harness.provider as any).startAttempt(
				lambdaOptions({
					additionalHeaders: async () => {
						statesWhenTokenRead = api.states();
						throw new Error('the host cannot produce a token');
					},
				}),
			),
		).rejects.toBeDefined();

		// The attempt was already visible when the work that failed began. This
		// ordering is the whole of the silent token wedge: anything that fails
		// above the record cannot be told apart from "no attempt was made".
		expect(statesWhenTokenRead[statesWhenTokenRead.length - 1]).toBe(
			CS.Connecting,
		);
		expect(api.states()).toContain(CS.ConnectionDisrupted);
		expect(failures()).toHaveLength(1);
	});

	test('the header stage has its own deadline', async () => {
		await replaceConstant('HEADER_RESOLVE_TIMEOUT', 20, async () => {
			await expect(
				(harness.provider as any).startAttempt(
					lambdaOptions({
						// A host with no limit of its own.
						additionalHeaders: () => new Promise(() => {}),
					}),
				),
			).rejects.toBeDefined();
		});

		expect(lastFailure().reason).toBe('token-timeout');
		expect(harness.socketStatus()).toBe(constants.SOCKET_STATUS.CLOSED);
		expect(harness.promiseArray()).toHaveLength(0);
	});

	test('one header resolution per attempt, shared by every waiter', async () => {
		let tokenReads = 0;
		const options = lambdaOptions({
			additionalHeaders: async () => {
				tokenReads += 1;

				return { Authorization: 'a-token' };
			},
		});

		for (let i = 0; i < 45; i++) {
			harness.provider.subscribe(options).subscribe({ error: () => {} });
		}
		await delay(40);

		// 45 subscriptions, one attempt, one token read.
		expect(tokenReads).toBe(1);
		expect(harness.sockets).toHaveLength(1);
	});

	test('a second subscribe during an in-flight attempt never starts a second attempt', async () => {
		harness.provider.subscribe(lambdaOptions()).subscribe({ error: () => {} });
		harness.provider.subscribe(lambdaOptions()).subscribe({ error: () => {} });
		await settle();

		expect(harness.sockets).toHaveLength(1);
		expect(harness.promiseArray()).toHaveLength(1);
	});

	test('a socket that never opens fails the attempt at the deadline and closes that socket', async () => {
		await failOneAttempt();

		expect(lastFailure().reason).toBe('connect-deadline');
		expect(lastFailure().diagnostics.socketsAttempted).toBe(0);
		// The socket is claimed synchronously, so the deadline can really close it.
		expect(harness.socket.closedWith).toEqual([3001]);
		expect(harness.socket.handlers).toEqual([null, null, null, null]);
		expect(
			harness.sockets.filter(socket => socket.readyState === 0),
		).toHaveLength(0);
	});

	test('the connect rejection drains promiseArray and releases every start latch', async () => {
		for (let i = 0; i < 45; i++) {
			harness.provider.subscribe(lambdaOptions()).subscribe({ error: () => {} });
		}
		await settle();
		expect(harness.promiseArray()).toHaveLength(44);

		await delay(40);

		// Every waiter was settled, so every start resumed and recorded its
		// outcome. A parked waiter would hold its subscription's start latch for
		// ever and the next replay round would be a no-op.
		expect(harness.promiseArray()).toHaveLength(0);
		expect(harness.map().size).toBe(45);
		[...harness.map().values()].forEach(entry => {
			expect(entry.subscriptionState).toBe(constants.SUBSCRIPTION_STATUS.FAILED);
		});
	});

	test('the attempt budget settles the chain when a phase deadline is missed', async () => {
		// A phase with no deadline of its own: only the backstop can end this.
		jest
			.spyOn(harness.provider as any, 'withPhaseDeadline')
			.mockImplementation((...args: any[]) => (args[3] as () => Promise<any>)());

		await replaceConstant('CONNECT_ATTEMPT_BUDGET', 30, async () => {
			await expect(
				(harness.provider as any).startAttempt(
					lambdaOptions({ additionalHeaders: () => new Promise(() => {}) }),
				),
			).rejects.toBeDefined();
		});

		expect(lastFailure().reason).toBe('attempt-budget');
		expect(harness.link()).toBeUndefined();
	});

	test('an auth or transient failure does not call observer.error', async () => {
		// no-auth-token
		const noToken = jest.fn();
		harness.provider
			.subscribe(lambdaOptions({ additionalHeaders: { Authorization: '' } }))
			.subscribe({ error: noToken });
		await settle();
		expect(lastFailure().reason).toBe('no-auth-token');
		expect(noToken).not.toHaveBeenCalled();

		// token-timeout
		const timedOut = jest.fn();
		await replaceConstant('HEADER_RESOLVE_TIMEOUT', 20, async () => {
			harness.provider
				.subscribe(
					lambdaOptions({ additionalHeaders: () => new Promise(() => {}) }),
				)
				.subscribe({ error: timedOut });
			await delay(40);
		});
		expect(lastFailure().reason).toBe('token-timeout');
		expect(timedOut).not.toHaveBeenCalled();

		// auth-rejected
		const rejected = jest.fn();
		harness.provider.subscribe(lambdaOptions()).subscribe({ error: rejected });
		await settle();
		harness.socket.open();
		await settle();
		harness.socket.connectionError('UnauthorizedException', 401);
		await settle();
		expect(lastFailure().reason).toBe('auth-rejected');
		expect(rejected).not.toHaveBeenCalled();
	});

	test('a configuration error does call observer.error', async () => {
		const error = jest.fn();
		harness.provider.subscribe(lambdaOptions()).subscribe({ error });
		await settle();
		harness.socket.open();
		await settle();
		harness.socket.connectionError('BadRequestException', 400);
		await settle();

		expect(lastFailure().reason).toBe('config-error');
		// No replay can fix a malformed query, so this one is told to the caller.
		expect(error).toHaveBeenCalledTimes(1);
		expect(error.mock.calls[0][0].errors[0].message).toBe(
			'Connection failed: BadRequestException',
		);
	});

	test('the second consecutive failure reports open; the first reports none', async () => {
		await failOneAttempt();
		expect(failures()).toHaveLength(1);
		expect(failures()[0].data.report).toBe('none');
		expect(failures()[0].data.diagnostics.consecutiveFailures).toBe(1);

		await failOneAttempt();
		expect(failures()).toHaveLength(2);
		expect(failures()[1].data.report).toBe('open');
		expect(failures()[1].data.diagnostics.consecutiveFailures).toBe(2);
		expect(failures()[1].data.diagnostics.episodeId).toBe(1);
	});

	test('a repeat report arrives once per repeat interval while the episode continues', async () => {
		let now = 1_000_000;
		jest.spyOn(Date, 'now').mockImplementation(() => now);

		await failOneAttempt();
		await failOneAttempt();
		expect(failures()[1].data.report).toBe('open');

		now += 30 * 60 * 1000;
		await failOneAttempt();
		expect(failures()[2].data.report).toBe('none');

		now += 31 * 60 * 1000;
		await failOneAttempt();
		expect(failures()[3].data.report).toBe('repeat');

		now += 1000;
		await failOneAttempt();
		expect(failures()[4].data.report).toBe('none');
	});

	test('a failure while the network is down is neither counted nor reported', async () => {
		await harness.provider.close();
		harness = makeProvider({ online: false });

		await failOneAttempt();
		await failOneAttempt();

		expect(api.states()).toContain(CS.ConnectionDisruptedPendingNetwork);
		expect(failures()).toHaveLength(2);
		failures().forEach(failure => {
			expect(failure.data.report).toBe('none');
			expect(failure.data.diagnostics.consecutiveFailures).toBe(0);
		});
	});

	test('a working connection clears the episode and dispatches realtimeRecovered once', async () => {
		await failOneAttempt();
		await failOneAttempt();
		expect(failures()[1].data.report).toBe('open');

		const { id } = await connectOneSubscription(harness);
		expect(api.states()).toContain(CS.Connected);
		expect(api.of(constants.REALTIME_RECOVERED)).toHaveLength(1);

		// A second ack on the same connection is not a second recovery.
		harness.socket.startAck(id);
		await settle();
		expect(api.of(constants.REALTIME_RECOVERED)).toHaveLength(1);
		expect((harness.provider as any).consecutiveFailures).toBe(0);
		expect((harness.provider as any).episodeStartedAt).toBe(0);
	});

	test('realtimeRecovered is not dispatched when the episode was never reported', async () => {
		await failOneAttempt();
		expect(failures()[0].data.report).toBe('none');

		await connectOneSubscription(harness);

		expect(api.states()).toContain(CS.Connected);
		expect(api.of(constants.REALTIME_RECOVERED)).toHaveLength(0);
	});

	test('one round with 45 subscriptions dispatches one realtimeConnectionFailure', async () => {
		for (let i = 0; i < 45; i++) {
			harness.provider.subscribe(lambdaOptions()).subscribe({ error: () => {} });
		}
		await delay(40);

		expect(failures()).toHaveLength(1);
		expect(failures()[0].data.diagnostics.subscriptions).toBe(45);
	});

	test('a close settles the step the attempt was waiting on, and is not a failure', async () => {
		// The host's token read has no limit of its own here, and close() disarms
		// the deadline that would have ended it, so the release has to settle it.
		const attempt = (harness.provider as any).startAttempt(
			lambdaOptions({ additionalHeaders: () => new Promise(() => {}) }),
		);
		await settle();

		await harness.provider.close();

		await expect(attempt).rejects.toBeDefined();
		// A deliberate shutdown is not a failure episode.
		expect(failures()).toHaveLength(0);
	});

	test('a patch for a removed subscription does not resurrect it', async () => {
		const observer = {
			next: jest.fn(),
			error: jest.fn(),
			complete: jest.fn(),
		} as any;

		// Work in flight outlives an unsubscribe. A resurrected entry would hold
		// the socket open with no observer behind it.
		(harness.provider as any)._failStartSubscription(
			'already-unsubscribed',
			observer,
			new Error('too late'),
		);

		expect(harness.map().size).toBe(0);
		expect(observer.error).not.toHaveBeenCalled();
	});

	test('the failure message matches DataStore subscriptionErrorMap.Unauthorized', async () => {
		harness.provider
			.subscribe(lambdaOptions({ additionalHeaders: { Authorization: '' } }))
			.subscribe({ error: () => {} });
		await settle();

		// The host contract: anything that classifies a realtime failure by
		// message instead of by errorType must get the same answer.
		expect(/Connection failed.+Unauthorized/.test(lastFailure().message)).toBe(
			true,
		);
		expect(lastFailure().isAuthFailure).toBe(true);
	});
});
