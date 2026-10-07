// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
import { Hub } from '@aws-amplify/core';

import { AWSAppSyncRealTimeProvider } from '../src/Providers/AWSAppSyncRealTimeProvider';
import {
	SOCKET_STATUS,
	SUBSCRIPTION_STATUS,
} from '../src/Providers/constants';

/**
 * Instrumentation for orphaned server-side subscriptions
 * (MaxSubscriptionsReachedError): every code path that deletes local
 * subscription bookkeeping WITHOUT sending GQL_STOP must dispatch a
 * `subscriptionStopSkipped` Hub event on the `api` channel, so the app can
 * tell which path fires and whether the socket was really open. These tests
 * pin the dispatch — the skip behavior itself is unchanged.
 *
 * The reasons are 'never-connected', 'connect-failed', 'socket-not-ready',
 * 'send-failed' and 'wait-timeout'. The last one is pinned in
 * AWSWebSocketProvider.teardown.test.ts, beside the deadline that raises it.
 */
describe('subscriptionStopSkipped instrumentation', () => {
	let provider: AWSAppSyncRealTimeProvider;
	let dispatchSpy: jest.SpyInstance;

	const seedSubscription = (
		subscriptionState: SUBSCRIPTION_STATUS,
		id = 'sub-id-1',
	) => {
		(provider as any).subscriptionObserverMap.set(id, {
			observer: { next: jest.fn(), error: jest.fn(), complete: jest.fn() },
			query: 'subscription Op { onUpdateCattle { id } }',
			variables: {},
			subscriptionState,
		});

		return id;
	};

	const dispatchedEvents = () =>
		dispatchSpy.mock.calls
			.map(([channel, payload]) => ({ channel, payload }))
			.filter(({ payload }) => payload?.event === 'subscriptionStopSkipped');

	beforeEach(() => {
		provider = new AWSAppSyncRealTimeProvider();
		dispatchSpy = jest.spyOn(Hub, 'dispatch').mockImplementation(() => {});
	});

	afterEach(() => {
		dispatchSpy.mockRestore();
	});

	test('cleanup of a never-connected subscription dispatches never-connected', async () => {
		const id = seedSubscription(SUBSCRIPTION_STATUS.FAILED);

		await (provider as any)._cleanupSubscription(id, undefined);

		const events = dispatchedEvents();
		expect(events).toHaveLength(1);
		expect(events[0].channel).toBe('api');
		expect(events[0].payload.data).toMatchObject({
			reason: 'never-connected',
			subscriptionState: SUBSCRIPTION_STATUS.FAILED,
			queryName: 'onUpdateCattle',
		});
		// Behavior unchanged: local bookkeeping is still removed
		expect((provider as any).subscriptionObserverMap.size).toBe(0);
	});

	test('cleanup of a PENDING subscription with no link is never-connected too', async () => {
		const id = seedSubscription(SUBSCRIPTION_STATUS.PENDING);

		await (provider as any)._cleanupSubscription(id, undefined);

		// While realtime is down EVERY entry is PENDING. Waiting for an ack that
		// no link can send is what froze a DataStore.clear() for hours.
		const events = dispatchedEvents();
		expect(events).toHaveLength(1);
		expect(events[0].payload.data).toMatchObject({
			reason: 'never-connected',
			subscriptionState: SUBSCRIPTION_STATUS.PENDING,
		});
		expect((provider as any).subscriptionObserverMap.size).toBe(0);
	});

	test('unsubscribing a CONNECTED subscription without a ready socket dispatches socket-not-ready', () => {
		const id = seedSubscription(SUBSCRIPTION_STATUS.CONNECTED);
		(provider as any).socketStatus = SOCKET_STATUS.CLOSED;
		(provider as any).awsRealTimeSocket = undefined;

		(provider as any)._sendUnsubscriptionMessage(id);

		const events = dispatchedEvents();
		expect(events).toHaveLength(1);
		expect(events[0].payload.data).toMatchObject({
			reason: 'socket-not-ready',
			socketStatus: SOCKET_STATUS.CLOSED,
			remainingSubscriptions: 1,
		});
		// The undefined socket is recorded as an absent readyState
		expect(events[0].payload.data.readyState).toBeUndefined();
	});

	test('no event is dispatched when GQL_STOP is actually sent', () => {
		const id = seedSubscription(SUBSCRIPTION_STATUS.CONNECTED);
		const send = jest.fn();
		(provider as any).socketStatus = SOCKET_STATUS.READY;
		(provider as any).awsRealTimeSocket = {
			readyState: WebSocket.OPEN,
			send,
		};

		(provider as any)._sendUnsubscriptionMessage(id);

		expect(send).toHaveBeenCalledTimes(1);
		expect(dispatchedEvents()).toHaveLength(0);
	});
});
