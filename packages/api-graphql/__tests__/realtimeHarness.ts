// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * A small harness for the provider's own invariants: one socket I drive by
 * hand, the `api` Hub events the provider publishes, and the privates the
 * tests call. The FakeWebSocketInterface in helpers.ts orchestrates a whole
 * handshake, which is the wrong tool when the point of a test is a step that
 * never completes.
 */
import { Hub } from '@aws-amplify/core';
import { Reachability } from '@aws-amplify/core/internals/utils';
import { Observable } from 'rxjs';

import { AWSAppSyncRealTimeProvider } from '../src/Providers/AWSAppSyncRealTimeProvider';
import { MESSAGE_TYPES } from '../src/Providers/constants';

export const delay = (ms: number) =>
	new Promise(resolve => {
		setTimeout(resolve, ms);
	});

/** Lets the provider's queued microtasks run. */
export const settle = () => delay(1);

export class TestSocket {
	static CONNECTING = 0;
	static OPEN = 1;
	static CLOSING = 2;
	static CLOSED = 3;

	readyState = 0;
	bufferedAmount = 0;
	sent: any[] = [];
	closedWith: (number | undefined)[] = [];
	closeHangs = false;

	onopen: ((ev: any) => void) | null = null;
	onclose: ((ev: any) => void) | null = null;
	onerror: ((ev: any) => void) | null = null;
	onmessage: ((ev: any) => void) | null = null;

	send(data: string) {
		this.sent.push(JSON.parse(String(data)));
	}

	close(code?: number) {
		this.closedWith.push(code);
		this.readyState = TestSocket.CLOSED;
		if (!this.closeHangs) {
			this.onclose?.({ reason: 'closed' });
		}
	}

	/** The server answers the upgrade. */
	open() {
		this.readyState = TestSocket.OPEN;
		this.onopen?.({});
	}

	serverMessage(message: Record<string, unknown>) {
		this.onmessage?.({ data: JSON.stringify(message) });
	}

	connectionAck(connectionTimeoutMs = 100_000) {
		this.serverMessage({
			type: MESSAGE_TYPES.GQL_CONNECTION_ACK,
			payload: { connectionTimeoutMs },
		});
	}

	connectionError(errorType: string, errorCode: number) {
		this.serverMessage({
			type: MESSAGE_TYPES.GQL_CONNECTION_ERROR,
			payload: { errors: [{ errorType, errorCode }] },
		});
	}

	startAck(id: string) {
		this.serverMessage({
			type: MESSAGE_TYPES.GQL_START_ACK,
			payload: {},
			id,
		});
	}

	serverClose() {
		this.readyState = TestSocket.CLOSED;
		this.onclose?.({ reason: 'server closed' });
	}

	serverError() {
		this.onerror?.({ isTrusted: false });
	}

	get startFrames() {
		return this.sent.filter(frame => frame.type === MESSAGE_TYPES.GQL_START);
	}

	get handlers() {
		return [this.onopen, this.onclose, this.onerror, this.onmessage];
	}
}

export interface ApiEvent {
	event: string;
	data: any;
}

/** Records every `api` Hub event, in order. */
export const listenApi = () => {
	const events: ApiEvent[] = [];
	const stop = Hub.listen('api', (payload: any) => {
		events.push({
			event: payload?.payload?.event,
			data: payload?.payload?.data,
		});
	});

	return {
		events,
		stop,
		of(event: string) {
			return events.filter(e => e.event === event);
		},
		states(): string[] {
			return events
				.filter(e => e.event === 'ConnectionStateChange')
				.map(e => e.data.connectionState);
		},
	};
};

export type ApiListener = ReturnType<typeof listenApi>;

export const lambdaOptions = (
	overrides: Record<string, unknown> = {},
): any => ({
	appSyncGraphqlEndpoint: 'ws://localhost:8080',
	authenticationType: 'none',
	additionalHeaders: { Authorization: 'a-token' },
	query: 'subscription Op { onCreateThing { id } }',
	variables: {},
	...overrides,
});

export const makeProvider = ({ online = true }: { online?: boolean } = {}) => {
	// The network state the provider sees, instead of whatever jsdom reports.
	// It decides whether a failure is a dead end or just an offline device.
	jest.spyOn(Reachability.prototype, 'networkMonitor').mockImplementation(
		() =>
			new Observable<{ online: boolean }>(observer => {
				observer.next({ online });
			}) as any,
	);

	const provider = new AWSAppSyncRealTimeProvider();
	const sockets: TestSocket[] = [];

	jest.spyOn(provider as any, '_getNewWebSocket').mockImplementation(() => {
		const socket = new TestSocket();
		sockets.push(socket);

		return socket as unknown as WebSocket;
	});

	return {
		provider,
		sockets,
		get socket() {
			return sockets[sockets.length - 1];
		},
		link: () => (provider as any).link,
		gen: () => (provider as any).link?.gen as number,
		map: () => (provider as any).subscriptionObserverMap as Map<string, any>,
		promiseArray: () => (provider as any).promiseArray as unknown[],
		socketStatus: () => (provider as any).socketStatus as number,
	};
};

export type Harness = ReturnType<typeof makeProvider>;

/**
 * Takes one attempt all the way to a live link with one subscription acked, so
 * a test can start from a healthy connection.
 */
export const connectOneSubscription = async (harness: Harness) => {
	const errorSpy = jest.fn();
	const subscription = harness.provider
		.subscribe(lambdaOptions())
		.subscribe({ next: () => {}, error: errorSpy });

	await settle();
	harness.socket.open();
	await settle();
	harness.socket.connectionAck();
	await settle();
	const id = harness.socket.startFrames[0]?.id as string;
	harness.socket.startAck(id);
	await settle();

	return { subscription, id, errorSpy };
};
