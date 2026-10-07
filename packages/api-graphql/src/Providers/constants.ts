// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
export { AMPLIFY_SYMBOL } from '@aws-amplify/core/internals/utils';

export const MAX_DELAY_MS = 5000;

export const NON_RETRYABLE_CODES = [400, 401, 403];
export const NON_RETRYABLE_ERROR_TYPES = [
	'BadRequestException',
	'UnauthorizedException',
];

export const CONNECTION_STATE_CHANGE = 'ConnectionStateChange';

export enum MESSAGE_TYPES {
	/**
	 * Client -> Server message.
	 * This message type is the first message after handshake and this will initialize AWS AppSync RealTime communication
	 */
	GQL_CONNECTION_INIT = 'connection_init',
	/**
	 * Server -> Client message
	 * This message type is in case there is an issue with AWS AppSync RealTime when establishing connection
	 */
	GQL_CONNECTION_ERROR = 'connection_error',
	/**
	 * Server -> Client message.
	 * This message type is for the ack response from AWS AppSync RealTime for GQL_CONNECTION_INIT message
	 */
	GQL_CONNECTION_ACK = 'connection_ack',
	/**
	 * Client -> Server message.
	 * This message type is for register subscriptions with AWS AppSync RealTime
	 */
	GQL_START = 'start',
	/**
	 * Server -> Client message.
	 * This message type is for the ack response from AWS AppSync RealTime for GQL_START message
	 */
	GQL_START_ACK = 'start_ack',
	/**
	 * Server -> Client message.
	 * This message type is for subscription message from AWS AppSync RealTime or Events
	 */
	DATA = 'data',
	/**
	 * Server -> Client message.
	 * This message type helps the client to know is still receiving messages from AWS AppSync RealTime
	 */
	GQL_CONNECTION_KEEP_ALIVE = 'ka',
	/**
	 * Client -> Server message.
	 * This message type is for unregister subscriptions with AWS AppSync RealTime
	 */
	GQL_STOP = 'stop',
	/**
	 * Server -> Client message.
	 * This message type is for the ack response from AWS AppSync RealTime for GQL_STOP message
	 */
	GQL_COMPLETE = 'complete',
	/**
	 * Server -> Client message.
	 * This message type is for sending error messages from AWS AppSync RealTime to the client
	 */
	GQL_ERROR = 'error', // Server -> Client
	/**
	 * Client -> Server message.
	 * This message type is for registering subscriptions with Events
	 */
	EVENT_SUBSCRIBE = 'subscribe',
	/**
	 * Client -> Server message.
	 * This message type is for publishing a message with Events
	 */
	EVENT_PUBLISH = 'publish',
	/**
	 * Server -> Client message.
	 * Server acknowledges successful subscription
	 */
	EVENT_SUBSCRIBE_ACK = 'subscribe_success',
	/**
	 * Server -> Client message.
	 * Server acknowledges successful publish
	 */
	EVENT_PUBLISH_ACK = 'publish_success',
	/**
	 * Client -> Server message.
	 * This message type is for unregister subscriptions with AWS AppSync RealTime
	 */
	EVENT_STOP = 'unsubscribe',
	/**
	 * Server -> Client message.
	 * This is the ack response from AWS AppSync Events to EVENT_STOP message
	 */
	EVENT_COMPLETE = 'unsubscribe_success',
	/**
	 * Server -> Client message.
	 * This message type is for sending error messages from AWS AppSync Events to the client
	 */
	EVENT_SUBSCRIBE_ERROR = 'subscribe_error',
}

export enum SUBSCRIPTION_STATUS {
	PENDING,
	CONNECTED,
	FAILED,
}

export enum SOCKET_STATUS {
	CLOSED,
	READY,
	CONNECTING,
}

export const AWS_APPSYNC_REALTIME_HEADERS = {
	accept: 'application/json, text/javascript',
	'content-encoding': 'amz-1.0',
	'content-type': 'application/json; charset=UTF-8',
};

/**
 * Time in milleseconds to wait for GQL_CONNECTION_INIT message
 */
export const CONNECTION_INIT_TIMEOUT = 15000;

/**
 * Time in milleseconds to wait for GQL_START_ACK message
 */
export const START_ACK_TIMEOUT = 15000;

/**
 * Default Time in milleseconds to wait for GQL_CONNECTION_KEEP_ALIVE message
 */
export const DEFAULT_KEEP_ALIVE_TIMEOUT = 5 * 60 * 1000;

/**
 * Default Time in milleseconds between monitoring checks of keep alive status
 */
export const DEFAULT_KEEP_ALIVE_HEARTBEAT_TIMEOUT = 5 * 1000;

/**
 * Default Time in milleseconds to alert for missed GQL_CONNECTION_KEEP_ALIVE message
 */
export const DEFAULT_KEEP_ALIVE_ALERT_TIMEOUT = 65 * 1000;

/**
 * Default delay time in milleseconds between when reconnect is triggered vs when it is attempted
 */
export const RECONNECT_DELAY = 5 * 1000;

/**
 * Default interval time in milleseconds between when reconnect is re-attempted
 */
export const RECONNECT_INTERVAL = 60 * 1000;

/**
 * Longest wait for one WebSocket to reach `onopen`. A TCP path that completes
 * and is then never answered at the HTTP Upgrade produces no event at all — no
 * open, no close, no error — and the browser applies no deadline of its own.
 * Same size as the two sibling steps of the same handshake
 * (CONNECTION_INIT_TIMEOUT, START_ACK_TIMEOUT): one step of an AppSync realtime
 * handshake must answer within 15 s. A healthy open is under a second.
 */
export const CONNECT_SOCKET_TIMEOUT = 15000;

/**
 * Longest wait for the credentials of one attempt: the `authToken` thunk,
 * `additionalHeaders`, and the signing that turns them into the subprotocol.
 * Equal to DataStore's own TOKEN_REQUEST_TIMEOUT_MS, so neither limit fires
 * spuriously, and it bounds a host that has no limit of its own. The library
 * must not trust the host here.
 */
export const HEADER_RESOLVE_TIMEOUT = 30000;

/**
 * Backstop for one whole connect attempt. It should never fire: every phase has
 * its own deadline and a phase deadline is non-retryable, so the worst
 * legitimate chain is about 45 s (the credentials, then one 15 s handshake
 * deadline). One RECONNECT_INTERVAL is the right size for a backstop: if an
 * attempt has not settled by the time its replacement is due, it is stuck, not
 * slow. Settling is done by failAttempt, not by the retry loop's onTerminate —
 * `retry()` cannot interrupt a hung functionToRetry.
 */
export const CONNECT_ATTEMPT_BUDGET = RECONNECT_INTERVAL;

/**
 * A deliberate close must not wait on the socket it is tearing down. A clean
 * close is sub-second; one grace period for a buffered send, then close anyway.
 */
export const SOCKET_CLOSE_TIMEOUT = 5000;

/**
 * An unsubscribe waits for a GQL_START_ACK only while a link can still carry
 * this subscription's start. The ack is itself bounded at START_ACK_TIMEOUT, so
 * the wait can never usefully be longer.
 */
export const SUBSCRIPTION_STOP_WAIT_TIMEOUT = START_ACK_TIMEOUT;

/**
 * Consecutive failed attempts before an episode is reported. Attempts land
 * about 5 s and 65 s after a link is lost (RECONNECT_DELAY then
 * RECONNECT_INTERVAL), so the report is about 65 s in. That is deliberate: a
 * host's token read can have a short failure window and a success cache, so a
 * transient read failure recovers well inside one interval and can never put a
 * blocking message in front of a working session. One rule, every reason.
 */
export const REALTIME_REPORT_AFTER_FAILURES = 2;

/**
 * While an episode continues, repeat the report this often. A tab can stay dead
 * for a day; this bounds that to about 24 events, all with the same message so
 * they group into one issue.
 */
export const REALTIME_REPEAT_REPORT_MS = 60 * 60 * 1000;

/** Hub events on the `api` channel, source 'PubSub'. */
export const REALTIME_CONNECTION_FAILURE = 'realtimeConnectionFailure';
export const REALTIME_RECOVERED = 'realtimeRecovered';

/**
 * Teardown reasons that `_cleanupSubscription` tells apart. They are compared,
 * not printed, so they are constants rather than inline strings.
 */
export const NEVER_CONNECTED_MESSAGE = 'Subscription never connected';
export const STOP_WAIT_TIMEOUT_MESSAGE = 'Subscription connect wait timed out';
