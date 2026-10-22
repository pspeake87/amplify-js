// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
import { Observable, Subscription, SubscriptionLike } from 'rxjs';
import { GraphQLError } from 'graphql';
import { ConsoleLogger, Hub, HubPayload } from '@aws-amplify/core';
import {
	CustomUserAgentDetails,
	DocumentType,
	NonRetryableError,
	amplifyUuid,
	base64Encoder,
	jitteredExponentialRetry,
} from '@aws-amplify/core/internals/utils';

import {
	CONTROL_MSG,
	ConnectionState,
	PubSubContentObserver,
} from '../../types/PubSub';
import {
	AMPLIFY_SYMBOL,
	CONNECTION_INIT_TIMEOUT,
	CONNECTION_STATE_CHANGE,
	CONNECT_ATTEMPT_BUDGET,
	CONNECT_SOCKET_TIMEOUT,
	DEFAULT_KEEP_ALIVE_ALERT_TIMEOUT,
	DEFAULT_KEEP_ALIVE_HEARTBEAT_TIMEOUT,
	HEADER_RESOLVE_TIMEOUT,
	MAX_DELAY_MS,
	MESSAGE_TYPES,
	NEVER_CONNECTED_MESSAGE,
	NON_RETRYABLE_CODES,
	NON_RETRYABLE_ERROR_TYPES,
	NON_RETRYABLE_START_ERROR_TYPES,
	REALTIME_CONNECTION_FAILURE,
	REALTIME_RECOVERED,
	REALTIME_REPEAT_REPORT_MS,
	REALTIME_REPORT_AFTER_MS,
	SOCKET_CLOSE_TIMEOUT,
	SOCKET_STATUS,
	START_ACK_TIMEOUT,
	STOP_WAIT_TIMEOUT_MESSAGE,
	SUBSCRIPTION_STATUS,
	SUBSCRIPTION_STOP_WAIT_TIMEOUT,
} from '../constants';
import {
	CONNECTION_CHANGE,
	ConnectionStateMonitor,
} from '../../utils/ConnectionStateMonitor';
import {
	ReconnectEvent,
	ReconnectionMonitor,
} from '../../utils/ReconnectionMonitor';
import type { AWSAppSyncRealTimeProviderOptions } from '../AWSAppSyncRealTimeProvider';

import {
	additionalHeadersFromOptions,
	queryParamsFromCustomHeaders,
	realtimeUrlWithQueryString,
} from './appsyncUrl';
import { awsRealTimeHeaderBasedAuth } from './authHeaders';
import {
	AttemptBudgetError,
	LinkPhase,
	PhaseDeadlineError,
	RealtimeConnectionFailure,
	RealtimeDiagnostics,
	RealtimeFailureReason,
	StaleAttemptError,
	classifyRealtimeFailure,
} from './realtimeFailure';

const dispatchApiEvent = (payload: HubPayload) => {
	Hub.dispatch('api', payload, 'PubSub', AMPLIFY_SYMBOL);
};

const AUTH_ERROR_TYPES = [
	'UnauthorizedException',
	'Unauthorized',
	'NotAuthorizedException',
];

/**
 * A refused GQL_START that no replay can fix. Classified by the service's
 * own fields; an error with no type at all is retryable (the service gave no
 * reason to stop).
 */
const isNonRetryableStartError = (error: {
	errorType?: string;
	errorCode?: number;
	message?: string;
}): boolean =>
	NON_RETRYABLE_START_ERROR_TYPES.includes(error.errorType ?? '') ||
	error.errorCode === 400 ||
	/validation error/i.test(error.message ?? '');

/**
 * A wait on the host that is not part of an attempt (an attempt's waits go
 * through withPhaseDeadline). The library must not trust the host to answer.
 */
const withDeadline = <T>(work: Promise<T>, ms: number): Promise<T> =>
	new Promise<T>((resolve, reject) => {
		const timer = setTimeout(() => {
			reject(new PhaseDeadlineError('headers', ms));
		}, ms);
		work.then(
			value => {
				clearTimeout(timer);
				resolve(value);
			},
			err => {
				clearTimeout(timer);
				reject(err);
			},
		);
	});

export interface ObserverQuery {
	observer: PubSubContentObserver;
	query: string;
	variables: DocumentType;
	subscriptionState: SUBSCRIPTION_STATUS;
	subscriptionReadyCallback?(): void;
	subscriptionFailedCallback?(reason?: any): void;
	startAckTimeoutId?: ReturnType<typeof setTimeout>;
	/** The link generation whose socket carries this subscription's GQL_START. */
	startedGen?: number;
}

interface ParsedMessagePayload {
	type: string;
	payload: {
		connectionTimeoutMs: number;
		errors?: [{ errorType: string; errorCode: number }];
	};
}

interface AWSWebSocketProviderArgs {
	providerName: string;
	wsProtocolName: string;
	connectUri: string;
}

/**
 * One connect attempt and the socket it owns. Its generation stamps every
 * timer, socket callback and server message, so work that belongs to an
 * abandoned attempt can never touch the live one.
 */
interface Link {
	gen: number;
	phase: LinkPhase;
	socket?: WebSocket;
	headers?: Record<string, string>;
	libraryConfigHeaders?: Record<string, string>;
	budgetTimer?: ReturnType<typeof setTimeout>;
	phaseTimer?: ReturnType<typeof setTimeout>;
	/** A deliberate close that waits for the socket's buffer to drain. */
	closeTimer?: ReturnType<typeof setTimeout>;
	budgetReached: Promise<void>;
	/**
	 * Rejects the step this attempt is waiting on. Releasing a link disarms its
	 * deadline, so without this the wait would have no settler left at all.
	 */
	abandon?(reason: unknown): void;
}

export abstract class AWSWebSocketProvider {
	protected logger: ConsoleLogger;
	protected subscriptionObserverMap = new Map<string, ObserverQuery>();
	protected allowNoSubscriptions = false;

	// Preserved by name and meaning: external diagnostics read all of these, so
	// a rename silently loses them.
	protected awsRealTimeSocket?: WebSocket;
	private socketStatus: SOCKET_STATUS = SOCKET_STATUS.CLOSED;
	private promiseArray: { res(): void; rej(reason?: any): void }[] = [];
	private keepAliveTimestamp: number = Date.now();
	private keepAliveHeartbeatIntervalId?: ReturnType<typeof setInterval>;

	private readonly connectionStateMonitor = new ConnectionStateMonitor();
	private readonly reconnectionMonitor = new ReconnectionMonitor();

	/**
	 * Every subscription teardown in progress. rxjs runs an unsubscribe
	 * synchronously and cannot await it; the teardown itself (wait for the
	 * server's ack, send GQL_STOP, drop the entry) is asynchronous. A host that
	 * ends a round and starts another (DataStore's sync engine) awaits
	 * `settleTeardowns()` in between, so the two rounds never overlap on the
	 * server, where both count against the 200-subscription cap.
	 */
	private readonly teardowns = new Set<Promise<void>>();

	/** The state before the current one: a network return is a transition */
	private lastConnectionState?: ConnectionState;
	private connectionStateMonitorSubscription: SubscriptionLike;
	private readonly wsProtocolName: string;
	private readonly wsConnectUri: string;
	protected readonly providerName: string;

	private generation = 0;
	private link?: Link;
	private lastFailure?: { gen: number; failure: RealtimeConnectionFailure };

	// The failure episode: one signal per episode, not one per attempt.
	private episodeId = 0;
	private episodeStartedAt = 0;
	private consecutiveFailures = 0;
	private episodeReportedAt = 0;
	private episodeSuspended = false;
	private socketsAttemptedInEpisode = 0;
	private failureReasons: Partial<Record<RealtimeFailureReason, number>> = {};
	private lastConnectedAt = 0;

	constructor(args: AWSWebSocketProviderArgs) {
		this.logger = new ConsoleLogger(args.providerName);
		this.providerName = args.providerName;
		this.wsProtocolName = args.wsProtocolName;
		this.wsConnectUri = args.connectUri;

		this.connectionStateMonitorSubscription =
			this._startConnectionStateMonitoring();
	}

	/**
	 * THE rule of this file. A socket, a timer or a server message may touch
	 * shared state only while it belongs to the current link. Everything else is
	 * a no-op.
	 *
	 * This one check replaces every ad hoc staleness guard. It rejects a stale
	 * event in every phase, which is what stops a dead socket's late onclose
	 * recording against a healthy connection and clearing its keep-alive.
	 */
	private isCurrent(gen: number): boolean {
		return this.link?.gen === gen;
	}

	private entry(subscriptionId: string) {
		return this.subscriptionObserverMap.get(subscriptionId);
	}

	/**
	 * Merge, never replace. Writing a full entry silently dropped fields: a
	 * replay dropped a concurrent unsubscribe's resolve/reject, and the
	 * unsubscribe wait dropped startAckTimeoutId. One method closes both.
	 *
	 * A patch for a subscription that is already gone is dropped. Work in flight
	 * outlives an unsubscribe, and a resurrected entry would hold the socket open
	 * and be counted in every diagnostic, with no observer behind it.
	 */
	private updateEntry(subscriptionId: string, patch: Partial<ObserverQuery>) {
		const current = this.subscriptionObserverMap.get(subscriptionId);
		if (!current) {
			return;
		}
		this.subscriptionObserverMap.set(subscriptionId, { ...current, ...patch });
	}

	/**
	 * Mark the socket closed and release all active listeners
	 */
	async close() {
		this.socketStatus = SOCKET_STATUS.CLOSED;

		// Only here. A deliberate shutdown must halt reconnection, and
		// CONNECTION_FAILED is what does that. It has no other caller: a failed
		// attempt we intend to retry records CLOSED instead.
		this.connectionStateMonitor.record(CONNECTION_CHANGE.CONNECTION_FAILED);

		// Turn off the subscription monitor Hub publishing
		this.connectionStateMonitorSubscription.unsubscribe();
		// Complete all reconnect observers
		this.reconnectionMonitor.close();

		// A shutdown must not wait on the socket it is tearing down, so nothing
		// here waits for an onclose that may never arrive.
		const { link } = this;
		this.link = undefined;
		if (link) {
			this.releaseLink(link);
		}
		this.subscriptionObserverMap = new Map();
		this.rejectWaiters(new Error('Realtime provider closed'));
	}

	subscribe(
		options?: AWSAppSyncRealTimeProviderOptions,
		customUserAgentDetails?: CustomUserAgentDetails,
	): Observable<Record<string, unknown>> {
		return new Observable(observer => {
			if (!options?.appSyncGraphqlEndpoint) {
				observer.error({
					errors: [
						{
							...new GraphQLError(
								`Subscribe only available for AWS AppSync endpoint`,
							),
						},
					],
				});
				observer.complete();

				return;
			}

			let subscriptionStartInProgress = false;
			const subscriptionId = amplifyUuid();

			const startSubscription = () => {
				if (!subscriptionStartInProgress) {
					subscriptionStartInProgress = true;

					this._startSubscriptionWithAWSAppSyncRealTime({
						options,
						observer,
						subscriptionId,
						customUserAgentDetails,
					})
						.catch(err => {
							// The start path settles every failure itself, so nothing
							// should reach here. A failure that does must still mark the
							// entry and settle a concurrent unsubscribe. It must NOT touch
							// the link: a connection-level failure already settled its own
							// link and reported itself exactly once.
							this._failStartSubscription(subscriptionId, observer, err);
						})
						.finally(() => {
							subscriptionStartInProgress = false;
						});
				}
			};

			// Add an observable to the reconnection list to manage reconnection for this subscription
			const reconnectSubscription = new Observable(
				reconnectSubscriptionObserver => {
					this.reconnectionMonitor.addObserver(reconnectSubscriptionObserver);

					// rxjs calls this on unsubscribe. Without it the observer array
					// grows for the life of the page and every replay walks the whole
					// history.
					return () => {
						this.reconnectionMonitor.removeObserver(
							reconnectSubscriptionObserver,
						);
					};
				},
			).subscribe(() => {
				startSubscription();
			});

			startSubscription();

			return async () => {
				const teardown = this._cleanupSubscription(
					subscriptionId,
					reconnectSubscription,
				);
				this.teardowns.add(teardown);
				try {
					await teardown;
				} finally {
					this.teardowns.delete(teardown);
				}
			};
		});
	}

	/**
	 * Resolves when every teardown in progress has ended: its GQL_STOP was
	 * sent, or it gave up (no link, or the ack wait ran out). Bounded by the
	 * unsubscribe wait of one subscription (SUBSCRIPTION_STOP_WAIT_TIMEOUT);
	 * resolves at once when nothing is being torn down.
	 */
	async settleTeardowns(): Promise<void> {
		while (this.teardowns.size > 0) {
			await Promise.allSettled([...this.teardowns]);
		}
	}

	protected async connect(
		options: AWSAppSyncRealTimeProviderOptions,
	): Promise<void> {
		if (this.link?.phase === 'live') {
			return;
		}

		await this.ensureLink(options);
	}

	protected async publish(
		options: AWSAppSyncRealTimeProviderOptions,
		customUserAgentDetails?: CustomUserAgentDetails,
	): Promise<void> {
		if (this.socketStatus !== SOCKET_STATUS.READY) {
			throw new Error('Subscription has not been initialized');
		}

		return this._publishMessage(options, customUserAgentDetails);
	}

	private async _publishMessage(
		options: AWSAppSyncRealTimeProviderOptions,
		customUserAgentDetails?: CustomUserAgentDetails,
	): Promise<void> {
		const subscriptionId = amplifyUuid();

		const { additionalCustomHeaders, libraryConfigHeaders } =
			await additionalHeadersFromOptions(options);

		const serializedSubscriptionMessage =
			await this._prepareSubscriptionPayload({
				options,
				subscriptionId,
				customUserAgentDetails,
				additionalCustomHeaders,
				libraryConfigHeaders,
				publish: true,
			});

		return new Promise((resolve, reject) => {
			if (this.awsRealTimeSocket) {
				const publishListener = (event: MessageEvent) => {
					const data = JSON.parse(event.data);
					if (data.id === subscriptionId && data.type === 'publish_success') {
						this.awsRealTimeSocket &&
							this.awsRealTimeSocket.removeEventListener(
								'message',
								publishListener,
							);
						cleanup();
						resolve();
					}

					if (data.errors && data.errors.length > 0) {
						const errorTypes = data.errors.map((error: any) => error.errorType);
						cleanup();
						reject(new Error(`Publish errors: ${errorTypes.join(', ')}`));
					}
				};

				const errorListener = (error: Event) => {
					cleanup();
					reject(new Error(`WebSocket error: ${error}`));
				};

				const closeListener = () => {
					cleanup();
					reject(new Error('WebSocket is closed'));
				};

				const cleanup = () => {
					this.awsRealTimeSocket?.removeEventListener(
						'message',
						publishListener,
					);
					this.awsRealTimeSocket?.removeEventListener('error', errorListener);
					this.awsRealTimeSocket?.removeEventListener('close', closeListener);
				};

				this.awsRealTimeSocket.addEventListener('message', publishListener);
				this.awsRealTimeSocket.addEventListener('error', errorListener);
				this.awsRealTimeSocket.addEventListener('close', closeListener);

				this.awsRealTimeSocket.send(serializedSubscriptionMessage);
			} else {
				reject(new Error('WebSocket is not connected'));
			}
		});
	}

	/**
	 * Instrumentation only — no behavior change. AppSync counts a started
	 * subscription against the connection's 200-subscription limit until a
	 * GQL_STOP frame arrives or the socket truly closes. Every code path that
	 * skips sending GQL_STOP while deleting local bookkeeping can therefore
	 * orphan a server-side subscription on a long-lived socket
	 * (MaxSubscriptionsReachedError, Sentry JAVASCRIPT-NEXTJS-2X). Dispatch a
	 * Hub event at each skip site so the app can report which path fires and
	 * whether the socket was really open (readyState OPEN = orphan created;
	 * CLOSED = server cleaned up with the connection, benign).
	 */
	private _dispatchStopSkipped(subscriptionId: string, reason: string) {
		try {
			const entry = this.entry(subscriptionId);
			dispatchApiEvent({
				event: 'subscriptionStopSkipped',
				data: {
					reason,
					subscriptionState: entry?.subscriptionState,
					queryName: entry?.query?.match(/\{\s*(\w+)/)?.[1],
					socketStatus: this.socketStatus,
					readyState: this.awsRealTimeSocket?.readyState,
					remainingSubscriptions: this.subscriptionObserverMap.size,
				},
				message: `GQL_STOP not sent for subscription (${reason})`,
			});
		} catch (err) {
			// Telemetry must never break the cleanup path
			this.logger.debug('Failed to dispatch subscriptionStopSkipped', err);
		}
	}

	private async _cleanupSubscription(
		subscriptionId: string,
		reconnectSubscription: Subscription,
	) {
		try {
			// The reconnect observer is the ONLY thing that can settle the wait
			// below. It is unsubscribed in `finally`, after the wait, never before
			// it: unsubscribing first froze every teardown that happened while
			// realtime was down.
			await this._waitForSubscriptionToBeConnected(subscriptionId);

			const { subscriptionState } = this.entry(subscriptionId) ?? {};

			if (!subscriptionState) {
				// subscription already unsubscribed
				return;
			}

			if (subscriptionState === SUBSCRIPTION_STATUS.CONNECTED) {
				this._sendUnsubscriptionMessage(subscriptionId);
			} else {
				throw new Error(NEVER_CONNECTED_MESSAGE);
			}
		} catch (err) {
			const message = err instanceof Error ? err.message : '';
			this._dispatchStopSkipped(
				subscriptionId,
				message === NEVER_CONNECTED_MESSAGE
					? 'never-connected'
					: message === STOP_WAIT_TIMEOUT_MESSAGE
						? 'wait-timeout'
						: 'connect-failed',
			);
			this.logger.debug(`Error while unsubscribing ${err}`);
		} finally {
			this._removeSubscriptionObserver(subscriptionId);
			reconnectSubscription?.unsubscribe();
		}
	}

	// Waiting that subscription has been connected before trying to unsubscribe
	private async _waitForSubscriptionToBeConnected(subscriptionId: string) {
		const entry = this.entry(subscriptionId);
		if (!entry || entry.subscriptionState !== SUBSCRIPTION_STATUS.PENDING) {
			return;
		}

		// A GQL_START can still be acked only while a link exists: an attempt in
		// flight (this start is queued behind it) or a live link that carried this
		// start. With no link there is no possible settler, so the wait is not
		// entered at all — it is not a safety margin, it is a hang. While realtime
		// is down EVERY entry is PENDING, which is why a clear() while down froze.
		const { link } = this;
		const startedOnAnOlderLink =
			entry.startedGen !== undefined && entry.startedGen !== link?.gen;
		if (!link || (link.phase === 'live' && startedOnAnOlderLink)) {
			throw new Error(NEVER_CONNECTED_MESSAGE);
		}

		return new Promise<void>((resolve, reject) => {
			const timer = setTimeout(() => {
				reject(new Error(STOP_WAIT_TIMEOUT_MESSAGE));
			}, SUBSCRIPTION_STOP_WAIT_TIMEOUT);

			// updateEntry, so startAckTimeoutId is kept: replacing the entry here
			// left an orphan 15 s timer behind.
			this.updateEntry(subscriptionId, {
				subscriptionReadyCallback: () => {
					clearTimeout(timer);
					resolve();
				},
				subscriptionFailedCallback: (reason?: any) => {
					clearTimeout(timer);
					reject(
						reason instanceof Error
							? reason
							: new Error(NEVER_CONNECTED_MESSAGE),
					);
				},
			});
		});
	}

	// Monitor the connection state and pass changes along to Hub
	private _startConnectionStateMonitoring() {
		return this.connectionStateMonitor.connectionStateObservable.subscribe(
			connectionState => {
				dispatchApiEvent({
					event: CONNECTION_STATE_CHANGE,
					data: {
						provider: this,
						providerName: this.providerName,
						connectionState,
					},
					message: `Connection state is ${connectionState}`,
				});

				// A failure while the device is offline is not a dead end, and the
				// reconnect monitor is halted for it anyway. No DOM: the
				// *PendingNetwork states come from ReachabilityMonitor, which has a
				// React Native implementation.
				this.episodeSuspended =
					connectionState ===
						ConnectionState.ConnectionDisruptedPendingNetwork ||
					connectionState === ConnectionState.ConnectedPendingNetwork;

				// Trigger START_RECONNECT when the connection is disrupted. Ranchr
				// fork: when the disruption was the device's own network and the
				// network is back, reconnect now (walk 1, MA8: the fixed delay kept
				// the realtime link 5 s behind every return to online).
				const networkReturned =
					this.lastConnectionState ===
						ConnectionState.ConnectionDisruptedPendingNetwork &&
					connectionState === ConnectionState.ConnectionDisrupted;
				this.lastConnectionState = connectionState;
				if (connectionState === ConnectionState.ConnectionDisrupted) {
					this.reconnectionMonitor.record(
						networkReturned
							? ReconnectEvent.NETWORK_RETURNED
							: ReconnectEvent.START_RECONNECT,
					);
				}

				// Trigger HALT_RECONNECT to halt reconnection attempts when the state is anything other than
				// ConnectionDisrupted or Connecting
				if (
					[
						ConnectionState.Connected,
						ConnectionState.ConnectedPendingDisconnect,
						ConnectionState.ConnectedPendingKeepAlive,
						ConnectionState.ConnectedPendingNetwork,
						ConnectionState.ConnectionDisruptedPendingNetwork,
						ConnectionState.Disconnected,
					].includes(connectionState)
				) {
					this.reconnectionMonitor.record(ReconnectEvent.HALT_RECONNECT);
				}
			},
		);
	}

	protected abstract _prepareSubscriptionPayload(param: {
		options: AWSAppSyncRealTimeProviderOptions;
		subscriptionId: string;
		customUserAgentDetails: CustomUserAgentDetails | undefined;
		additionalCustomHeaders: Record<string, string>;
		libraryConfigHeaders: Record<string, string>;
		publish?: boolean;
	}): Promise<string>;

	private async _startSubscriptionWithAWSAppSyncRealTime({
		options,
		observer,
		subscriptionId,
		customUserAgentDetails,
	}: {
		options: AWSAppSyncRealTimeProviderOptions;
		observer: PubSubContentObserver;
		subscriptionId: string;
		customUserAgentDetails: CustomUserAgentDetails | undefined;
	}) {
		const { query, variables } = options;

		// A replay for an id this live link already started is a no-op: AppSync
		// scopes ids per connection and answers a repeat with
		// DuplicatedOperationError. Checked BEFORE the map write, so a replay can
		// never reset a CONNECTED entry back to PENDING and strand it with no ack
		// timer.
		if (
			this.link?.phase === 'live' &&
			this.entry(subscriptionId)?.startedGen === this.link.gen
		) {
			return;
		}

		// A replay reuses the subscription id. A start_ack timer of the previous
		// attempt must not fire against this attempt's link.
		const previousStartAckTimeoutId =
			this.entry(subscriptionId)?.startAckTimeoutId;
		if (previousStartAckTimeoutId) {
			clearTimeout(previousStartAckTimeoutId);
		}

		// The one write that may create the entry. It still merges, so a replay
		// keeps a concurrent unsubscribe's callbacks.
		this.subscriptionObserverMap.set(subscriptionId, {
			...this.entry(subscriptionId),
			observer,
			query: query ?? '',
			variables: variables ?? {},
			subscriptionState: SUBSCRIPTION_STATUS.PENDING,
			startAckTimeoutId: undefined,
			startedGen: undefined,
		});

		// Two kinds of start. A start queued behind an attempt is signed with that
		// attempt's credentials: one token read for the whole replay round, read
		// seconds ago. A start on a link that is ALREADY live resolves its own:
		// the link's credentials were read when it connected, and a link can live
		// for a day while the token it carried has expired.
		const linkWasLive = this.link?.phase === 'live';

		let link: Link;
		try {
			// Single flight: the whole round is one attempt, one failure.
			link = await this.ensureLink(options);
		} catch (err) {
			this._failStartSubscription(subscriptionId, observer, err);

			return;
		}

		let serializedSubscriptionMessage: string;
		try {
			const headers = linkWasLive
				? await withDeadline(
						additionalHeadersFromOptions(options),
						HEADER_RESOLVE_TIMEOUT,
					)
				: {
						additionalCustomHeaders: link.headers ?? {},
						libraryConfigHeaders: link.libraryConfigHeaders ?? {},
					};
			serializedSubscriptionMessage = await this._prepareSubscriptionPayload({
				options,
				subscriptionId,
				customUserAgentDetails,
				additionalCustomHeaders: headers.additionalCustomHeaders as Record<
					string,
					string
				>,
				libraryConfigHeaders: headers.libraryConfigHeaders as Record<
					string,
					string
				>,
			});
		} catch (err) {
			if (linkWasLive && this.isCurrent(link.gen)) {
				// A start on a live link has no replay of its own: left FAILED it
				// would stay so for the life of the link. Its credentials failed (an
				// empty token fails here, in the payload), so drop the link and the
				// attempt machinery owns the retry and the report.
				this.loseLink(link.gen, CONTROL_MSG.CONNECTION_FAILED);
			}
			this._failStartSubscription(subscriptionId, observer, err);

			return;
		}

		try {
			if (!this.entry(subscriptionId)) {
				// Unsubscribed while this start was queued: the teardown gave up its
				// wait and deleted the entry. A start sent now would be a server-side
				// subscription that no GQL_STOP ever ends.
				this.logger.debug(
					`Start of ${subscriptionId} skipped: unsubscribed while queued`,
				);

				return;
			}

			if (!this.isCurrent(link.gen) || link.phase !== 'live' || !link.socket) {
				// The link died while the payload was built. The next replay covers
				// it; this is not a new connection-level failure.
				this._failStartSubscription(
					subscriptionId,
					observer,
					new Error('Realtime link is not live'),
				);

				return;
			}

			// Set before the send, so a start_ack that arrives in the same tick
			// finds the entry.
			this.updateEntry(subscriptionId, {
				startedGen: link.gen,
				startAckTimeoutId: setTimeout(() => {
					this._timeoutStartSubscriptionAck(link.gen, subscriptionId);
				}, START_ACK_TIMEOUT),
			});

			link.socket.send(serializedSubscriptionMessage);
		} catch (err) {
			this._failStartSubscription(subscriptionId, observer, err);
		}
	}

	/**
	 * A start that failed. Connection-level reporting is already done, once, by
	 * failAttempt, so this method is per subscription only.
	 *
	 * It must NOT call observer.error for an auth or a transient failure.
	 * DataStore's SubscriptionProcessor unsubscribes the model's subscriptions
	 * for any message containing 'Connection failed', and with one read auth mode
	 * it never recreates them: 45 loud errors would kill realtime for the whole
	 * session. Only a configuration error, which no replay can fix, errors the
	 * observable.
	 */
	private _failStartSubscription(
		subscriptionId: string,
		observer: PubSubContentObserver,
		err: unknown,
	) {
		// A failed start must leave a trace even though the link is unharmed.
		this.logger.debug(
			`${CONTROL_MSG.REALTIME_SUBSCRIPTION_INIT_ERROR}: ${err}`,
		);

		this.updateEntry(subscriptionId, {
			subscriptionState: SUBSCRIPTION_STATUS.FAILED,
		});

		const { subscriptionFailedCallback } = this.entry(subscriptionId) ?? {};

		// Notify concurrent unsubscription
		if (typeof subscriptionFailedCallback === 'function') {
			subscriptionFailedCallback(err);
		}

		const reason = (err as RealtimeConnectionFailure | undefined)?.reason;
		if (reason === 'config-error') {
			observer.error({
				errors: [
					{
						...new GraphQLError(
							String((err as Error)?.message ?? CONTROL_MSG.CONNECTION_FAILED),
						),
					},
				],
			});
		}
	}

	private _timeoutStartSubscriptionAck(gen: number, subscriptionId: string) {
		const entry = this.entry(subscriptionId);
		if (!entry || entry.startedGen !== gen) {
			// a stale timer from a previous link
			return;
		}
		this.updateEntry(subscriptionId, {
			subscriptionState: SUBSCRIPTION_STATUS.FAILED,
			startAckTimeoutId: undefined,
		});
		this.logger.debug(
			'timeoutStartSubscription',
			JSON.stringify({ query: entry.query, variables: entry.variables }),
		);

		// The link must genuinely close: the ReconnectionMonitor replays every
		// subscription with its original id, and AppSync scopes ids per
		// connection, so a replay onto a still-open socket is rejected with
		// DuplicatedOperationError.
		this.loseLink(gen, CONTROL_MSG.TIMEOUT_DISCONNECT);
	}

	protected abstract _unsubscribeMessage(subscriptionId: string): {
		id: string;
		type: string;
	};

	private _sendUnsubscriptionMessage(subscriptionId: string) {
		try {
			if (
				this.awsRealTimeSocket &&
				this.awsRealTimeSocket.readyState === WebSocket.OPEN &&
				this.socketStatus === SOCKET_STATUS.READY
			) {
				// Preparing unsubscribe message to stop receiving messages for that subscription
				const unsubscribeMessage = this._unsubscribeMessage(subscriptionId);
				const stringToAWSRealTime = JSON.stringify(unsubscribeMessage);
				this.awsRealTimeSocket.send(stringToAWSRealTime);
			} else {
				// Only reached for a locally-CONNECTED subscription — the server
				// definitely started it. If readyState is OPEN here (status flag
				// merely stale), an orphan was just created.
				this._dispatchStopSkipped(subscriptionId, 'socket-not-ready');
			}
		} catch (err) {
			// If GQL_STOP is not sent because of disconnection issue, then there is nothing the client can do
			this._dispatchStopSkipped(subscriptionId, 'send-failed');
			this.logger.debug({ err });
		}
	}

	private _removeSubscriptionObserver(subscriptionId: string) {
		this.subscriptionObserverMap.delete(subscriptionId);

		// Verifying 1000ms after removing subscription in case there are new subscription unmount/mount
		if (!this.allowNoSubscriptions) {
			setTimeout(this._closeSocketIfRequired.bind(this), 1000);
		}
	}

	protected _closeSocketIfRequired() {
		if (this.subscriptionObserverMap.size > 0) {
			// Active subscriptions on the WebSocket
			return;
		}

		const { link } = this;
		if (!link) {
			// No subscriptions and no link: the provider is idle by intent.
			// Publish that. Without it the last label stays ConnectionDisrupted,
			// the reconnect monitor keeps replaying into an empty observer list,
			// and a host reads a dead end where there is none.
			this.socketStatus = SOCKET_STATUS.CLOSED;
			this.connectionStateMonitor.record(CONNECTION_CHANGE.CLOSING_CONNECTION);
			this.connectionStateMonitor.record(CONNECTION_CHANGE.CLOSED);
			this.rejectWaiters(new Error('Realtime link closed'));

			return;
		}
		if (link.closeTimer) {
			// A close is already waiting for the socket's buffer.
			return;
		}

		const close = () => {
			link.closeTimer = undefined;
			if (!this.isCurrent(link.gen) || this.subscriptionObserverMap.size > 0) {
				// The link was lost meanwhile, or a subscription arrived during the
				// grace period. The link never changed phase or label while it
				// waited, so there is nothing to undo: that subscription started on
				// the open socket like any other.
				return;
			}
			this.logger.debug('closing WebSocket...');
			this.connectionStateMonitor.record(CONNECTION_CHANGE.CLOSING_CONNECTION);
			this.releaseLink(link);
			this.link = undefined;
			this.connectionStateMonitor.record(CONNECTION_CHANGE.CLOSED);
			this.rejectWaiters(new Error('Realtime link closed'));
		};

		// ONE grace period for a buffered send on a live socket, then the close
		// happens anyway. Rescheduling while bufferedAmount stayed above zero
		// meant a teardown could never complete. An attempt that is not live yet
		// has nothing buffered worth waiting for and is abandoned at once.
		if (
			link.phase === 'live' &&
			link.socket &&
			link.socket.bufferedAmount > 0
		) {
			link.closeTimer = setTimeout(close, SOCKET_CLOSE_TIMEOUT);
		} else {
			close();
		}
	}

	protected abstract _handleSubscriptionData(
		message: MessageEvent,
	): [
		boolean,
		{ id: string; payload: string | Record<string, unknown>; type: string },
	];

	protected abstract _extractConnectionTimeout(
		data: Record<string, any>,
	): number;

	protected abstract _extractErrorCodeAndType(data: Record<string, any>): {
		errorCode: number;
		errorType: string;
	};

	private maintainKeepAlive() {
		this.keepAliveTimestamp = Date.now();
	}

	private keepAliveHeartbeat(gen: number, connectionTimeoutMs: number) {
		if (!this.isCurrent(gen)) {
			// a stale tick never records
			return;
		}

		const currentTime = Date.now();

		// Check for missed KA message
		if (
			currentTime - this.keepAliveTimestamp >
			DEFAULT_KEEP_ALIVE_ALERT_TIMEOUT
		) {
			this.connectionStateMonitor.record(CONNECTION_CHANGE.KEEP_ALIVE_MISSED);
		} else {
			this.connectionStateMonitor.record(CONNECTION_CHANGE.KEEP_ALIVE);
		}

		// Recognize we are disconnected if we haven't seen messages in the keep alive timeout period
		if (currentTime - this.keepAliveTimestamp > connectionTimeoutMs) {
			this.loseLink(gen, CONTROL_MSG.TIMEOUT_DISCONNECT);
		}
	}

	private _handleIncomingSubscriptionMessage(message: MessageEvent) {
		if (typeof message.data !== 'string') {
			return;
		}

		const [isData, data] = this._handleSubscriptionData(message);
		if (isData) {
			this.maintainKeepAlive();

			return;
		}

		const { type, id, payload } = data;

		const {
			observer = null,
			query = '',
			variables = {},
			startAckTimeoutId,
			subscriptionReadyCallback,
			subscriptionFailedCallback,
		} = this.entry(id) || {};

		if (
			type === MESSAGE_TYPES.GQL_START_ACK ||
			type === MESSAGE_TYPES.EVENT_SUBSCRIBE_ACK
		) {
			this.logger.debug(
				`subscription ready for ${JSON.stringify({ query, variables })}`,
			);
			if (typeof subscriptionReadyCallback === 'function') {
				subscriptionReadyCallback();
			}
			if (startAckTimeoutId) clearTimeout(startAckTimeoutId);
			dispatchApiEvent({
				event: CONTROL_MSG.SUBSCRIPTION_ACK,
				data: { query, variables },
				message: 'Connection established for subscription',
			});
			if (observer) {
				this.updateEntry(id, {
					startAckTimeoutId: undefined,
					subscriptionState: SUBSCRIPTION_STATUS.CONNECTED,
				});
			}
			this.connectionStateMonitor.record(
				CONNECTION_CHANGE.CONNECTION_ESTABLISHED,
			);
			// Realtime works end to end — the server acked a start. This, not an
			// open socket, is what closes a failure episode.
			this.onRealtimeWorking();

			return;
		}

		if (type === MESSAGE_TYPES.GQL_CONNECTION_KEEP_ALIVE) {
			this.maintainKeepAlive();

			return;
		}

		if (
			type === MESSAGE_TYPES.GQL_ERROR ||
			type === MESSAGE_TYPES.EVENT_SUBSCRIBE_ERROR
		) {
			if (!observer) {
				return;
			}
			const { subscriptionState, startedGen } = this.entry(id)!;

			this.updateEntry(id, {
				subscriptionState: SUBSCRIPTION_STATUS.FAILED,
				startAckTimeoutId: undefined,
			});
			if (startAckTimeoutId) clearTimeout(startAckTimeoutId);

			let errorMessage = JSON.stringify(payload ?? data);
			let error: { errorType?: string; errorCode?: number; message?: string } =
				{};

			if (type === MESSAGE_TYPES.EVENT_SUBSCRIBE_ERROR) {
				const { errors } = JSON.parse(String(message.data));
				if (Array.isArray(errors) && errors.length > 0) {
					[error] = errors;
					errorMessage = `${error.errorType}: ${error.message}`;
				}
			} else if (
				type === MESSAGE_TYPES.GQL_ERROR &&
				payload &&
				typeof payload === 'object'
			) {
				// AppSync sends `errors` as a list; the cap error has been seen as
				// one object. Read either.
				const { errors } = payload as Record<string, unknown>;
				const first = Array.isArray(errors) ? errors[0] : errors;
				if (first && typeof first === 'object') {
					error = first as typeof error;
				}
			}
			const isAuthError =
				AUTH_ERROR_TYPES.includes(error.errorType ?? '') ||
				!!error.message?.includes('Token expired');

			this.logger.debug(`${CONTROL_MSG.CONNECTION_FAILED}: ${errorMessage}`);

			// A start this live link sent and the service refused. Retryable
			// (an expired credential, the 200-subscription cap, a limit, anything
			// unknown): the entry is kept, the link is lost, and the next link
			// replays every entry with credentials read for that link. Before,
			// the refusal errored the observable, DataStore dropped that
			// model-operation for the life of the engine, and the tab had no
			// realtime for it until a reload (JAVASCRIPT-NEXTJS-50 → -51).
			const refusedStart =
				subscriptionState === SUBSCRIPTION_STATUS.PENDING &&
				this.link?.phase === 'live' &&
				startedGen === this.link.gen;
			if (refusedStart && !isNonRetryableStartError(error)) {
				const refusal = Object.assign(
					new Error(`${CONTROL_MSG.CONNECTION_FAILED}: ${errorMessage}`),
					{ errorType: error.errorType, errorCode: error.errorCode },
				);
				// Settle a concurrent unsubscribe: the server never registered it,
				// so there is nothing to stop.
				if (typeof subscriptionFailedCallback === 'function') {
					subscriptionFailedCallback(refusal);
				}
				this.refuseLink(this.link!.gen, refusal);

				return;
			}

			// Non-retryable, or a running subscription the service ended: this
			// model-operation is over. On an auth error, lose the link so the
			// replay of the others reconnects with fresh credentials.
			if (isAuthError && this.link?.phase === 'live') {
				this.logger.warn(
					'Subscription failed due to auth error, closing WebSocket to trigger reconnection with fresh tokens',
				);
				this.loseLink(this.link.gen, CONTROL_MSG.CONNECTION_FAILED);
			}

			observer.error({
				errors: [
					{
						...new GraphQLError(
							`${CONTROL_MSG.CONNECTION_FAILED}: ${errorMessage}`,
						),
					},
				],
			});

			if (typeof subscriptionFailedCallback === 'function') {
				subscriptionFailedCallback();
			}
		}
	}

	/**
	 * The live link lost because the service refused a start on it. One
	 * episode failure per link loss: the other refusals of the round never
	 * arrive (a closing socket delivers no message), and a late one finds no
	 * live link. The reason reaches the episode record, so an auth refusal
	 * reports as an auth failure (the host raises its overlay from that) and
	 * the cap reports as `start-refused`.
	 */
	private refuseLink(gen: number, refusal: Error) {
		if (!this.isCurrent(gen) || this.link!.phase !== 'live') {
			return;
		}
		this.recordEpisodeFailure(
			refusal,
			'live',
			SOCKET_STATUS[this.socketStatus],
		);
		this.loseLink(gen, CONTROL_MSG.CONNECTION_FAILED);
	}

	/**
	 * Single flight. Every caller of a down link awaits the same attempt, so the
	 * token is read once per attempt instead of once per subscription, one replay
	 * round is exactly one attempt, and one failure is one failure. The existence
	 * of `this.link` IS the lock: there is no separate attempt id to keep in step
	 * with it.
	 */
	private async ensureLink(
		options: AWSAppSyncRealTimeProviderOptions,
	): Promise<Link> {
		if (this.link?.phase === 'live') {
			return this.link;
		}

		if (this.link) {
			// An attempt is in flight. Wait for ITS settlement, never start a
			// second. Every exit that clears this.link drains promiseArray, so this
			// wait always ends.
			return new Promise<Link>((resolve, reject) => {
				this.promiseArray.push({
					res: () => {
						const live = this.link?.phase === 'live' ? this.link : undefined;
						if (live) {
							resolve(live);
						} else {
							reject(new Error('Realtime link is not live'));
						}
					},
					rej: reject,
				});
			});
		}

		return this.startAttempt(options);
	}

	private async startAttempt(
		options: AWSAppSyncRealTimeProviderOptions,
	): Promise<Link> {
		const gen = ++this.generation;
		let reachBudget!: () => void;
		const link: Link = {
			gen,
			phase: 'headers',
			budgetReached: new Promise<void>(resolve => {
				reachBudget = resolve;
			}),
		};
		this.link = link;
		this.socketStatus = SOCKET_STATUS.CONNECTING;

		// The attempt's first and only trace. NOTHING that can fail or hang may
		// run above this line. A failure above it cannot be told apart from "no
		// attempt was made", because the state monitor publishes nothing for a
		// state the page already holds, and the reconnect watchdog and every
		// host-side detector read only published state.
		this.connectionStateMonitor.record(CONNECTION_CHANGE.OPENING_CONNECTION);

		link.budgetTimer = setTimeout(() => {
			reachBudget();
			this.failAttempt(
				gen,
				new AttemptBudgetError(CONNECT_ATTEMPT_BUDGET, link.phase),
			);
		}, CONNECT_ATTEMPT_BUDGET);

		try {
			// The budget settles this chain too, not only the shared state: a
			// pending promise cannot be cancelled, so a step that somehow has no
			// deadline of its own would otherwise hold this caller for ever.
			await Promise.race([
				this.runConnect(gen, link, options),
				link.budgetReached.then((): never => {
					throw new AttemptBudgetError(CONNECT_ATTEMPT_BUDGET, link.phase);
				}),
			]);

			this.onLinkLive(gen);

			return link;
		} catch (err) {
			// Idempotent: a phase deadline or the budget may have run first.
			throw this.failAttempt(gen, err) ?? err;
		}
	}

	/** The two bounded stages of one attempt: the credentials, then the socket. */
	private async runConnect(
		gen: number,
		link: Link,
		options: AWSAppSyncRealTimeProviderOptions,
	) {
		// One deadline for the whole credential stage: the host's token thunk, the
		// custom headers, and the signing that needs them. Signing can read
		// credentials over the network, so it must not sit outside a deadline.
		const subprotocol = await this.withPhaseDeadline(
			gen,
			'headers',
			HEADER_RESOLVE_TIMEOUT,
			async () => {
				const resolved = await additionalHeadersFromOptions(options);
				if (!this.isCurrent(gen)) {
					throw new StaleAttemptError();
				}
				link.headers = resolved.additionalCustomHeaders as Record<
					string,
					string
				>;
				link.libraryConfigHeaders = resolved.libraryConfigHeaders as Record<
					string,
					string
				>;

				return this.authSubprotocol(options, link.headers);
			},
		);

		const url = realtimeUrlWithQueryString(
			options.appSyncGraphqlEndpoint!,
			queryParamsFromCustomHeaders(link.headers),
		);

		await jitteredExponentialRetry(
			this._establishConnection.bind(this),
			[url, subprotocol, gen],
			MAX_DELAY_MS,
			link.budgetReached,
		);
	}

	/**
	 * The signed subprotocol this attempt connects with. No network for lambda
	 * auth; iam and userPool read credentials here, which is why the caller
	 * bounds it.
	 */
	private async authSubprotocol(
		options: AWSAppSyncRealTimeProviderOptions,
		additionalCustomHeaders: Record<string, string>,
	): Promise<string> {
		const { apiKey, appSyncGraphqlEndpoint, authenticationType, region } =
			options;

		const authHeader = await awsRealTimeHeaderBasedAuth({
			authenticationType,
			// Empty payload on connect
			payload: '{}',
			canonicalUri: this.wsConnectUri,
			apiKey,
			appSyncGraphqlEndpoint,
			region,
			additionalCustomHeaders,
		});

		const headerString = authHeader ? JSON.stringify(authHeader) : '';
		// base64url-encoded string
		const encodedHeader = base64Encoder.convert(headerString, {
			urlSafe: true,
			skipPadding: true,
		});

		return `header-${encodedHeader}`;
	}

	/**
	 * Every wait on an external event goes through here. On the deadline the
	 * WHOLE attempt is settled (failAttempt), not only this one promise. A
	 * deadline that abandons its own step and leaves the chain waiting is the
	 * bug this file is written around.
	 */
	private withPhaseDeadline<T>(
		gen: number,
		phase: LinkPhase,
		ms: number,
		run: () => Promise<T>,
	): Promise<T> {
		const { link } = this;
		if (!link || link.gen !== gen) {
			return Promise.reject(new StaleAttemptError());
		}
		link.phase = phase;

		return new Promise<T>((resolve, reject) => {
			const timer = setTimeout(() => {
				const err = new PhaseDeadlineError(phase, ms);
				reject(err);
				this.failAttempt(gen, err);
			}, ms);
			const clear = () => {
				clearTimeout(timer);
				if (link.phaseTimer === timer) {
					link.phaseTimer = undefined;
				}
				if (link.abandon === reject) {
					link.abandon = undefined;
				}
			};
			link.phaseTimer = timer;
			link.abandon = reject;
			run().then(
				value => {
					clear();
					resolve(value);
				},
				err => {
					clear();
					reject(err);
				},
			);
		});
	}

	private async _openConnection(
		awsRealTimeUrl: string,
		subprotocol: string,
		gen: number,
	) {
		return new Promise<void>((resolve, reject) => {
			const { link } = this;
			if (!link || link.gen !== gen) {
				reject(new StaleAttemptError());

				return;
			}

			const previousSocket = link.socket;
			const newSocket = this._getNewWebSocket(awsRealTimeUrl, [
				this.wsProtocolName,
				subprotocol,
			]);

			// Claimed SYNCHRONOUSLY, before any handler can fire and before any
			// deadline can expire. releaseLink closes link.socket, so a socket
			// referenced only from inside onopen would be left in CONNECTING for
			// ever when the open deadline fires — one leaked socket per attempt.
			link.socket = newSocket;
			// Counted at creation, not at onopen: a socket that never opens is
			// exactly the shape the diagnostic must show (0 = the failure came
			// before any socket, 1 or more = a socket was tried).
			this.socketsAttemptedInEpisode += 1;
			if (previousSocket && previousSocket !== newSocket) {
				// A retry inside this attempt replaces the socket.
				this.discardSocket(previousSocket);
			}

			newSocket.onerror = () => {
				// A socket that errors and never closes was an unbounded wait; this
				// rejection ends it before the deadline has to.
				this.logger.debug('WebSocket connection error');
				if (this.isCurrent(gen)) {
					reject(new Error('Connection handshake error'));
				}
			};
			newSocket.onclose = () => {
				this.logger.debug('WebSocket closed before it opened');
				if (this.isCurrent(gen)) {
					reject(new Error('Connection handshake error'));
				}
			};
			newSocket.onopen = () => {
				if (!this.isCurrent(gen) || link.socket !== newSocket) {
					this.discardSocket(newSocket);
					reject(new StaleAttemptError());

					return;
				}
				this.awsRealTimeSocket = newSocket;
				resolve();
			};
		});
	}

	private _getNewWebSocket(url: string, protocol: string[]) {
		return new WebSocket(url, protocol);
	}

	private async _initiateHandshake(gen: number): Promise<string> {
		return new Promise((resolve, reject) => {
			const { link } = this;
			const socket = link?.socket;
			if (!link || link.gen !== gen || !socket) {
				reject(new StaleAttemptError());

				return;
			}

			socket.onerror = error => {
				this.logger.debug(`WebSocket error ${JSON.stringify(error)}`);
			};

			socket.onclose = event => {
				this.logger.debug(`WebSocket closed ${event.reason}`);
				if (this.isCurrent(gen)) {
					reject(new Error('Connection closed during the handshake'));
				}
			};

			socket.onmessage = (message: MessageEvent) => {
				if (typeof message.data !== 'string' || !this.isCurrent(gen)) {
					return;
				}
				this.logger.debug(
					`subscription message from AWS AppSyncRealTime: ${message.data} `,
				);

				const data = JSON.parse(message.data) as ParsedMessagePayload;
				const { type } = data;

				if (type === MESSAGE_TYPES.GQL_CONNECTION_ACK) {
					this._registerWebsocketHandlers(
						gen,
						this._extractConnectionTimeout(data),
					);
					resolve('Connected to AWS AppSyncRealTime');

					return;
				}

				if (type === MESSAGE_TYPES.GQL_CONNECTION_ERROR) {
					const { errorType, errorCode } = this._extractErrorCodeAndType(data);

					// TODO(Eslint): refactor to reject an Error object instead of a plain object
					// eslint-disable-next-line prefer-promise-reject-errors
					reject({ errorType, errorCode });
				}
			};

			socket.send(JSON.stringify({ type: MESSAGE_TYPES.GQL_CONNECTION_INIT }));
		});
	}

	private _registerWebsocketHandlers(gen: number, connectionTimeoutMs: number) {
		const { link } = this;
		if (!link || link.gen !== gen || !link.socket) {
			return;
		}
		const { socket } = link;

		// The link is live from the connection ack, not from the end of the
		// connect chain: from this instant a socket failure is a lost live link
		// and must be recorded, never swallowed.
		link.phase = 'live';

		this.keepAliveTimestamp = Date.now();
		// releaseLink is the only owner of this handle and clears it on every
		// exit. Overwriting it without clearing leaked one 5 s interval per
		// connection for the life of the page.
		this.keepAliveHeartbeatIntervalId = setInterval(() => {
			this.keepAliveHeartbeat(gen, connectionTimeoutMs);
		}, DEFAULT_KEEP_ALIVE_HEARTBEAT_TIMEOUT);

		socket.onmessage = (message: MessageEvent) => {
			if (!this.isCurrent(gen)) {
				// a stale socket never pushes data
				return;
			}
			this._handleIncomingSubscriptionMessage(message);
		};

		socket.onerror = err => {
			this.logger.debug(err);
			this.loseLink(gen, CONTROL_MSG.CONNECTION_CLOSED);
		};

		socket.onclose = event => {
			this.logger.debug(`WebSocket closed ${event.reason}`);
			this.loseLink(gen, CONTROL_MSG.CONNECTION_CLOSED);
		};
	}

	/**
	 * Open WebSocket connection & perform handshake
	 * Ref: https://docs.aws.amazon.com/appsync/latest/devguide/real-time-websocket-client.html#appsynclong-real-time-websocket-client-implementation-guide-for-graphql-subscriptions
	 *
	 * Both steps are bounded. The deadlines are non-retryable, so a step that
	 * does not answer ends the attempt instead of being retried.
	 */
	private async _establishConnection(
		awsRealTimeUrl: string,
		subprotocol: string,
		gen: number,
	) {
		this.logger.debug(`Establishing WebSocket connection to ${awsRealTimeUrl}`);
		try {
			await this.withPhaseDeadline(gen, 'open', CONNECT_SOCKET_TIMEOUT, () =>
				this._openConnection(awsRealTimeUrl, subprotocol, gen),
			);
			await this.withPhaseDeadline(
				gen,
				'handshake',
				CONNECTION_INIT_TIMEOUT,
				() => this._initiateHandshake(gen),
			);
		} catch (err) {
			const { errorType, errorCode } = err as {
				errorType: string;
				errorCode: number;
			};

			if (
				NON_RETRYABLE_CODES.includes(errorCode) ||
				// Event API does not currently return `errorCode`. This may change in the future.
				// For now fall back to also checking known non-retryable error types
				NON_RETRYABLE_ERROR_TYPES.includes(errorType)
			) {
				// NonRetryableError carries only a message, so the type would be lost
				// and the classifier would read 'unknown'. Re-attach it.
				const fatal = new NonRetryableError(errorType);
				(fatal as any).errorType = errorType;
				(fatal as any).errorCode = errorCode;
				throw fatal;
			} else if (errorType) {
				const retryable = new Error(errorType);
				(retryable as any).errorType = errorType;
				throw retryable;
			} else {
				throw err;
			}
		}
	}

	/**
	 * A socket that is not the current link's: silence it, then close it for real
	 * so AppSync releases its subscriptions and a replay can never be answered
	 * with DuplicatedOperationError.
	 */
	private discardSocket(socket?: WebSocket) {
		if (!socket) {
			return;
		}
		socket.onopen = null;
		socket.onerror = null;
		socket.onclose = null;
		socket.onmessage = null;
		try {
			// Skip only a socket that is already going away; everything else,
			// including a socket with no readyState to read, gets a real close.
			if (
				socket.readyState !== WebSocket.CLOSING &&
				socket.readyState !== WebSocket.CLOSED
			) {
				socket.close(3001);
			}
		} catch (err) {
			this.logger.debug('Failed to close a discarded socket', err);
		}
	}

	/** No timer of a settled attempt may still be armed. */
	private clearLinkTimers(link: Link) {
		if (link.budgetTimer) {
			clearTimeout(link.budgetTimer);
			link.budgetTimer = undefined;
		}
		if (link.phaseTimer) {
			clearTimeout(link.phaseTimer);
			link.phaseTimer = undefined;
		}
		if (link.closeTimer) {
			clearTimeout(link.closeTimer);
			link.closeTimer = undefined;
		}
	}

	/** The one place that frees an attempt's resources. */
	private releaseLink(link: Link) {
		this.clearLinkTimers(link);
		// The step this attempt was waiting on has just lost its deadline, so
		// nothing else could ever settle it.
		const { abandon } = link;
		link.abandon = undefined;
		abandon?.(new StaleAttemptError());
		if (this.keepAliveHeartbeatIntervalId) {
			clearInterval(this.keepAliveHeartbeatIntervalId);
			this.keepAliveHeartbeatIntervalId = undefined;
		}
		this.discardSocket(link.socket);
		if (this.awsRealTimeSocket === link.socket) {
			this.awsRealTimeSocket = undefined;
		}
		this.socketStatus = SOCKET_STATUS.CLOSED;
	}

	/**
	 * No exit may leave a waiter parked: a parked waiter holds its caller's start
	 * latch for ever.
	 */
	private rejectWaiters(reason: unknown) {
		const waiters = this.promiseArray;
		this.promiseArray = [];
		waiters.forEach(({ rej }) => {
			rej(reason);
		});
	}

	/**
	 * The single terminal transition for a link that never went live. Idempotent
	 * and generation guarded, so a phase deadline, the budget, a socket callback
	 * and the awaited rejection may all call it and only the first one acts.
	 * Returns the failure so every path reports the same reason.
	 */
	private failAttempt(
		gen: number,
		err: unknown,
	): RealtimeConnectionFailure | undefined {
		if (!this.isCurrent(gen)) {
			return this.lastFailure?.gen === gen
				? this.lastFailure.failure
				: undefined;
		}
		const link = this.link!;
		const { phase } = link;
		// Read before the release: what the attempt was doing is the diagnostic,
		// and releaseLink sets the status to CLOSED.
		const socketStatus = SOCKET_STATUS[this.socketStatus];

		this.releaseLink(link);
		this.link = undefined;

		// CLOSED, never CONNECTION_FAILED. CONNECTION_FAILED sets
		// intendedConnectionState to 'disconnected', which publishes Disconnected,
		// fires HALT_RECONNECT and disables network monitoring — a permanent dead
		// end. A failed attempt we intend to retry is ConnectionDisrupted.
		this.connectionStateMonitor.record(CONNECTION_CHANGE.CLOSED);

		const failure = this.recordEpisodeFailure(err, phase, socketStatus);
		this.lastFailure = { gen, failure };

		// The deadline settles the whole chain, not only its own step: every
		// parked ensureLink waiter rejects here, so every start latch is released
		// by its `.finally`.
		this.rejectWaiters(failure);

		return failure;
	}

	/** The attempt succeeded: tell everyone who was waiting for it. */
	private onLinkLive(gen: number) {
		if (!this.isCurrent(gen)) {
			return;
		}
		const link = this.link!;
		this.clearLinkTimers(link);
		link.phase = 'live';
		this.socketStatus = SOCKET_STATUS.READY;

		const waiters = this.promiseArray;
		this.promiseArray = [];
		waiters.forEach(({ res }) => {
			this.logger.debug('Notifying connection successful');
			res();
		});
	}

	/**
	 * The single terminal transition for a link that WAS live. The generation and
	 * phase guard is what stops a dead socket's late onclose recording CLOSED
	 * against a healthy connection, clearing its keep-alive interval and
	 * orphaning the server's subscriptions.
	 */
	private loseLink(gen: number, msg: string) {
		if (!this.isCurrent(gen) || this.link!.phase !== 'live') {
			this.logger.debug(
				`Disconnect ignored (${msg}): not the current live link`,
			);

			return;
		}
		this.logger.debug(`Disconnect error: ${msg}`);

		const link = this.link!;
		this.releaseLink(link);
		this.link = undefined;
		this.connectionStateMonitor.record(CONNECTION_CHANGE.CLOSED);
		this.rejectWaiters(new Error('Realtime link lost'));
	}

	/**
	 * One signal per failure episode. An episode opens on the first failed
	 * attempt after the last Connected and closes on the next Connected. While
	 * the device is offline it is suspended, not cleared: if the fault is still
	 * there when the network returns, it is the same unbroken episode.
	 *
	 * Every attempt dispatches an event, so a host can see attempt evidence, but
	 * only `report !== 'none'` is meant to reach a user or an error reporter.
	 */
	private recordEpisodeFailure(
		err: unknown,
		phase: LinkPhase,
		socketStatus: string,
	): RealtimeConnectionFailure {
		const { reason, detail } = classifyRealtimeFailure(err, phase);
		const now = Date.now();
		const suspended = this.episodeSuspended;

		if (!suspended) {
			if (this.episodeStartedAt === 0) {
				// The counters are already at rest: onRealtimeWorking zeroed them
				// when the last episode closed, and the socket of THIS failed attempt
				// is already counted. Nothing is reset here. A failure while offline
				// opens no episode: the first failure with a network does.
				this.episodeId += 1;
				this.episodeStartedAt = now;
			}
			this.consecutiveFailures += 1;
			this.failureReasons[reason] = (this.failureReasons[reason] ?? 0) + 1;
		}

		const diagnostics: RealtimeDiagnostics = {
			episodeId: this.episodeId,
			consecutiveFailures: this.consecutiveFailures,
			socketsAttempted: this.socketsAttemptedInEpisode,
			socketStatus,
			phase,
			msSinceEpisodeStart: this.episodeStartedAt
				? now - this.episodeStartedAt
				: 0,
			msSinceLastConnected: this.lastConnectedAt
				? now - this.lastConnectedAt
				: null,
			subscriptions: this.subscriptionObserverMap.size,
			reasons: { ...this.failureReasons },
		};

		const failure = new RealtimeConnectionFailure(reason, detail, diagnostics);
		// One line that names the failure. Every attempt leaves this trace, even
		// the ones the episode rules do not report.
		this.logger.debug(failure.message);

		let report: 'none' | 'open' | 'repeat' = 'none';
		if (!suspended) {
			if (
				this.episodeReportedAt === 0 &&
				now - this.episodeStartedAt >= REALTIME_REPORT_AFTER_MS
			) {
				report = 'open';
			} else if (
				this.episodeReportedAt !== 0 &&
				now - this.episodeReportedAt >= REALTIME_REPEAT_REPORT_MS
			) {
				report = 'repeat';
			}
		}
		if (report !== 'none') {
			this.episodeReportedAt = now;
		}

		dispatchApiEvent({
			event: REALTIME_CONNECTION_FAILURE,
			data: {
				provider: this,
				providerName: this.providerName,
				report,
				reason,
				isAuthFailure: failure.isAuthFailure,
				message: failure.message,
				diagnostics,
			},
			message: failure.message,
		});

		return failure;
	}

	/**
	 * Realtime works end to end: the episode is over. Idempotent, so the many
	 * acks of one connection give one recovery event.
	 */
	private onRealtimeWorking() {
		const reported = this.episodeReportedAt !== 0;
		const { episodeId } = this;
		const attempts = this.consecutiveFailures;
		const durationMs = this.episodeStartedAt
			? Date.now() - this.episodeStartedAt
			: 0;

		this.lastConnectedAt = Date.now();
		this.episodeStartedAt = 0;
		this.consecutiveFailures = 0;
		this.episodeReportedAt = 0;
		this.socketsAttemptedInEpisode = 0;
		this.failureReasons = {};
		this.lastFailure = undefined;

		if (reported) {
			dispatchApiEvent({
				event: REALTIME_RECOVERED,
				data: {
					provider: this,
					providerName: this.providerName,
					episodeId,
					attempts,
					durationMs,
				},
				message: 'Realtime connection recovered',
			});
		}
	}
}
