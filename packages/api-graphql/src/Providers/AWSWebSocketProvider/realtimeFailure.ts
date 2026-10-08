// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
import { NonRetryableError } from '@aws-amplify/core/internals/utils';

import { GraphQLAuthError } from '../../types';
import { CONTROL_MSG } from '../../types/PubSub';

/**
 * Where one connect attempt is. A failure is only readable with its phase.
 * There is no closing phase: a deliberate close either happens or it does
 * not, and a link stays live until it does.
 */
export type LinkPhase = 'headers' | 'open' | 'handshake' | 'live';

export type RealtimeFailureReason =
	| 'no-auth-token' // the app could not produce a credential
	| 'auth-rejected' // the service rejected the credential (401/403)
	| 'token-timeout' // the header stage did not answer in time
	| 'token-error' // the token provider threw
	| 'connect-deadline' // the socket never reached onopen
	| 'handshake-deadline' // connection_init was never acked
	| 'handshake-rejected' // connection_error from the service
	| 'socket-closed' // onclose/onerror during connect or handshake
	| 'config-error' // BadRequestException, a malformed query
	| 'stale-attempt' // superseded by a newer attempt
	| 'attempt-budget' // the whole attempt used its budget (see constants.ts)
	| 'unknown';

export interface RealtimeDiagnostics {
	episodeId: number;
	consecutiveFailures: number;
	/** 0 means the failure is before any socket; >=1 means a socket did open. */
	socketsAttempted: number;
	socketStatus: string;
	phase: LinkPhase;
	msSinceEpisodeStart: number;
	msSinceLastConnected: number | null;
	subscriptions: number;
	reasons: Partial<Record<RealtimeFailureReason, number>>;
}

/**
 * A newer attempt owns the link. Non-retryable on purpose: `retry()` must stop
 * at once, because the attempt this error belongs to is already over.
 */
export class StaleAttemptError extends NonRetryableError {
	constructor() {
		super('Realtime attempt superseded');
		this.name = 'StaleAttemptError';
		Object.setPrototypeOf(this, new.target.prototype);
	}
}

/**
 * A phase did not answer in time. Non-retryable: a deadline means "this step
 * will not answer", and the attempt is already over. A fast failure (a refused
 * socket) is retryable and does retry inside the attempt.
 */
export class PhaseDeadlineError extends NonRetryableError {
	constructor(
		public readonly phase: LinkPhase,
		public readonly ms: number,
	) {
		super(`Realtime ${phase} did not finish in ${ms} ms`);
		this.name = 'PhaseDeadlineError';
		Object.setPrototypeOf(this, new.target.prototype);
	}
}

/**
 * The whole attempt used its budget: fast socket failures retried until it ran
 * out, or a phase has no deadline. The diagnostics carry the phase.
 */
export class AttemptBudgetError extends NonRetryableError {
	constructor(ms: number, phase: LinkPhase) {
		super(`Realtime connect did not settle in ${ms} ms (phase ${phase})`);
		this.name = 'AttemptBudgetError';
		Object.setPrototypeOf(this, new.target.prototype);
	}
}

export const isAuthReason = (reason: RealtimeFailureReason): boolean =>
	reason === 'no-auth-token' || reason === 'auth-rejected';

/**
 * One reason per failure.
 *
 * Classification is by NAME and by the message constant, never by class
 * identity: the empty-token throw is raised in @aws-amplify/datastore
 * (MissingAuthTokenError) and also in this package's own authHeaders.ts, and
 * both carry GraphQLAuthError.NO_AUTH_TOKEN as the message. A marker that works
 * across the package boundary beats a shared base class here.
 */
export const classifyRealtimeFailure = (
	err: unknown,
	phase: LinkPhase,
): { reason: RealtimeFailureReason; detail: string } => {
	const e = (err ?? {}) as {
		name?: string;
		message?: string;
		errorType?: string;
		errorCode?: number;
	};
	const name = e.name ?? '';
	const message = String(e.message ?? '');
	const errorType = e.errorType ?? '';

	// The messages name the client-side fact. None of these failures reached
	// the service, so none may read like a service exception: "AppSync rejected
	// the token" is the false lead an investigation had to rule out. The host
	// reads `reason` and `isAuthFailure`, never the message.
	if (
		name === 'MissingAuthTokenError' ||
		message.includes(GraphQLAuthError.NO_AUTH_TOKEN)
	) {
		return {
			reason: 'no-auth-token',
			detail: 'the app produced no auth token',
		};
	}
	if (name === 'RequestTimeoutError') {
		return {
			reason: 'token-timeout',
			detail: 'the token provider did not answer in time',
		};
	}
	if (err instanceof PhaseDeadlineError) {
		if (err.phase === 'open') {
			return { reason: 'connect-deadline', detail: message };
		}
		if (err.phase === 'handshake') {
			return { reason: 'handshake-deadline', detail: message };
		}
		if (err.phase === 'headers') {
			return { reason: 'token-timeout', detail: message };
		}

		return { reason: 'unknown', detail: message };
	}
	if (err instanceof AttemptBudgetError) {
		return { reason: 'attempt-budget', detail: message };
	}
	if (err instanceof StaleAttemptError) {
		return { reason: 'stale-attempt', detail: message };
	}
	if (
		errorType === 'UnauthorizedException' ||
		e.errorCode === 401 ||
		e.errorCode === 403
	) {
		return {
			reason: 'auth-rejected',
			detail: `the service rejected the credential (${
				errorType || e.errorCode
			})`,
		};
	}
	if (errorType === 'BadRequestException' || e.errorCode === 400) {
		return {
			reason: 'config-error',
			detail: errorType || 'BadRequestException',
		};
	}
	if (name === 'TransientRequestError') {
		return { reason: 'token-error', detail: message };
	}
	if (errorType) {
		return { reason: 'handshake-rejected', detail: errorType };
	}
	if (phase === 'open' || phase === 'handshake') {
		return {
			reason: 'socket-closed',
			detail: message || `the socket closed during ${phase}`,
		};
	}

	return { reason: 'unknown', detail: message || 'unknown realtime failure' };
};

/**
 * One failure object. The message prefix stays CONTROL_MSG.CONNECTION_FAILED so
 * every existing string check keeps working.
 */
export class RealtimeConnectionFailure extends Error {
	constructor(
		readonly reason: RealtimeFailureReason,
		readonly detail: string,
		readonly diagnostics: RealtimeDiagnostics,
	) {
		super(`${CONTROL_MSG.CONNECTION_FAILED}: ${detail}`);
		this.name = 'RealtimeConnectionFailure';
		Object.setPrototypeOf(this, new.target.prototype);
	}

	get isAuthFailure() {
		return isAuthReason(this.reason);
	}
}
