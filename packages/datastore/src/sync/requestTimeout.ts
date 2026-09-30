// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
import { InternalGraphQLAPI } from '@aws-amplify/api-graphql/internals';
import { CustomUserAgentDetails } from '@aws-amplify/core/internals/utils';

import { AmplifyContext } from '../types';

/**
 * AppSync stops a mutation at 30 seconds. A request that has not settled by
 * then (half-open connection after a device wake-up, for example) will never
 * settle, so we stop waiting for it.
 */
export const MUTATION_REQUEST_TIMEOUT_MS = 30000;

/**
 * A sync page is up to 1000 records. The limit covers the download of the
 * body on a slow link, not only the server execution.
 */
export const SYNC_REQUEST_TIMEOUT_MS = 180000;

/**
 * Longest wait for the app's `functionAuthProvider` to return a token.
 */
export const TOKEN_REQUEST_TIMEOUT_MS = 30000;

/**
 * A failure that says nothing definitive about the record: the transport
 * failed, the request timed out or was stopped, no token was available, or
 * the service rejected the request before the resolver ran (401, 429, 5xx).
 *
 * The mutation processor keeps such a write in the outbox and tries again.
 * The sync processor requests the page again.
 *
 * The default message is `'Network Error'` so that the error type maps like a
 * transport failure everywhere it is reported.
 */
export class TransientRequestError extends Error {
	constructor(message = 'Network Error') {
		super(message);
		this.name = 'TransientRequestError';
		Object.setPrototypeOf(this, new.target.prototype);
	}
}

/**
 * The request did not settle within its time limit.
 */
export class RequestTimeoutError extends TransientRequestError {
	constructor() {
		super();
		this.name = 'RequestTimeoutError';
	}
}

/**
 * The processor was stopped (`DataStore.stop()` / `DataStore.clear()`) while
 * the request was in flight.
 */
export class RequestStoppedError extends TransientRequestError {
	constructor() {
		super();
		this.name = 'RequestStoppedError';
	}
}

/**
 * Rejects with `RequestTimeoutError` when `promise` has not settled after
 * `timeoutMs`.
 */
export const withTimeLimit = <T>(
	promise: Promise<T>,
	timeoutMs: number,
): Promise<T> =>
	new Promise<T>((resolve, reject) => {
		const timer = setTimeout(() => {
			reject(new RequestTimeoutError());
		}, timeoutMs);

		Promise.resolve(promise).then(
			value => {
				clearTimeout(timer);
				resolve(value);
			},
			error => {
				clearTimeout(timer);
				reject(error);
			},
		);
	});

type InternalAPIType = AmplifyContext['InternalAPI'];

interface RequestLimits {
	timeoutMs: number;
	/**
	 * Resolves when the processor that owns the request stops. The request
	 * is cancelled and the returned promise rejects with `RequestStoppedError`.
	 */
	onStop?: Promise<void>;
}

/**
 * `InternalAPI.graphql()` for queries and mutations, with a time limit and a
 * stop signal. On either, the in-flight request is cancelled.
 *
 * `InternalAPI` (the DataStore-only facade in `@aws-amplify/api`) exposes no
 * `cancel()`. The cancel registry is keyed by the request promise at module
 * level, so the GraphQL API singleton can cancel a request that
 * `InternalAPI.graphql()` started.
 *
 * Not for subscriptions.
 */
export const graphqlWithTimeout = <T = any>(
	api: InternalAPIType,
	options: Parameters<InternalAPIType['graphql']>[0],
	additionalHeaders: Parameters<InternalAPIType['graphql']>[1],
	customUserAgentDetails: CustomUserAgentDetails,
	{ timeoutMs, onStop }: RequestLimits,
): Promise<T> => {
	const request = api.graphql(
		options,
		additionalHeaders,
		customUserAgentDetails,
	) as unknown as Promise<T>;

	return new Promise<T>((resolve, reject) => {
		let settled = false;

		const settle = <V>(fn: (value: V) => void) => {
			return (value: V) => {
				if (settled) {
					return;
				}
				settled = true;
				clearTimeout(timer);
				fn(value);
			};
		};

		const cancel = (error: TransientRequestError) => {
			if (settled) {
				return;
			}
			try {
				InternalGraphQLAPI.cancel(request, error.name);
			} catch (e) {
				// the request is abandoned either way
			}
			settle(reject)(error);
		};

		const timer = setTimeout(() => {
			cancel(new RequestTimeoutError());
		}, timeoutMs);

		onStop?.then(() => {
			cancel(new RequestStoppedError());
		});

		Promise.resolve(request).then(settle(resolve), settle(reject));
	});
};
