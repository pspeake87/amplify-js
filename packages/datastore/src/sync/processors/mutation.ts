// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
import { GraphQLResult } from '@aws-amplify/api';
import { InternalAPI } from '@aws-amplify/api/internals';
import {
	BackgroundProcessManager,
	Category,
	CustomUserAgentDetails,
	DataStoreAction,
	GraphQLAuthMode,
	NonRetryableError,
	retry,
} from '@aws-amplify/core/internals/utils';
import { Observable, Observer } from 'rxjs';
import { ConsoleLogger, Hub } from '@aws-amplify/core';

import { MutationEvent } from '../';
import { ModelInstanceCreator } from '../../datastore/datastore';
import { ExclusiveStorage as Storage } from '../../storage/storage';
import {
	AmplifyContext,
	AuthModeStrategy,
	ConflictHandler,
	DISCARD,
	ErrorHandler,
	GraphQLCondition,
	InternalSchema,
	ModelInstanceMetadata,
	OpType,
	PersistentModel,
	PersistentModelConstructor,
	ProcessName,
	SchemaModel,
	TypeConstructorMap,
	isModelFieldType,
	isTargetNameAssociation,
} from '../../types';
import { ID, USER, extractTargetNamesFromSrc } from '../../util';
import { MutationEventOutbox } from '../outbox';
import {
	MUTATION_REQUEST_TIMEOUT_MS,
	TransientRequestError,
	graphqlWithTimeout,
} from '../requestTimeout';
import {
	TransformerMutationType,
	buildGraphQLOperation,
	createMutationInstanceFromModelOperation,
	getModelAuthModes,
	getTokenForCustomAuth,
	resolveServiceErrorStatusCode,
} from '../utils';

import { getMutationErrorType } from './errorMaps';

const MAX_ATTEMPTS = 10;

/**
 * Longest wait between two attempts to send the head of the outbox. Short
 * enough that a write goes out soon after the network or the token returns.
 */
export const MAX_RETRY_DELAY_MS = 30 * 1000;

/**
 * After this long of failed attempts for one event, `outboxHeadStuck` is
 * dispatched on the `datastore` Hub channel one time for that event.
 */
export const OUTBOX_HEAD_STUCK_AFTER_MS = 5 * 60 * 1000;

const logger = new ConsoleLogger('DataStore');

interface MutationProcessorEvent {
	operation: TransformerMutationType;
	modelDefinition: SchemaModel;
	model: PersistentModel;
	hasMore: boolean;
}

class MutationProcessor {
	/**
	 * The observer that receives messages when mutations are successfully completed
	 * against cloud storage.
	 *
	 * A value of `undefined` signals that the sync has either been stopped or has not
	 * yet started. In this case, `isReady()` will be `false` and `resume()` will exit
	 * early.
	 */
	private observer?: Observer<MutationProcessorEvent>;
	private readonly typeQuery = new WeakMap<
		SchemaModel,
		[TransformerMutationType, string, string][]
	>();

	private processing = false;

	/**
	 * Identifies the current `resume()` loop. A loop that finds a different
	 * value has been superseded (pause / restart) and must exit without
	 * touching the outbox.
	 */
	private loopGeneration = 0;

	/**
	 * The mutation event an `Unauthorized` (HTTP 401) rejection was last
	 * reported for, so the error handler is called one time per event.
	 */
	private unauthorizedReportedFor?: string;

	/**
	 * Failed attempts for the event at the head of the outbox. Reset when an
	 * attempt succeeds or the head changes.
	 */
	private headFailures?: {
		id: string;
		since: number;
		attempts: number;
		stuckReported: boolean;
	};

	/**
	 * Wakes the current loop out of a retry delay when it is superseded.
	 */
	private supersedeCurrentLoop?: () => void;

	/**
	 * True while the current loop waits between two attempts. A new save
	 * supersedes such a loop so that the write is tried at once.
	 */
	private retrySleeping = false;

	/**
	 * Settles when the most recently started loop has exited. A new loop waits
	 * for it, so two loops never send at the same time.
	 */
	private lastLoopDone: Promise<void> = Promise.resolve();

	private runningProcesses = new BackgroundProcessManager();

	constructor(
		private readonly schema: InternalSchema,
		private readonly storage: Storage,
		private readonly userClasses: TypeConstructorMap,
		private readonly outbox: MutationEventOutbox,
		private readonly modelInstanceCreator: ModelInstanceCreator,
		private readonly _MutationEvent: PersistentModelConstructor<MutationEvent>,
		private readonly amplifyConfig: Record<string, any> = {},
		private readonly authModeStrategy: AuthModeStrategy,
		private readonly errorHandler: ErrorHandler,
		private readonly conflictHandler: ConflictHandler,
		private readonly amplifyContext: AmplifyContext,
	) {
		this.amplifyContext.InternalAPI =
			this.amplifyContext.InternalAPI || InternalAPI;
		this.generateQueries();
	}

	private generateQueries() {
		Object.values(this.schema.namespaces).forEach(namespace => {
			Object.values(namespace.models)
				.filter(({ syncable }) => syncable)
				.forEach(model => {
					const [createMutation] = buildGraphQLOperation(
						namespace,
						model,
						'CREATE',
					);
					const [updateMutation] = buildGraphQLOperation(
						namespace,
						model,
						'UPDATE',
					);
					const [deleteMutation] = buildGraphQLOperation(
						namespace,
						model,
						'DELETE',
					);

					this.typeQuery.set(model, [
						createMutation,
						updateMutation,
						deleteMutation,
					]);
				});
		});
	}

	private isReady() {
		return this.observer !== undefined;
	}

	public start(): Observable<MutationProcessorEvent> {
		this.runningProcesses = new BackgroundProcessManager();
		// a loop that belongs to the previous process manager must not continue
		this.pause();

		const observable = new Observable<MutationProcessorEvent>(observer => {
			this.observer = observer;

			try {
				this.resume();
			} catch (error) {
				logger.error('mutations processor start error', error);
				throw error;
			}

			return this.runningProcesses.addCleaner(async () => {
				// The observer has unsubscribed and/or `stop()` has been called.
				this.removeObserver();
				this.pause();
			});
		});

		return observable;
	}

	public async stop() {
		this.removeObserver();
		await this.runningProcesses.close();
		await this.runningProcesses.open();
	}

	public removeObserver() {
		this.observer?.complete?.();
		this.observer = undefined;
	}

	public async resume(): Promise<void> {
		if (this.runningProcesses.isOpen) {
			await this.runningProcesses.add(async onTerminate => {
				if (this.processing && this.retrySleeping) {
					// A loop that waits out a retry delay is superseded: the new
					// loop tries the head at once (a new save, a token that works
					// again, or a reconnect must not wait up to MAX_RETRY_DELAY_MS).
					this.pause();
				}
				if (
					this.processing ||
					!this.isReady() ||
					!this.runningProcesses.isOpen
				) {
					return;
				}
				this.processing = true;
				const generation = ++this.loopGeneration;
				const isCurrentLoop = () =>
					this.processing && generation === this.loopGeneration;
				const superseded = new Promise<void>(resolve => {
					this.supersedeCurrentLoop = resolve;
				});
				const onLoopEnd = Promise.race([onTerminate, superseded]);

				const previousLoopDone = this.lastLoopDone;
				let signalLoopDone!: () => void;
				this.lastLoopDone = new Promise<void>(resolve => {
					signalLoopDone = resolve;
				});

				try {
					// A superseded loop can still have one request in flight (at most
					// MUTATION_REQUEST_TIMEOUT_MS). Let it finish before sending, so
					// that one write is never in flight two times.
					await previousLoopDone;
					await this.drainOutbox(
						generation,
						isCurrentLoop,
						onLoopEnd,
						onTerminate,
					);
				} finally {
					signalLoopDone();
				}
			}, 'mutation resume loop');
		}
	}

	/**
	 * @param onLoopEnd resolves when the loop is superseded or stopped: a
	 * retry delay ends early
	 * @param onStop resolves when the processor stops: an in-flight request
	 * is cancelled. A superseded loop lets its request finish.
	 */
	private async drainOutbox(
		generation: number,
		isCurrentLoop: () => boolean,
		onLoopEnd: Promise<void>,
		onStop: Promise<void>,
	): Promise<void> {
		let head: MutationEvent;
		const namespaceName = USER;

		// start to drain outbox
		while (
			isCurrentLoop() &&
			this.runningProcesses.isOpen &&
			(head = await this.outbox.peek(this.storage)) !== undefined
		) {
			const { model, operation, data, condition } = head;
			const modelConstructor = this.userClasses[
				model
			] as PersistentModelConstructor<MutationEvent>;
			let result: GraphQLResult<Record<string, PersistentModel>> = undefined!;
			let opName: string = undefined!;
			let modelDefinition: SchemaModel = undefined!;

			try {
				const modelAuthModes = await getModelAuthModes({
					authModeStrategy: this.authModeStrategy,
					defaultAuthMode: this.amplifyConfig.aws_appsync_authenticationType,
					modelName: model,
					schema: this.schema,
				});

				const operationAuthModes = modelAuthModes[operation.toUpperCase()];

				let authModeAttempts = 0;
				const authModeRetry = async () => {
					try {
						logger.debug(
							`Attempting mutation with authMode: ${operationAuthModes[authModeAttempts]}`,
						);
						const response = await this.jitteredRetry(
							namespaceName,
							model,
							operation,
							data,
							condition,
							modelConstructor,
							this._MutationEvent,
							head,
							operationAuthModes[authModeAttempts],
							onLoopEnd,
							onStop,
							isCurrentLoop,
						);

						logger.debug(
							`Mutation sent successfully with authMode: ${operationAuthModes[authModeAttempts]}`,
						);

						return response;
					} catch (error) {
						if (error === undefined || error instanceof TransientRequestError) {
							// Retries were terminated (stop / pause) while the
							// transport was failing, or before the first attempt.
							// The write stays in the outbox.
							throw new TransientRequestError();
						}
						authModeAttempts++;
						if (authModeAttempts >= operationAuthModes.length) {
							logger.debug(
								`Mutation failed with authMode: ${
									operationAuthModes[authModeAttempts - 1]
								}`,
							);
							try {
								// eslint-disable-next-line @typescript-eslint/no-confusing-void-expression
								await this.errorHandler({
									recoverySuggestion:
										'Ensure app code is up to date, auth directives exist and are correct on each model, and that server-side data has not been invalidated by a schema change. If the problem persists, search for or create an issue: https://github.com/aws-amplify/amplify-js/issues',
									localModel: null!,
									message: error.message,
									model: modelConstructor.name,
									operation: opName,
									errorType: getMutationErrorType(error),
									process: ProcessName.sync,
									remoteModel: null!,
									cause: error,
								});
							} catch (e) {
								logger.error('Mutation error handler failed with:', e);
							}
							throw error;
						}
						logger.debug(
							`Mutation failed with authMode: ${
								operationAuthModes[authModeAttempts - 1]
							}. Retrying with authMode: ${
								operationAuthModes[authModeAttempts]
							}`,
						);

						return authModeRetry();
					}
				};

				[result, opName, modelDefinition] = await authModeRetry();
			} catch (error) {
				if (
					error instanceof TransientRequestError ||
					error?.message === 'RetryMutation'
				) {
					// Not a definitive answer: the write stays in the outbox.
					continue;
				}
			}

			// A definitive answer removes the event, also when this loop was
			// superseded meanwhile: the next loop must not send it again.
			const sentId = head.id;

			if (result === undefined) {
				logger.debug('done retrying');
				await this.storage.runExclusive(async storage => {
					await this.outbox.dequeue(storage, undefined, undefined, sentId);
				});
				continue;
			}

			const record = result.data![opName!];
			let hasMore = false;
			let dequeued: MutationEvent | undefined;

			await this.storage.runExclusive(async storage => {
				// using runExclusive to prevent possible race condition
				// when another record gets enqueued between dequeue and peek
				dequeued = await this.outbox.dequeue(
					storage,
					record,
					operation,
					sentId,
				);
				hasMore = (await this.outbox.peek(storage)) !== undefined;
			});

			if (!dequeued) {
				// another tab already removed this event
				continue;
			}

			this.observer?.next?.({
				operation,
				modelDefinition,
				model: record,
				hasMore,
			});
		}

		// pauses itself, unless a newer loop has taken over
		if (generation === this.loopGeneration) {
			this.pause();
		}
	}

	/**
	 * A failure that says nothing definitive about the record: the transport
	 * failed, the request timed out or was cancelled, no token was available,
	 * or the service rejected the request before the resolver ran (401, 429,
	 * 5xx). Such a write must stay in the outbox.
	 */
	private isRetryableMutationError(err: any): boolean {
		if (err instanceof TransientRequestError) {
			return true;
		}
		if (!err?.errors || err.errors.length === 0) {
			// client-side errors that don't come back in the `GraphQLError`
			// format, `NoAuthorizationHeader` (empty token) included
			return true;
		}

		const [error] = err.errors;
		const { originalError } = error;

		if (
			error.message === 'Network Error' ||
			error.message === 'A network error has occurred.' ||
			originalError?.name === 'NetworkError' ||
			originalError?.code === 'ERR_NETWORK' // refers to axios timeout error caused by device's bad network condition
		) {
			return true;
		}

		if (this.isUnauthorizedException(error)) {
			return true;
		}

		const status = resolveServiceErrorStatusCode(originalError);

		return (
			status === 429 || (status !== null && status >= 500 && status <= 599)
		);
	}

	/**
	 * HTTP 401 from the service (token rejected), as opposed to the
	 * `Unauthorized` errorType a resolver returns for a specific record.
	 */
	private isUnauthorizedException(error: any): boolean {
		return (
			error?.errorType === 'UnauthorizedException' ||
			Boolean(
				error?.originalError?.name?.startsWith?.('UnauthorizedException'),
			) ||
			resolveServiceErrorStatusCode(error?.originalError) === 401
		);
	}

	private async jitteredRetry(
		namespaceName: string,
		model: string,
		operation: TransformerMutationType,
		data: string,
		condition: string,
		modelConstructor: PersistentModelConstructor<PersistentModel>,
		MutationEventCtor: PersistentModelConstructor<MutationEvent>,
		mutationEvent: MutationEvent,
		authMode: GraphQLAuthMode,
		onLoopEnd: Promise<void>,
		onStop: Promise<void>,
		isCurrentLoop: () => boolean,
	): Promise<
		[GraphQLResult<Record<string, PersistentModel>>, string, SchemaModel]
	> {
		/**
		 * Waits between two attempts, for ever, unless the loop was
		 * superseded or stopped. Only `TransientRequestError` reaches this
		 * function: every other failure is a definitive answer.
		 */
		const delayBetweenAttempts = (attempt: number): number | false => {
			if (!isCurrentLoop()) {
				return false;
			}
			this.retrySleeping = true;

			return mutationRetryDelay(attempt);
		};

		return retry(
			async (
				retriedModel: string,
				retriedOperation: TransformerMutationType,
				retriedData: string,
				retriedCondition: string,
				retriedModelConstructor: PersistentModelConstructor<PersistentModel>,
				retiredMutationEventCtor: PersistentModelConstructor<MutationEvent>,
				retiredMutationEvent: MutationEvent,
			) => {
				const [query, variables, graphQLCondition, opName, modelDefinition] =
					this.createQueryVariables(
						namespaceName,
						retriedModel,
						retriedOperation,
						retriedData,
						retriedCondition,
					);

				this.retrySleeping = false;

				// Keeps the write in the outbox. `delayBetweenAttempts` ends the
				// retries when the loop is no longer current.
				const keepAndRetry = (cause?: unknown) => {
					this.noteHeadFailure(retiredMutationEvent, cause);

					return new TransientRequestError();
				};

				if (!isCurrentLoop()) {
					throw new TransientRequestError();
				}

				let authToken: string | undefined;
				try {
					authToken = await getTokenForCustomAuth(authMode, this.amplifyConfig);
				} catch (tokenError) {
					logger.warn('Mutation token retrieval failed', tokenError);
					throw keepAndRetry(tokenError);
				}

				const tryWith = {
					query,
					variables,
					authMode,
					authToken,
				};
				let attempt = 0;

				const opType = this.opTypeFromTransformerOperation(retriedOperation);

				const customUserAgentDetails: CustomUserAgentDetails = {
					category: Category.DataStore,
					action: DataStoreAction.GraphQl,
				};

				do {
					try {
						const result = (await graphqlWithTimeout(
							this.amplifyContext.InternalAPI,
							tryWith,
							undefined,
							customUserAgentDetails,
							{ timeoutMs: MUTATION_REQUEST_TIMEOUT_MS, onStop },
						)) as GraphQLResult<Record<string, PersistentModel>>;

						this.unauthorizedReportedFor = undefined;
						this.headFailures = undefined;

						// Use `as any` because TypeScript doesn't seem to like passing tuples
						// through generic params.
						return [result, opName, modelDefinition] as any;
					} catch (err) {
						if (this.isRetryableMutationError(err)) {
							const [unauthorized] = err?.errors ?? [];
							if (
								unauthorized &&
								this.isUnauthorizedException(unauthorized) &&
								this.unauthorizedReportedFor !== retiredMutationEvent.id
							) {
								this.unauthorizedReportedFor = retiredMutationEvent.id;
								try {
									// `localModel` is null on purpose: a report that carries
									// the record means "the server rejected and dropped this
									// change" to the apps. This write is kept and sent later.
									this.errorHandler({
										recoverySuggestion:
											'Ensure the auth token is valid. The mutation stays in the outbox and is retried.',
										localModel: null!,
										message: unauthorized.message,
										operation: retriedOperation,
										errorType: 'Unauthorized',
										process: ProcessName.mutate,
										cause: unauthorized,
										remoteModel: null!,
									});
								} catch (caughtErr) {
									logger.warn('Mutation error handler failed with:', caughtErr);
								}
							}

							throw keepAndRetry(err);
						}

						// `isRetryableMutationError` keeps every error that does not
						// come back in the `GraphQLError` format.
						const [error] = err.errors;

						if (error.errorType === 'Unauthorized') {
							throw new NonRetryableError('Unauthorized');
						}

						if (
							retriedOperation === TransformerMutationType.CREATE &&
							isConditionalCheckFailure(error)
						) {
							// The record exists on the server already: a previous
							// attempt was applied, but its response was lost or cut
							// off. Read the record back so that the dequeue learns
							// its `_version` and later updates of the record carry it.
							const serverRecord = await this.readBack(
								namespaceName,
								modelDefinition,
								variables.input,
								authMode,
								customUserAgentDetails,
								onStop,
							);

							if (serverRecord) {
								this.unauthorizedReportedFor = undefined;
								this.headFailures = undefined;

								return [
									{ data: { [opName]: serverRecord } },
									opName,
									modelDefinition,
								] as any;
							}
						}

						if (error.errorType === 'ConflictUnhandled') {
							// TODO: add on ConflictConditionalCheck error query last from server
							attempt++;
							let retryWith: PersistentModel | typeof DISCARD;

							if (attempt > MAX_ATTEMPTS) {
								retryWith = DISCARD;
							} else {
								try {
									retryWith = await this.conflictHandler!({
										modelConstructor: retriedModelConstructor,
										localModel: this.modelInstanceCreator(
											retriedModelConstructor,
											variables.input,
										),
										remoteModel: this.modelInstanceCreator(
											retriedModelConstructor,
											error.data,
										),
										operation: opType,
										attempts: attempt,
									});
								} catch (caughtErr) {
									logger.warn('conflict trycatch', caughtErr);
									continue;
								}
							}

							if (retryWith === DISCARD) {
								// Query latest from server and notify merger

								const [[, builtOpName, builtQuery]] = buildGraphQLOperation(
									this.schema.namespaces[namespaceName],
									modelDefinition,
									'GET',
								);

								const newAuthToken = await getTokenForCustomAuth(
									authMode,
									this.amplifyConfig,
								);

								const serverData = (await graphqlWithTimeout(
									this.amplifyContext.InternalAPI,
									{
										query: builtQuery,
										variables: { id: variables.input.id },
										authMode,
										authToken: newAuthToken,
									},
									undefined,
									customUserAgentDetails,
									{ timeoutMs: MUTATION_REQUEST_TIMEOUT_MS, onStop },
								)) as GraphQLResult<Record<string, PersistentModel>>;

								return [serverData, builtOpName, modelDefinition];
							}

							const namespace = this.schema.namespaces[namespaceName];

							// convert retry with to tryWith
							const updatedMutation = createMutationInstanceFromModelOperation(
								namespace.relationships!,
								modelDefinition,
								opType,
								retriedModelConstructor,
								retryWith,
								graphQLCondition,
								retiredMutationEventCtor,
								this.modelInstanceCreator,
								retiredMutationEvent.id,
							);

							await this.storage.save(updatedMutation);

							throw new NonRetryableError('RetryMutation');
						}

						try {
							this.errorHandler({
								recoverySuggestion:
									'Ensure app code is up to date, auth directives exist and are correct on each model, and that server-side data has not been invalidated by a schema change. If the problem persists, search for or create an issue: https://github.com/aws-amplify/amplify-js/issues',
								localModel: variables.input,
								message: error.message,
								operation: retriedOperation,
								errorType: getMutationErrorType(error),
								errorInfo: error.errorInfo,
								process: ProcessName.mutate,
								cause: error,
								remoteModel: error.data
									? this.modelInstanceCreator(
											retriedModelConstructor,
											error.data,
										)
									: null!,
							});
						} catch (caughtErr) {
							logger.warn('Mutation error handler failed with:', caughtErr);
						} finally {
							// Return empty tuple, dequeues the mutation
							// eslint-disable-next-line no-unsafe-finally
							return error.data
								? [{ data: { [opName]: error.data } }, opName, modelDefinition]
								: [];
						}
					}
					// eslint-disable-next-line no-unmodified-loop-condition
				} while (tryWith);
			},
			[
				model,
				operation,
				data,
				condition,
				modelConstructor,
				MutationEventCtor,
				mutationEvent,
			],
			delayBetweenAttempts,
			onLoopEnd,
		);
	}

	/**
	 * GETs a record from the server. Returns `undefined` when the record is
	 * absent or deleted. Throws `TransientRequestError` when the read failed.
	 */
	private async readBack(
		namespaceName: string,
		modelDefinition: SchemaModel,
		input: ModelInstanceMetadata,
		authMode: GraphQLAuthMode,
		customUserAgentDetails: CustomUserAgentDetails,
		onStop: Promise<void>,
	): Promise<PersistentModel | undefined> {
		const [[, opName, query]] = buildGraphQLOperation(
			this.schema.namespaces[namespaceName],
			modelDefinition,
			'GET',
		);
		const { primaryKey } =
			this.schema.namespaces[namespaceName].keys![modelDefinition.name];
		const variables = {};
		for (const pkField of primaryKey?.length ? primaryKey : [ID]) {
			variables[pkField] = input[pkField];
		}

		let response: GraphQLResult<Record<string, PersistentModel>>;
		try {
			const authToken = await getTokenForCustomAuth(
				authMode,
				this.amplifyConfig,
			);
			response = await graphqlWithTimeout(
				this.amplifyContext.InternalAPI,
				{ query, variables, authMode, authToken },
				undefined,
				customUserAgentDetails,
				{ timeoutMs: MUTATION_REQUEST_TIMEOUT_MS, onStop },
			);
		} catch (error) {
			logger.warn('Read-back of an existing record failed', error);
			throw new TransientRequestError();
		}

		const record = response?.data?.[opName];

		return record && !record._deleted ? record : undefined;
	}

	/**
	 * Counts a failed attempt for the head of the outbox. When the head has
	 * failed for `OUTBOX_HEAD_STUCK_AFTER_MS`, dispatches `outboxHeadStuck`
	 * one time so that the app can tell the user that a change is waiting.
	 */
	private noteHeadFailure(mutationEvent: MutationEvent, cause: unknown) {
		const now = Date.now();
		if (this.headFailures?.id !== mutationEvent.id) {
			this.headFailures = {
				id: mutationEvent.id,
				since: now,
				attempts: 0,
				stuckReported: false,
			};
		}
		const failures = this.headFailures;
		failures.attempts++;

		if (
			!failures.stuckReported &&
			now - failures.since >= OUTBOX_HEAD_STUCK_AFTER_MS
		) {
			failures.stuckReported = true;
			Hub.dispatch('datastore', {
				event: 'outboxHeadStuck',
				data: {
					model: mutationEvent.model,
					operation: mutationEvent.operation,
					modelId: mutationEvent.modelId,
					attempts: failures.attempts,
					since: failures.since,
					cause,
				},
			});
		}
	}

	private createQueryVariables(
		namespaceName: string,
		model: string,
		operation: TransformerMutationType,
		data: string,
		condition: string,
	): [string, Record<string, any>, GraphQLCondition, string, SchemaModel] {
		const modelDefinition = this.schema.namespaces[namespaceName].models[model];
		const { primaryKey } = this.schema.namespaces[namespaceName].keys![model];

		const auth = modelDefinition.attributes?.find(a => a.type === 'auth');
		const ownerFields: string[] = auth?.properties?.rules
			.map(rule => rule.ownerField)
			.filter(f => f) || ['owner'];

		const queriesTuples = this.typeQuery.get(modelDefinition);

		const [, opName, query] = queriesTuples!.find(
			([transformerMutationType]) => transformerMutationType === operation,
		)!;

		const { _version, ...parsedData } = JSON.parse(
			data,
		) as ModelInstanceMetadata;

		// include all the fields that comprise a custom PK if one is specified
		const deleteInput = {};
		if (primaryKey && primaryKey.length) {
			for (const pkField of primaryKey) {
				deleteInput[pkField] = parsedData[pkField];
			}
		} else {
			deleteInput[ID] = (parsedData as any).id;
		}

		let mutationInput;

		if (operation === TransformerMutationType.DELETE) {
			// For DELETE mutations, only the key(s) are included in the input
			mutationInput = deleteInput as ModelInstanceMetadata;
		} else {
			// Otherwise, we construct the mutation input with the following logic
			mutationInput = {};
			const modelFields = Object.values(modelDefinition.fields);

			for (const { name, type, association, isReadOnly } of modelFields) {
				// omit readonly fields. cloud storage doesn't need them and won't take them!
				if (isReadOnly) {
					continue;
				}

				// omit owner fields if it's `null`. cloud storage doesn't allow it.
				if (ownerFields.includes(name) && parsedData[name] === null) {
					continue;
				}

				// model fields should be stripped out from the input
				if (isModelFieldType(type)) {
					// except for belongs to relations - we need to replace them with the correct foreign key(s)
					if (
						isTargetNameAssociation(association) &&
						association.connectionType === 'BELONGS_TO'
					) {
						const targetNames: string[] | undefined =
							extractTargetNamesFromSrc(association);

						if (targetNames) {
							// instead of including the connected model itself, we add its key(s) to the mutation input
							for (const targetName of targetNames) {
								mutationInput[targetName] = parsedData[targetName];
							}
						}
					}
					continue;
				}
				// scalar fields / non-model types

				if (operation === TransformerMutationType.UPDATE) {
					if (!Object.prototype.hasOwnProperty.call(parsedData, name)) {
						// for update mutations - strip out a field if it's unchanged
						continue;
					}
				}

				// all other fields are added to the input object
				mutationInput[name] = parsedData[name];
			}
		}

		// Build mutation variables input object
		const input: ModelInstanceMetadata = {
			...mutationInput,
			_version,
		};

		const graphQLCondition = JSON.parse(condition) as GraphQLCondition;

		const variables = {
			input,
			...(operation === TransformerMutationType.CREATE
				? {}
				: {
						condition:
							Object.keys(graphQLCondition).length > 0
								? graphQLCondition
								: null,
					}),
		};

		return [query, variables, graphQLCondition, opName, modelDefinition];
	}

	private opTypeFromTransformerOperation(
		operation: TransformerMutationType,
	): OpType {
		switch (operation) {
			case TransformerMutationType.CREATE:
				return OpType.INSERT;
			case TransformerMutationType.DELETE:
				return OpType.DELETE;
			case TransformerMutationType.UPDATE:
				return OpType.UPDATE;
			case TransformerMutationType.GET: // Intentionally blank
				break;
			default:
				throw new Error(`Invalid operation ${operation}`);
		}

		// because it makes TS happy ...
		return undefined!;
	}

	public pause() {
		this.processing = false;
		this.retrySleeping = false;
		this.loopGeneration++;
		this.supersedeCurrentLoop?.();
		this.supersedeCurrentLoop = undefined;
	}
}

/**
 * DynamoDB refused the write because its condition failed. For a CREATE that
 * means the record exists already.
 */
const isConditionalCheckFailure = (error: any): boolean =>
	String(error?.errorType ?? '').includes('ConditionalCheckFailedException') ||
	/^The conditional request failed/.test(String(error?.message ?? ''));

/**
 * Wait before attempt `attempt + 1`: exponential with jitter, never more
 * than `MAX_RETRY_DELAY_MS`, never `false` (a kept write is retried for
 * ever, until the loop is superseded or stopped).
 */
export const mutationRetryDelay = (attempt: number): number => {
	const BASE_TIME_MS = 100;
	const JITTER_MS = 100;

	return Math.min(
		2 ** attempt * BASE_TIME_MS + JITTER_MS * Math.random(),
		MAX_RETRY_DELAY_MS,
	);
};

export { MutationProcessor };
