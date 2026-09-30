// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
import { GraphQLResult } from '@aws-amplify/api';
import { InternalAPI } from '@aws-amplify/api/internals';
import { Observable } from 'rxjs';
import {
	BackgroundProcessManager,
	Category,
	CustomUserAgentDetails,
	DataStoreAction,
	GraphQLAuthMode,
	NonRetryableError,
	jitteredExponentialRetry,
} from '@aws-amplify/core/internals/utils';
import { ConsoleLogger, Hub } from '@aws-amplify/core';

import {
	AmplifyContext,
	AuthModeStrategy,
	ErrorHandler,
	GraphQLFilter,
	InternalSchema,
	ModelInstanceMetadata,
	ModelPredicate,
	PredicatesGroup,
	ProcessName,
	SchemaModel,
} from '../../types';
import {
	SYNC_REQUEST_TIMEOUT_MS,
	TransientRequestError,
	graphqlWithTimeout,
} from '../requestTimeout';
import {
	buildGraphQLOperation,
	getClientSideAuthError,
	getForbiddenError,
	getModelAuthModes,
	getTokenForCustomAuth,
	predicateToGraphQLFilter,
} from '../utils';
import { ModelPredicateCreator } from '../../predicates';

import { getSyncErrorType } from './errorMaps';

const logger = new ConsoleLogger('DataStore');

class SyncProcessor {
	private readonly typeQuery = new WeakMap<SchemaModel, [string, string]>();

	private runningProcesses = new BackgroundProcessManager();

	constructor(
		private readonly schema: InternalSchema,
		private readonly syncPredicates: WeakMap<
			SchemaModel,
			ModelPredicate<any> | null
		>,
		private readonly amplifyConfig: Record<string, any> = {},
		private readonly authModeStrategy: AuthModeStrategy,
		private readonly errorHandler: ErrorHandler,
		private readonly amplifyContext: AmplifyContext,
	) {
		amplifyContext.InternalAPI = amplifyContext.InternalAPI || InternalAPI;
		this.generateQueries();
	}

	private generateQueries() {
		Object.values(this.schema.namespaces).forEach(namespace => {
			Object.values(namespace.models)
				.filter(({ syncable }) => syncable)
				.forEach(model => {
					const [[, ...opNameQuery]] = buildGraphQLOperation(
						namespace,
						model,
						'LIST',
					);

					this.typeQuery.set(model, opNameQuery);
				});
		});
	}

	private graphqlFilterFromPredicate(model: SchemaModel): GraphQLFilter {
		if (!this.syncPredicates) {
			return null!;
		}
		const predicatesGroup: PredicatesGroup<any> =
			ModelPredicateCreator.getPredicates(
				this.syncPredicates.get(model)!,
				false,
			)!;

		if (!predicatesGroup) {
			return null!;
		}

		return predicateToGraphQLFilter(predicatesGroup);
	}

	private async retrievePage<T extends ModelInstanceMetadata>(
		modelDefinition: SchemaModel,
		lastSync: number,
		nextToken: string,
		limit: number = null!,
		filter: GraphQLFilter,
		onTerminate: Promise<void>,
	): Promise<{ nextToken: string; startedAt: number; items: T[] }> {
		const [opName, query] = this.typeQuery.get(modelDefinition)!;

		const variables = {
			limit,
			nextToken,
			lastSync,
			filter,
		};

		const modelAuthModes = await getModelAuthModes({
			authModeStrategy: this.authModeStrategy,
			defaultAuthMode: this.amplifyConfig.aws_appsync_authenticationType,
			modelName: modelDefinition.name,
			schema: this.schema,
		});

		// sync only needs the READ auth mode(s)
		const readAuthModes = modelAuthModes.READ;

		let authModeAttempts = 0;
		const authModeRetry = async () => {
			if (!this.runningProcesses.isOpen) {
				throw new Error(
					'sync.retreievePage termination was requested. Exiting.',
				);
			}

			try {
				logger.debug(
					`Attempting sync with authMode: ${readAuthModes[authModeAttempts]}`,
				);
				const response = await this.jitteredRetry<T>({
					query,
					variables,
					opName,
					modelDefinition,
					authMode: readAuthModes[authModeAttempts],
					onTerminate,
				});
				logger.debug(
					`Sync successful with authMode: ${readAuthModes[authModeAttempts]}`,
				);

				return response;
			} catch (error) {
				authModeAttempts++;
				if (authModeAttempts >= readAuthModes.length) {
					const authMode = readAuthModes[authModeAttempts - 1];
					logger.debug(`Sync failed with authMode: ${authMode}`, error);
					throw error;
				}
				logger.debug(
					`Sync failed with authMode: ${
						readAuthModes[authModeAttempts - 1]
					}. Retrying with authMode: ${readAuthModes[authModeAttempts]}`,
				);

				return authModeRetry();
			}
		};

		const { data } = await authModeRetry();

		const { [opName]: opResult } = data;

		const { items, nextToken: newNextToken, startedAt } = opResult;

		return {
			nextToken: newNextToken,
			startedAt,
			items,
		};
	}

	private async jitteredRetry<T>({
		query,
		variables,
		opName,
		modelDefinition,
		authMode,
		onTerminate,
	}: {
		query: string;
		variables: { limit: number; lastSync: number; nextToken: string };
		opName: string;
		modelDefinition: SchemaModel;
		authMode: GraphQLAuthMode;
		onTerminate: Promise<void>;
	}): Promise<
		GraphQLResult<
			Record<
				string,
				{
					items: T[];
					nextToken: string;
					startedAt: number;
				}
			>
		>
	> {
		return jitteredExponentialRetry(
			async (retriedQuery, retriedVariables) => {
				try {
					// throws `TransientRequestError` when the token is missing
					const authToken = await getTokenForCustomAuth(
						authMode,
						this.amplifyConfig,
					);

					const customUserAgentDetails: CustomUserAgentDetails = {
						category: Category.DataStore,
						action: DataStoreAction.GraphQl,
					};

					return await graphqlWithTimeout(
						this.amplifyContext.InternalAPI,
						{
							query: retriedQuery,
							variables: retriedVariables,
							authMode,
							authToken,
						},
						undefined,
						customUserAgentDetails,
						{ timeoutMs: SYNC_REQUEST_TIMEOUT_MS, onStop: onTerminate },
					);
				} catch (error) {
					if (error instanceof TransientRequestError) {
						// a transport failure, not an answer: retry with backoff
						throw error;
					}

					// A client-side auth error from the API layer means the auth
					// config is missing (an empty lambda token is thrown as
					// `TransientRequestError` above, before the request). That is
					// definitive for this round: the caller reports it.
					const clientSideAuthErrorMessage = getClientSideAuthError(error);
					if (clientSideAuthErrorMessage) {
						logger.error('Sync processor auth config error:', error);
						throw new NonRetryableError(clientSideAuthErrorMessage);
					}

					// A token the service rejected (401) or a forbidden request
					// (403) is not an answer about the data. Every model uses
					// custom (lambda) auth, so the token is expected to work
					// again: retry with backoff.
					const forbiddenErrorMessage = getForbiddenError(error);
					if (forbiddenErrorMessage) {
						logger.warn('Sync request not authorized, retrying:', error);
						throw new TransientRequestError(forbiddenErrorMessage);
					}

					const hasItems = Boolean(error?.data?.[opName]?.items);

					const unauthorized =
						error?.errors &&
						(error.errors as [any]).some(
							err => err.errorType === 'Unauthorized',
						);

					const otherErrors =
						error?.errors &&
						(error.errors as [any]).filter(
							err => err.errorType !== 'Unauthorized',
						);

					const result = error;

					if (hasItems) {
						result.data[opName].items = result.data[opName].items.filter(
							item => item !== null,
						);
					}

					if (hasItems && otherErrors?.length) {
						await Promise.all(
							otherErrors.map(async err => {
								try {
									// eslint-disable-next-line @typescript-eslint/no-confusing-void-expression
									await this.errorHandler({
										recoverySuggestion:
											'Ensure app code is up to date, auth directives exist and are correct on each model, and that server-side data has not been invalidated by a schema change. If the problem persists, search for or create an issue: https://github.com/aws-amplify/amplify-js/issues',
										localModel: null!,
										message: err.message,
										model: modelDefinition.name,
										operation: opName,
										errorType: getSyncErrorType(err),
										process: ProcessName.sync,
										remoteModel: null!,
										cause: err,
									});
								} catch (e) {
									logger.error('Sync error handler failed with:', e);
								}
							}),
						);
						Hub.dispatch('datastore', {
							event: 'nonApplicableDataReceived',
							data: {
								errors: otherErrors,
								modelName: modelDefinition.name,
							},
						});
					}

					/**
					 * Handle $util.unauthorized() in resolver request mapper, which responses with something
					 * like this:
					 *
					 * ```
					 * {
					 * 	data: { syncYourModel: null },
					 * 	errors: [
					 * 		{
					 * 			path: ['syncLegacyJSONComments'],
					 * 			data: null,
					 * 			errorType: 'Unauthorized',
					 * 			errorInfo: null,
					 * 			locations: [{ line: 2, column: 3, sourceName: null }],
					 * 			message:
					 * 				'Not Authorized to access syncYourModel on type Query',
					 * 			},
					 * 		],
					 * 	}
					 * ```
					 *
					 * The correct handling for this is to signal that we've encountered a non-retryable error,
					 * since the server has responded with an auth error and *NO DATA* at this point.
					 */
					if (unauthorized) {
						this.errorHandler({
							recoverySuggestion:
								'Ensure app code is up to date, auth directives exist and are correct on each model, and that server-side data has not been invalidated by a schema change. If the problem persists, search for or create an issue: https://github.com/aws-amplify/amplify-js/issues',
							localModel: null!,
							message: error.message,
							model: modelDefinition.name,
							operation: opName,
							errorType: getSyncErrorType(error.errors[0]),
							process: ProcessName.sync,
							remoteModel: null!,
							cause: error,
						});
						throw new NonRetryableError(error);
					}

					if (result.data?.[opName]?.items?.length) {
						return result;
					}

					throw error;
				}
			},
			[query, variables],
			undefined,
			onTerminate,
		);
	}

	start(
		typesLastSync: Map<SchemaModel, [string, number]>,
	): Observable<SyncModelPage> {
		const { maxRecordsToSync, syncPageSize } = this.amplifyConfig;
		const parentPromises = new Map<string, Promise<void>>();
		const observable = new Observable<SyncModelPage>(observer => {
			const sortedTypesLastSyncs = Object.values(this.schema.namespaces).reduce(
				(map, namespace) => {
					for (const modelName of Array.from(
						namespace.modelTopologicalOrdering!.keys(),
					)) {
						const typeLastSync = typesLastSync.get(namespace.models[modelName]);
						map.set(namespace.models[modelName], typeLastSync!);
					}

					return map;
				},
				new Map<SchemaModel, [string, number]>(),
			);

			const allModelsReady = Array.from(sortedTypesLastSyncs.entries())
				.filter(([{ syncable }]) => syncable)
				.map(
					([modelDefinition, [namespace, lastSync]]) =>
						this.runningProcesses.isOpen &&
						this.runningProcesses.add(async onTerminate => {
							let done = false;
							let nextToken: string = null!;
							let startedAt: number = null!;
							let items: ModelInstanceMetadata[] = null!;

							let recordsReceived = 0;
							const filter = this.graphqlFilterFromPredicate(modelDefinition);

							const parents = this.schema.namespaces[
								namespace
							].modelTopologicalOrdering!.get(modelDefinition.name);
							const promises = parents!.map(parent =>
								parentPromises.get(`${namespace}_${parent}`),
							);

							// eslint-disable-next-line no-async-promise-executor
							const promise = new Promise<void>(async resolve => {
								await Promise.all(promises);

								do {
									/**
									 * If `runningProcesses` is not open, it means that the sync processor has been
									 * stopped (for example by calling `DataStore.clear()` upstream) and has not yet
									 * finished terminating and/or waiting for its background processes to complete.
									 */
									if (!this.runningProcesses.isOpen) {
										logger.debug(
											`Sync processor has been stopped, terminating sync for ${modelDefinition.name}`,
										);

										resolve();

										return;
									}

									const limit = Math.min(
										maxRecordsToSync - recordsReceived,
										syncPageSize,
									);

									/**
									 * It's possible that `retrievePage` will fail.
									 * If it does fail, continue merging the rest of the data,
									 * and invoke the error handler for non-applicable data.
									 */
									try {
										({ items, nextToken, startedAt } = await this.retrievePage(
											modelDefinition,
											lastSync,
											nextToken,
											limit,
											filter,
											onTerminate,
										));
									} catch (error) {
										/**
										 * The sync processor was stopped while the request was in
										 * flight (`DataStore.clear()`, for example). The local store
										 * is being torn down: do not report, do not emit the page.
										 */
										if (!this.runningProcesses.isOpen) {
											resolve();

											return;
										}

										try {
											// eslint-disable-next-line @typescript-eslint/no-confusing-void-expression
											await this.errorHandler({
												recoverySuggestion:
													'Ensure app code is up to date, auth directives exist and are correct on each model, and that server-side data has not been invalidated by a schema change. If the problem persists, search for or create an issue: https://github.com/aws-amplify/amplify-js/issues',
												localModel: null!,
												message: error.message,
												model: modelDefinition.name,
												operation: null!,
												errorType: getSyncErrorType(error),
												process: ProcessName.sync,
												remoteModel: null!,
												cause: error,
											});
										} catch (e) {
											logger.error('Sync error handler failed with:', e);
										}
										/**
										 * If there's an error, this model fails, but the rest of the sync should
										 * continue. To facilitate this, we explicitly mark this model as `done`
										 * with no items and allow the loop to continue organically. This ensures
										 * all callbacks (subscription messages) happen as normal, so anything
										 * waiting on them knows the model is as done as it can be.
										 *
										 * `nextToken` is cleared so that a failing page 2+ is not
										 * requested again for ever. `startedAt` is cleared so that
										 * `lastSync` stays unset and the next round fetches the
										 * model again from the start.
										 */
										done = true;
										items = [];
										nextToken = null!;
										startedAt = null!;
									}

									/**
									 * The sync processor was stopped while the request was in
									 * flight (a request that timed out, for example). The local
									 * store is being torn down: do not emit the page.
									 */
									if (!this.runningProcesses.isOpen) {
										resolve();

										return;
									}

									recordsReceived += items.length;

									done =
										nextToken === null || recordsReceived >= maxRecordsToSync;

									observer.next({
										namespace,
										modelDefinition,
										items,
										done,
										startedAt,
										isFullSync: !lastSync,
									});
								} while (!done);

								resolve();
							});

							parentPromises.set(
								`${namespace}_${modelDefinition.name}`,
								promise,
							);

							await promise;
						}, `adding model ${modelDefinition.name}`),
				);

			Promise.all(allModelsReady as Promise<any>[]).then(() => {
				observer.complete();
			});
		});

		return observable;
	}

	async stop() {
		logger.debug('stopping sync processor');
		await this.runningProcesses.close();
		await this.runningProcesses.open();
		logger.debug('sync processor stopped');
	}
}

export interface SyncModelPage {
	namespace: string;
	modelDefinition: SchemaModel;
	items: ModelInstanceMetadata[];
	startedAt: number;
	done: boolean;
	isFullSync: boolean;
}

export { SyncProcessor };
