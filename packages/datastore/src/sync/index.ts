// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
import { BackgroundProcessManager } from '@aws-amplify/core/internals/utils';
import { ConsoleLogger, Hub } from '@aws-amplify/core';
import { Observable, SubscriptionLike, filter, of } from 'rxjs';
import {
	ConnectionState,
	CONNECTION_STATE_CHANGE as PUBSUB_CONNECTION_STATE_CHANGE,
	CONTROL_MSG as PUBSUB_CONTROL_MSG,
} from '@aws-amplify/api-graphql';

import { ModelInstanceCreator } from '../datastore/datastore';
import { ModelPredicateCreator } from '../predicates';
import { ExclusiveStorage as Storage } from '../storage/storage';
import {
	AmplifyContext,
	AuthModeStrategy,
	ConflictHandler,
	ControlMessageType,
	ErrorHandler,
	InternalSchema,
	ManagedIdentifier,
	ModelInit,
	ModelInstanceMetadata,
	ModelPredicate,
	MutableModel,
	NamespaceResolver,
	OpType,
	OptionallyManagedIdentifier,
	PersistentModelConstructor,
	ProcessName,
	SchemaModel,
	SchemaNamespace,
	TypeConstructorMap,
} from '../types';
import type { __modelMeta__ } from '../types';
import { SYNC, USER, getNow } from '../util';

import DataStoreConnectivity from './datastoreConnectivity';
import { ModelMerger } from './merger';
import { MutationEventOutbox } from './outbox';
import { MutationProcessor } from './processors/mutation';
import { CONTROL_MSG, SubscriptionProcessor } from './processors/subscription';
import { SyncProcessor } from './processors/sync';
import {
	TransformerMutationType,
	createMutationInstanceFromModelOperation,
	getIdentifierValue,
	predicateToGraphQLCondition,
} from './utils';

const logger = new ConsoleLogger('DataStore');

/**
 * Longest time the mutation processor waits for the sync queries after the
 * engine comes online.
 */
export const SENDER_START_MAX_WAIT_MS = 10000;

const ownSymbol = Symbol('sync');

interface StartParams {
	fullSyncInterval: number;
}

export declare class MutationEvent {
	readonly [__modelMeta__]: {
		identifier: OptionallyManagedIdentifier<MutationEvent, 'id'>;
	};

	public readonly id: string;
	public readonly model: string;
	public readonly operation: TransformerMutationType;
	public readonly modelId: string;
	public readonly condition: string;
	public readonly data: string;
	constructor(init: ModelInit<MutationEvent>);
	static copyOf(
		src: MutationEvent,
		mutator: (draft: MutableModel<MutationEvent>) => void | MutationEvent,
	): MutationEvent;
}

export declare class ModelMetadata {
	readonly [__modelMeta__]: {
		identifier: ManagedIdentifier<ModelMetadata, 'id'>;
	};

	public readonly id: string;
	public readonly namespace: string;
	public readonly model: string;
	public readonly fullSyncInterval: number;
	public readonly lastSync?: number;
	public readonly lastFullSync?: number;
	public readonly lastSyncPredicate?: null | string;
	constructor(init: ModelInit<ModelMetadata>);
	static copyOf(
		src: ModelMetadata,
		mutator: (draft: MutableModel<ModelMetadata>) => void | ModelMetadata,
	): ModelMetadata;
}

export enum ControlMessage {
	SYNC_ENGINE_STORAGE_SUBSCRIBED = 'storageSubscribed',
	SYNC_ENGINE_SUBSCRIPTIONS_ESTABLISHED = 'subscriptionsEstablished',
	SYNC_ENGINE_SYNC_QUERIES_STARTED = 'syncQueriesStarted',
	SYNC_ENGINE_SYNC_QUERIES_READY = 'syncQueriesReady',
	SYNC_ENGINE_MODEL_SYNCED = 'modelSynced',
	SYNC_ENGINE_OUTBOX_MUTATION_ENQUEUED = 'outboxMutationEnqueued',
	SYNC_ENGINE_OUTBOX_MUTATION_PROCESSED = 'outboxMutationProcessed',
	SYNC_ENGINE_OUTBOX_STATUS = 'outboxStatus',
	SYNC_ENGINE_NETWORK_STATUS = 'networkStatus',
	SYNC_ENGINE_READY = 'ready',
}

export class SyncEngine {
	private online = false;

	private readonly syncQueriesProcessor: SyncProcessor;
	private readonly subscriptionsProcessor: SubscriptionProcessor;
	private readonly mutationsProcessor: MutationProcessor;
	private readonly modelMerger: ModelMerger;
	private readonly outbox: MutationEventOutbox;
	private readonly datastoreConnectivity: DataStoreConnectivity;
	private readonly modelSyncedStatus = new WeakMap<
		PersistentModelConstructor<any>,
		boolean
	>();

	private unsleepSyncQueriesObservable: (() => void) | null;
	private waitForSleepState: Promise<void>;
	private syncQueriesObservableStartSleeping: (
		value?: void | PromiseLike<void>,
	) => void;

	private stopDisruptionListener: () => void;
	private connectionDisrupted = false;

	/**
	 * True from the start of `stop()` until the engine is ready to restart.
	 * Nothing new (the mutation processor in particular) starts meanwhile.
	 */
	private stopping = false;

	/**
	 * `_version` of every record merged from a mutation response or a realtime
	 * message since the current sync round started, keyed by model and id.
	 * A sync page the server read before such a merge carries an older
	 * version of the record; those items are dropped so that the page does
	 * not revert the newer local record.
	 */
	private readonly mergedVersions = new Map<string, number>();

	private runningProcesses: BackgroundProcessManager;

	public getModelSyncedStatus(
		modelConstructor: PersistentModelConstructor<any>,
	): boolean {
		return this.modelSyncedStatus.get(modelConstructor)!;
	}

	constructor(
		private readonly schema: InternalSchema,
		private readonly namespaceResolver: NamespaceResolver,
		private readonly modelClasses: TypeConstructorMap,
		private readonly userModelClasses: TypeConstructorMap,
		private readonly storage: Storage,
		private readonly modelInstanceCreator: ModelInstanceCreator,
		conflictHandler: ConflictHandler,
		private readonly errorHandler: ErrorHandler,
		private readonly syncPredicates: WeakMap<
			SchemaModel,
			ModelPredicate<any> | null
		>,
		private readonly amplifyConfig: Record<string, any> = {},
		private readonly authModeStrategy: AuthModeStrategy,
		private readonly amplifyContext: AmplifyContext,
		private readonly connectivityMonitor?: DataStoreConnectivity,
	) {
		this.runningProcesses = new BackgroundProcessManager();
		this.waitForSleepState = new Promise(resolve => {
			this.syncQueriesObservableStartSleeping = resolve;
		});

		const MutationEventCtor = this.modelClasses
			.MutationEvent as PersistentModelConstructor<MutationEvent>;

		this.outbox = new MutationEventOutbox(
			this.schema,
			MutationEventCtor,
			modelInstanceCreator,
			ownSymbol,
		);

		this.modelMerger = new ModelMerger(this.outbox, ownSymbol);

		this.syncQueriesProcessor = new SyncProcessor(
			this.schema,
			this.syncPredicates,
			this.amplifyConfig,
			this.authModeStrategy,
			errorHandler,
			this.amplifyContext,
		);

		this.subscriptionsProcessor = new SubscriptionProcessor(
			this.schema,
			this.syncPredicates,
			this.amplifyConfig,
			this.authModeStrategy,
			errorHandler,
			this.amplifyContext,
		);

		this.mutationsProcessor = new MutationProcessor(
			this.schema,
			this.storage,
			this.userModelClasses,
			this.outbox,
			this.modelInstanceCreator,
			MutationEventCtor,
			this.amplifyConfig,
			this.authModeStrategy,
			errorHandler,
			conflictHandler,
			this.amplifyContext,
		);

		this.datastoreConnectivity =
			this.connectivityMonitor || new DataStoreConnectivity();
	}

	start(params: StartParams) {
		return new Observable<ControlMessageType<ControlMessage>>(observer => {
			logger.log('starting sync engine...');

			let subscriptions: SubscriptionLike[] = [];

			this.runningProcesses.add(async onStartTerminate => {
				try {
					await this.setupModels(params);
				} catch (err) {
					observer.error(err);

					return;
				}

				let terminated = false;
				onStartTerminate.then(() => {
					terminated = true;
				});

				// Resolves when the mutation processor is running (or the engine
				// is offline): a save may then wake the processor.
				let signalSenderReady!: () => void;
				const senderReady = new Promise<void>(resolve => {
					signalSenderReady = resolve;
				});

				// this is awaited at the bottom. so, we don't need to register
				// this explicitly with the context. it's already contained.
				// Resolves when the first sync round is done (or the engine is
				// offline): `SYNC_ENGINE_READY` then tells the app that the local
				// store holds the server data.
				const startPromise = new Promise<void>((resolve, reject) => {
					const doneStarting = () => {
						signalSenderReady();
						resolve();
					};
					const failedStarting = reject;

					this.datastoreConnectivity.status().subscribe(
						async ({ online }) =>
							this.runningProcesses.isOpen &&
							this.runningProcesses.add(async onTerminate => {
								// From offline to online
								if (online && !this.online) {
									this.online = online;

									observer.next({
										type: ControlMessage.SYNC_ENGINE_NETWORK_STATUS,
										data: {
											active: this.online,
										},
									});

									this.stopDisruptionListener = this.startDisruptionListener();
									// #region GraphQL Subscriptions
									const [ctlSubsObservable, dataSubsObservable] =
										this.subscriptionsProcessor.start();

									try {
										await new Promise<void>((_resolve, _reject) => {
											onTerminate.then(_reject);
											const ctlSubsSubscription = ctlSubsObservable.subscribe({
												next: msg => {
													if (msg === CONTROL_MSG.CONNECTED) {
														_resolve();
													}
												},
												error: err => {
													_reject(err);
													const handleDisconnect = this.disconnectionHandler();
													handleDisconnect(err);
												},
											});

											subscriptions.push(ctlSubsSubscription);
										});
									} catch (err) {
										observer.error(err);
										failedStarting();

										return;
									}

									logger.log('Realtime ready');

									observer.next({
										type: ControlMessage.SYNC_ENGINE_SUBSCRIPTIONS_ESTABLISHED,
									});

									// #endregion

									// #region Base & Sync queries
									let syncTerminated = false;
									try {
										// The outbox must not depend on the sync queries: a sync
										// that is slow (or never completes) would otherwise keep
										// every queued mutation from being sent. Start the
										// mutation processor when the sync queries are ready or
										// after SENDER_START_MAX_WAIT_MS, whichever is first. The
										// sync continues in both cases.
										await new Promise<void>((_resolve, _reject) => {
											let raceDone = false;
											// eslint-disable-next-line prefer-const
											let senderStartTimer: ReturnType<typeof setTimeout>;
											const endRace = (error?: unknown) => {
												if (raceDone) {
													return false;
												}
												raceDone = true;
												clearTimeout(senderStartTimer);
												error === undefined ? _resolve() : _reject(error);

												return true;
											};
											senderStartTimer = setTimeout(() => {
												logger.warn(
													'Sync queries not ready; starting the mutation processor',
												);
												endRace();
											}, SENDER_START_MAX_WAIT_MS);
											onTerminate.then(() => {
												syncTerminated = true;
												endRace();
											});

											const syncQuerySubscription =
												this.syncQueriesObservable().subscribe({
													next: message => {
														const { type } = message;

														if (
															type ===
															ControlMessage.SYNC_ENGINE_SYNC_QUERIES_READY
														) {
															endRace();
															doneStarting();
														}

														observer.next(message);
													},
													complete: () => {
														endRace();
														doneStarting();
													},
													error: error => {
														if (!endRace(error)) {
															// the mutation processor already started
															observer.error(error);
														}
													},
												});

											if (syncQuerySubscription) {
												subscriptions.push(syncQuerySubscription);
											}
										});
									} catch (error) {
										observer.error(error);
										failedStarting();

										return;
									}

									if (syncTerminated || this.stopping) {
										// the engine is stopping: do not start the mutation processor
										doneStarting();

										return;
									}
									// #endregion

									// #region process mutations (outbox)
									subscriptions.push(
										this.mutationsProcessor
											.start()
											.subscribe(({ modelDefinition, model: item, hasMore }) =>
												this.runningProcesses.add(async () => {
													const modelConstructor = this.userModelClasses[
														modelDefinition.name
													] as PersistentModelConstructor<any>;

													const model = this.modelInstanceCreator(
														modelConstructor,
														item,
													);

													await this.reportJobFailure(
														'merge of a mutation response',
														modelDefinition.name,
														item,
														() =>
															this.storage.runExclusive(async storage => {
																await this.modelMerger.merge(
																	storage,
																	model,
																	modelDefinition,
																);
																this.noteMergedVersion(modelDefinition, item);
															}),
													);

													observer.next({
														type: ControlMessage.SYNC_ENGINE_OUTBOX_MUTATION_PROCESSED,
														data: {
															model: modelConstructor,
															element: model,
														},
													});

													observer.next({
														type: ControlMessage.SYNC_ENGINE_OUTBOX_STATUS,
														data: {
															isEmpty: !hasMore,
														},
													});
												}, 'mutation processor event'),
											),
									);
									// #endregion

									// #region Merge subscriptions buffer
									subscriptions.push(
										dataSubsObservable!.subscribe(
											([_transformerMutationType, modelDefinition, item]) =>
												this.runningProcesses.add(async () => {
													const modelConstructor = this.userModelClasses[
														modelDefinition.name
													] as PersistentModelConstructor<any>;

													const model = this.modelInstanceCreator(
														modelConstructor,
														item,
													);

													await this.reportJobFailure(
														'merge of a realtime message',
														modelDefinition.name,
														item,
														() =>
															this.storage.runExclusive(async storage => {
																await this.modelMerger.merge(
																	storage,
																	model,
																	modelDefinition,
																);
																this.noteMergedVersion(modelDefinition, item);
															}),
													);
												}, 'subscription dataSubsObservable event'),
										),
									);
									// #endregion

									signalSenderReady();
								} else if (!online) {
									this.online = online;

									observer.next({
										type: ControlMessage.SYNC_ENGINE_NETWORK_STATUS,
										data: {
											active: this.online,
										},
									});

									subscriptions.forEach(sub => {
										sub.unsubscribe();
									});
									subscriptions = [];

									doneStarting();
								}
							}, 'datastore connectivity event'),
					);
				});

				this.storage
					.observe(null, null, ownSymbol)
					.pipe(
						filter(({ model }) => {
							const modelDefinition = this.getModelDefinition(model);

							return modelDefinition.syncable === true;
						}),
					)
					.subscribe({
						next: async ({ opType, model, element, condition }) =>
							this.runningProcesses.add(async () => {
								const namespace =
									this.schema.namespaces[this.namespaceResolver(model)];
								const MutationEventConstructor = this.modelClasses
									.MutationEvent as PersistentModelConstructor<MutationEvent>;
								const modelDefinition = this.getModelDefinition(model);
								const graphQLCondition = predicateToGraphQLCondition(
									condition!,
									modelDefinition,
								);
								const mutationEvent = createMutationInstanceFromModelOperation(
									namespace.relationships!,
									this.getModelDefinition(model),
									opType,
									model,
									element,
									graphQLCondition,
									MutationEventConstructor,
									this.modelInstanceCreator,
								);

								// A save that does not reach the outbox is a lost change:
								// the record is in the local store, but it is never sent.
								const queued = await this.reportJobFailure(
									'enqueue of a local change',
									modelDefinition.name,
									element,
									async () => {
										await this.outbox.enqueue(this.storage, mutationEvent);

										return true;
									},
								);

								if (!queued) {
									return;
								}

								observer.next({
									type: ControlMessage.SYNC_ENGINE_OUTBOX_MUTATION_ENQUEUED,
									data: {
										model,
										element,
									},
								});

								observer.next({
									type: ControlMessage.SYNC_ENGINE_OUTBOX_STATUS,
									data: {
										isEmpty: false,
									},
								});

								await senderReady;

								// Set by the this.datastoreConnectivity.status().subscribe() loop
								if (this.online) {
									this.mutationsProcessor.resume();
								}
							}, 'storage event'),
					});

				observer.next({
					type: ControlMessage.SYNC_ENGINE_STORAGE_SUBSCRIBED,
				});

				const hasMutationsInOutbox =
					(await this.outbox.peek(this.storage)) === undefined;
				observer.next({
					type: ControlMessage.SYNC_ENGINE_OUTBOX_STATUS,
					data: {
						isEmpty: hasMutationsInOutbox,
					},
				});

				// a stop before the first sync round is done must not hang here
				await Promise.race([startPromise, onStartTerminate]);

				if (terminated) {
					return;
				}

				observer.next({
					type: ControlMessage.SYNC_ENGINE_READY,
				});
			}, 'sync start');
		});
	}

	private async getModelsMetadataWithNextFullSync(
		currentTimeStamp: number,
	): Promise<Map<SchemaModel, [string, number]>> {
		const modelLastSync = new Map<SchemaModel, [string, number]>(
			(
				await this.runningProcesses.add(
					() => this.getModelsMetadata(),
					'sync/index getModelsMetadataWithNextFullSync',
				)
			).map(
				({ namespace, model, lastSync, lastFullSync, fullSyncInterval }) => {
					const nextFullSync = lastFullSync! + fullSyncInterval;
					const syncFrom =
						!lastFullSync || nextFullSync < currentTimeStamp
							? 0 // perform full sync if expired
							: lastSync; // perform delta sync

					return [
						this.schema.namespaces[namespace].models[model],
						[namespace, syncFrom!],
					];
				},
			),
		);

		return modelLastSync;
	}

	private syncQueriesObservable(): Observable<
		ControlMessageType<ControlMessage>
	> {
		if (!this.online) {
			return of<ControlMessageType<ControlMessage>>({} as any); // TODO(v6): fix this
		}

		return new Observable<ControlMessageType<ControlMessage>>(observer => {
			let syncQueriesSubscription: SubscriptionLike;

			this.runningProcesses.isOpen &&
				this.runningProcesses.add(async onTerminate => {
					let terminated = false;

					while (!observer.closed && !terminated) {
						// merges before this point are in the server data this round reads
						this.mergedVersions.clear();

						const count = new WeakMap<
							PersistentModelConstructor<any>,
							{
								new: number;
								updated: number;
								deleted: number;
							}
						>();

						const modelLastSync = await this.getModelsMetadataWithNextFullSync(
							Date.now(),
						);
						const paginatingModels = new Set(modelLastSync.keys());

						let lastFullSyncStartedAt: number;
						let syncInterval: number;

						let start: number;
						let syncDuration: number;
						let lastStartedAt: number;
						await new Promise<void>((resolve, _reject) => {
							if (!this.runningProcesses.isOpen) resolve();
							onTerminate.then(() => {
								resolve();
							});
							syncQueriesSubscription = this.syncQueriesProcessor
								.start(modelLastSync)
								.subscribe({
									next: async ({
										namespace,
										modelDefinition,
										items,
										done,
										startedAt,
										isFullSync,
									}) => {
										const modelConstructor = this.userModelClasses[
											modelDefinition.name
										] as PersistentModelConstructor<any>;

										if (!count.has(modelConstructor)) {
											count.set(modelConstructor, {
												new: 0,
												updated: 0,
												deleted: 0,
											});

											start = getNow();
											lastStartedAt =
												lastStartedAt === undefined
													? startedAt
													: Math.max(lastStartedAt, startedAt);
										}

										/**
										 * If there are mutations in the outbox for a given id, those need to be
										 * merged individually. Otherwise, we can merge them in batches.
										 */
										await this.storage.runExclusive(async storage => {
											const idsInOutbox =
												await this.outbox.getModelIds(storage);

											const oneByOne: ModelInstanceMetadata[] = [];
											const page = this.dropStaleItems(
												modelDefinition,
												items,
											).filter(item => {
												const itemId = getIdentifierValue(
													modelDefinition,
													item,
												);

												if (!idsInOutbox.has(itemId)) {
													return true;
												}

												oneByOne.push(item);

												return false;
											});

											const opTypeCount: [any, OpType][] = [];

											for (const item of oneByOne) {
												const opType = await this.modelMerger.merge(
													storage,
													item,
													modelDefinition,
												);

												if (opType !== undefined) {
													opTypeCount.push([item, opType]);
												}
											}

											opTypeCount.push(
												...(await this.modelMerger.mergePage(
													storage,
													modelConstructor,
													page,
													modelDefinition,
												)),
											);

											const counts = count.get(modelConstructor)!;

											opTypeCount.forEach(([, opType]) => {
												switch (opType) {
													case OpType.INSERT:
														counts.new++;
														break;
													case OpType.UPDATE:
														counts.updated++;
														break;
													case OpType.DELETE:
														counts.deleted++;
														break;
													default:
														throw new Error(`Invalid opType ${opType}`);
												}
											});
										});

										if (done) {
											const { name: modelName } = modelDefinition;

											// #region update last sync for type
											let modelMetadata = await this.getModelMetadata(
												namespace,
												modelName,
											);

											const { lastFullSync, fullSyncInterval } = modelMetadata;

											syncInterval = fullSyncInterval;

											lastFullSyncStartedAt =
												lastFullSyncStartedAt === undefined
													? lastFullSync!
													: Math.max(
															lastFullSyncStartedAt,
															isFullSync ? startedAt : lastFullSync!,
														);

											modelMetadata = (
												this.modelClasses
													.ModelMetadata as PersistentModelConstructor<ModelMetadata>
											).copyOf(modelMetadata, draft => {
												draft.lastSync = startedAt;
												draft.lastFullSync = isFullSync
													? startedAt
													: modelMetadata.lastFullSync;
											});

											await this.storage.save(
												modelMetadata,
												undefined,
												ownSymbol,
											);
											// #endregion

											const counts = count.get(modelConstructor);

											this.modelSyncedStatus.set(modelConstructor, true);

											observer.next({
												type: ControlMessage.SYNC_ENGINE_MODEL_SYNCED,
												data: {
													model: modelConstructor,
													isFullSync,
													isDeltaSync: !isFullSync,
													counts,
												},
											});

											paginatingModels.delete(modelDefinition);

											if (paginatingModels.size === 0) {
												syncDuration = getNow() - start;
												resolve();
												observer.next({
													type: ControlMessage.SYNC_ENGINE_SYNC_QUERIES_READY,
												});
												syncQueriesSubscription.unsubscribe();
											}
										}
									},
									error: error => {
										observer.error(error);
									},
								});

							observer.next({
								type: ControlMessage.SYNC_ENGINE_SYNC_QUERIES_STARTED,
								data: {
									models: Array.from(paginatingModels).map(({ name }) => name),
								},
							});
						});

						// null is cast to 0 resulting in unexpected behavior.
						// undefined in arithmetic operations results in NaN also resulting in unexpected behavior.
						// If lastFullSyncStartedAt is null this is the first sync.
						// Assume lastStartedAt is is also newest full sync.
						let msNextFullSync;
						if (!lastFullSyncStartedAt!) {
							msNextFullSync = syncInterval! - syncDuration!;
						} else {
							msNextFullSync =
								lastFullSyncStartedAt! +
								syncInterval! -
								(lastStartedAt! + syncDuration!);
						}

						logger.debug(
							`Next fullSync in ${msNextFullSync / 1000} seconds. (${new Date(
								Date.now() + msNextFullSync,
							)})`,
						);

						// TODO: create `BackgroundProcessManager.sleep()` ... but, need to put
						// a lot of thought into what that contract looks like to
						//  support possible use-cases:
						//
						//  1. non-cancelable
						//  2. cancelable, unsleep on exit()
						//  3. cancelable, throw Error on exit()
						//  4. cancelable, callback first on exit()?
						//  5. ... etc. ? ...
						//
						// TLDR; this is a lot of complexity here for a sleep(),
						// but, it's not clear to me yet how to support an
						// extensible, centralized cancelable `sleep()` elegantly.
						await this.runningProcesses.add(async onRunningProcessTerminate => {
							let _sleepTimer;
							let unsleep;

							const sleep = new Promise(resolve => {
								unsleep = resolve;
								_sleepTimer = setTimeout(unsleep, msNextFullSync);
							});

							onRunningProcessTerminate.then(() => {
								terminated = true;
								this.syncQueriesObservableStartSleeping();
								unsleep();
							});

							this.unsleepSyncQueriesObservable = unsleep;
							this.syncQueriesObservableStartSleeping();

							return sleep;
						}, 'syncQueriesObservable sleep');

						this.unsleepSyncQueriesObservable = null;
						this.waitForSleepState = new Promise(resolve => {
							this.syncQueriesObservableStartSleeping = resolve;
						});
					}
				}, 'syncQueriesObservable main');
		});
	}

	/**
	 * Runs one background job. `BackgroundProcessManager` swallows a job's
	 * rejection, so a failure (a broken IndexedDB, for example) would be
	 * silent. It is reported through the error handler instead; the job
	 * then ends (nothing waits for its result).
	 */
	private async reportJobFailure<T>(
		what: string,
		modelName: string,
		record: Readonly<Record<string, any>> | undefined,
		job: () => Promise<T>,
	): Promise<T | undefined> {
		try {
			return await job();
		} catch (error) {
			logger.error(`Sync engine: ${what} failed`, error);
			try {
				// eslint-disable-next-line @typescript-eslint/no-confusing-void-expression
				await this.errorHandler({
					recoverySuggestion:
						'The local database refused a write. Reload the page; if it persists, the browser storage is broken.',
					localModel: (record ?? null) as any,
					message: `${what} failed: ${(error as any)?.message ?? error}`,
					model: modelName,
					operation: what,
					errorType: 'Unknown',
					process: ProcessName.sync,
					remoteModel: null!,
					cause: error as Error,
				});
			} catch (e) {
				logger.error('Sync engine error handler failed with:', e);
			}

			return undefined;
		}
	}

	private mergedVersionKey(
		modelDefinition: SchemaModel,
		item: ModelInstanceMetadata,
	): string {
		return `${modelDefinition.name}|${getIdentifierValue(modelDefinition, item)}`;
	}

	private noteMergedVersion(
		modelDefinition: SchemaModel,
		item: Readonly<Record<string, any>>,
	) {
		if (typeof item?._version === 'number') {
			this.mergedVersions.set(
				this.mergedVersionKey(modelDefinition, item as ModelInstanceMetadata),
				item._version,
			);
		}
	}

	/**
	 * Drops the items of a sync page that are older than a record merged from
	 * a mutation response or a realtime message during this round.
	 */
	private dropStaleItems(
		modelDefinition: SchemaModel,
		items: ModelInstanceMetadata[],
	): ModelInstanceMetadata[] {
		if (this.mergedVersions.size === 0) {
			return items;
		}

		return items.filter(item => {
			const merged = this.mergedVersions.get(
				this.mergedVersionKey(modelDefinition, item),
			);

			return merged === undefined || !(item._version <= merged);
		});
	}

	private disconnectionHandler(): (msg: string) => void {
		return (msg: string) => {
			// This implementation is tied to AWSAppSyncRealTimeProvider 'Connection closed', 'Timeout disconnect' msg
			if (
				PUBSUB_CONTROL_MSG.CONNECTION_CLOSED === msg ||
				PUBSUB_CONTROL_MSG.TIMEOUT_DISCONNECT === msg
			) {
				this.datastoreConnectivity.socketDisconnected();
			}
		};
	}

	public unsubscribeConnectivity() {
		this.datastoreConnectivity.unsubscribe();
	}

	/**
	 * Stops all subscription activities and resolves when all activies report
	 * that they're disconnected, done retrying, etc..
	 */
	public async stop() {
		logger.debug('stopping sync engine');
		this.stopping = true;

		/**
		 * Gracefully disconnecting subscribers first just prevents *more* work
		 * from entering the pipelines.
		 */
		this.unsubscribeConnectivity();

		/**
		 * Stop listening for websocket connection disruption
		 */
		this.stopDisruptionListener && this.stopDisruptionListener();

		/**
		 * aggressively shut down any lingering background processes.
		 * some of this might be semi-redundant with unsubscribing. however,
		 * unsubscribing doesn't allow us to wait for settling.
		 * (Whereas `stop()` does.)
		 */

		await this.mutationsProcessor.stop();
		await this.subscriptionsProcessor.stop();
		await this.datastoreConnectivity.stop();
		await this.syncQueriesProcessor.stop();
		await this.runningProcesses.close();
		await this.runningProcesses.open();
		this.stopping = false;

		logger.debug('sync engine stopped and ready to restart');
	}

	private async setupModels(params: StartParams) {
		const { fullSyncInterval } = params;
		const ModelMetadataConstructor = this.modelClasses
			.ModelMetadata as PersistentModelConstructor<ModelMetadata>;

		const models: [string, SchemaModel][] = [];
		let savedModel;

		Object.values(this.schema.namespaces).forEach(namespace => {
			Object.values(namespace.models)
				.filter(({ syncable }) => syncable)
				.forEach(model => {
					models.push([namespace.name, model]);
					if (namespace.name === USER) {
						const modelConstructor = this.userModelClasses[
							model.name
						] as PersistentModelConstructor<any>;
						this.modelSyncedStatus.set(modelConstructor, false);
					}
				});
		});

		const promises = models.map(async ([namespace, model]) => {
			const modelMetadata = await this.getModelMetadata(namespace, model.name);
			const syncPredicate = ModelPredicateCreator.getPredicates(
				this.syncPredicates.get(model)!,
				false,
			);
			const lastSyncPredicate = syncPredicate
				? JSON.stringify(syncPredicate)
				: null;

			if (modelMetadata === undefined) {
				[[savedModel]] = await this.storage.save(
					this.modelInstanceCreator(ModelMetadataConstructor, {
						model: model.name,
						namespace,
						lastSync: null!,
						fullSyncInterval,
						lastFullSync: null!,
						lastSyncPredicate,
					}),
					undefined,
					ownSymbol,
				);
			} else {
				const prevSyncPredicate = modelMetadata.lastSyncPredicate
					? modelMetadata.lastSyncPredicate
					: null;
				const syncPredicateUpdated = prevSyncPredicate !== lastSyncPredicate;

				[[savedModel]] = await this.storage.save(
					ModelMetadataConstructor.copyOf(modelMetadata, draft => {
						draft.fullSyncInterval = fullSyncInterval;
						// perform a base sync if the syncPredicate changed in between calls to DataStore.start
						// ensures that the local store contains all the data specified by the syncExpression
						if (syncPredicateUpdated) {
							draft.lastSync = null!;
							draft.lastFullSync = null!;
							(draft.lastSyncPredicate as any) = lastSyncPredicate;
						}
					}),
				);
			}

			return savedModel;
		});

		const result: Record<string, ModelMetadata> = {};
		for (const modelMetadata of await Promise.all(promises)) {
			const { model: modelName } = modelMetadata;

			result[modelName] = modelMetadata;
		}

		return result;
	}

	private async getModelsMetadata(): Promise<ModelMetadata[]> {
		const ModelMetadataCtor = this.modelClasses
			.ModelMetadata as PersistentModelConstructor<ModelMetadata>;

		const modelsMetadata = await this.storage.query(ModelMetadataCtor);

		return modelsMetadata;
	}

	private async getModelMetadata(
		namespace: string,
		model: string,
	): Promise<ModelMetadata> {
		const ModelMetadataCtor = this.modelClasses
			.ModelMetadata as PersistentModelConstructor<ModelMetadata>;

		const predicate = ModelPredicateCreator.createFromAST<ModelMetadata>(
			this.schema.namespaces[SYNC].models[ModelMetadataCtor.name],
			{ and: [{ namespace: { eq: namespace } }, { model: { eq: model } }] },
		);

		const [modelMetadata] = await this.storage.query(
			ModelMetadataCtor,
			predicate,
			{
				page: 0,
				limit: 1,
			},
		);

		return modelMetadata;
	}

	private getModelDefinition(
		modelConstructor: PersistentModelConstructor<any>,
	): SchemaModel {
		const namespaceName = this.namespaceResolver(modelConstructor);

		const modelDefinition =
			this.schema.namespaces[namespaceName].models[modelConstructor.name];

		return modelDefinition;
	}

	static getNamespace() {
		const namespace: SchemaNamespace = {
			name: SYNC,
			relationships: {},
			enums: {
				OperationType: {
					name: 'OperationType',
					values: ['CREATE', 'UPDATE', 'DELETE'],
				},
			},
			nonModels: {},
			models: {
				MutationEvent: {
					name: 'MutationEvent',
					pluralName: 'MutationEvents',
					syncable: false,
					fields: {
						id: {
							name: 'id',
							type: 'ID',
							isRequired: true,
							isArray: false,
						},
						model: {
							name: 'model',
							type: 'String',
							isRequired: true,
							isArray: false,
						},
						data: {
							name: 'data',
							type: 'String',
							isRequired: true,
							isArray: false,
						},
						modelId: {
							name: 'modelId',
							type: 'String',
							isRequired: true,
							isArray: false,
						},
						operation: {
							name: 'operation',
							type: {
								enum: 'Operationtype',
							},
							isArray: false,
							isRequired: true,
						},
						condition: {
							name: 'condition',
							type: 'String',
							isArray: false,
							isRequired: true,
						},
					},
				},
				ModelMetadata: {
					name: 'ModelMetadata',
					pluralName: 'ModelsMetadata',
					syncable: false,
					fields: {
						id: {
							name: 'id',
							type: 'ID',
							isRequired: true,
							isArray: false,
						},
						namespace: {
							name: 'namespace',
							type: 'String',
							isRequired: true,
							isArray: false,
						},
						model: {
							name: 'model',
							type: 'String',
							isRequired: true,
							isArray: false,
						},
						lastSync: {
							name: 'lastSync',
							type: 'Int',
							isRequired: false,
							isArray: false,
						},
						lastFullSync: {
							name: 'lastFullSync',
							type: 'Int',
							isRequired: false,
							isArray: false,
						},
						fullSyncInterval: {
							name: 'fullSyncInterval',
							type: 'Int',
							isRequired: true,
							isArray: false,
						},
						lastSyncPredicate: {
							name: 'lastSyncPredicate',
							type: 'String',
							isRequired: false,
							isArray: false,
						},
					},
				},
			},
		};

		return namespace;
	}

	/**
	 * listen for websocket connection disruption
	 *
	 * May indicate there was a period of time where messages
	 * from AppSync were missed. A sync needs to be triggered to
	 * retrieve the missed data.
	 */
	private startDisruptionListener() {
		return Hub.listen('api', (data: any) => {
			if (
				data.source === 'PubSub' &&
				data.payload.event === PUBSUB_CONNECTION_STATE_CHANGE
			) {
				const connectionState = data.payload.data
					.connectionState as ConnectionState;

				switch (connectionState) {
					// Do not need to listen for ConnectionDisruptedPendingNetwork
					// Normal network reconnection logic will handle the sync
					case ConnectionState.ConnectionDisrupted:
						this.connectionDisrupted = true;
						break;

					case ConnectionState.Connected:
						if (this.connectionDisrupted) {
							this.scheduleSync();
						}
						this.connectionDisrupted = false;
						break;
				}
			}
		});
	}

	/*
	 * Schedule a sync to start when syncQueriesObservable enters sleep state
	 * Start sync immediately if syncQueriesObservable is already in sleep state
	 */
	private scheduleSync() {
		return (
			this.runningProcesses.isOpen &&
			this.runningProcesses.add(() =>
				this.waitForSleepState.then(() => {
					// unsleepSyncQueriesObservable will be set if waitForSleepState has resolved
					this.unsleepSyncQueriesObservable!();
				}),
			)
		);
	}
}
