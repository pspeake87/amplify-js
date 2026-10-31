// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
import { ModelPredicateCreator } from '../predicates';
import {
	ExclusiveStorage as Storage,
	Storage as StorageClass,
	StorageFacade,
} from '../storage/storage';
import { ModelInstanceCreator } from '../datastore/datastore';
import {
	InternalSchema,
	PersistentModel,
	PersistentModelConstructor,
	QueryOne,
	SchemaModel,
} from '../types';
import {
	SYNC,
	USER,
	directedValueEquality,
	monotonicUlidFactory,
} from '../util';

import { TransformerMutationType, getIdentifierValue } from './utils';

import { MutationEvent, MutationOutcome, MutationOutcomeExit } from './index';

/**
 * Ranchr fork (-r13): how a row leaves the outbox, as the sender knows it.
 * The `version` of the outcome comes from the server record that the caller
 * hands to `dequeue`, when there is one.
 */
export interface OutboxExitInfo {
	exit: MutationOutcomeExit;
	errorType?: string;
	/** The sent fields the server's answer does not hold (an UPDATE only) */
	notApplied?: string[];
}

/**
 * The newest outcomes the store keeps when nobody acknowledges them (a web
 * build without a consumer). The mobile app acknowledges what it consumed, so
 * the store stays far below this.
 */
export const OUTCOME_LIMIT = 2000;
/** A prune runs once per this many exits, and at the first exit of a process */
const PRUNE_EVERY = 100;

const nextOutcomeId = monotonicUlidFactory();

const byId = (a: { id: string }, b: { id: string }) =>
	a.id < b.id ? -1 : a.id > b.id ? 1 : 0;

/**
 * Writes the outcome of `row` (Ranchr fork, -r13). Call it inside the
 * exclusive section that deletes the row, BEFORE the delete: a stop between
 * the two leaves the row in the outbox (it is sent again), never an exit with
 * no record of it.
 */
export const writeOutcome = async (
	storage: StorageClass,
	MutationOutcomeCtor: PersistentModelConstructor<MutationOutcome>,
	modelInstanceCreator: ModelInstanceCreator,
	ownSymbol: symbol,
	row: MutationEvent,
	info: OutboxExitInfo,
	record?: PersistentModel,
): Promise<MutationOutcome> => {
	const version = record?._version;
	const outcome = modelInstanceCreator(MutationOutcomeCtor, {
		id: nextOutcomeId(),
		model: row.model,
		modelId: row.modelId,
		operation: row.operation,
		exit: info.exit,
		data: row.data,
		version: typeof version === 'number' ? version : null,
		saves: row.saves ?? 1,
		errorType: info.errorType ?? null,
		notApplied: info.notApplied ? JSON.stringify(info.notApplied) : null,
		at: new Date().toISOString(),
	});
	await storage.save(outcome, undefined, ownSymbol);

	return outcome;
};

/** Keeps the newest `limit` outcomes. Returns how many it removed. */
export const pruneOutcomes = async (
	storage: StorageClass,
	MutationOutcomeCtor: PersistentModelConstructor<MutationOutcome>,
	ownSymbol: symbol,
	limit: number = OUTCOME_LIMIT,
): Promise<number> => {
	const all = await storage.query(MutationOutcomeCtor);
	if (all.length <= limit) {
		return 0;
	}
	const oldest = [...all].sort(byId).slice(0, all.length - limit);
	for (const outcome of oldest) {
		await storage.delete(outcome, undefined, ownSymbol);
	}

	return oldest.length;
};

// TODO: Persist deleted ids
// https://github.com/aws-amplify/amplify-js/blob/datastore-docs/packages/datastore/docs/sync-engine.md#outbox
class MutationEventOutbox {
	/** Starts one short of the limit: the first exit of a process prunes */
	private exitsSincePrune = PRUNE_EVERY - 1;

	constructor(
		private readonly schema: InternalSchema,
		private readonly _MutationEvent: PersistentModelConstructor<MutationEvent>,
		private readonly _MutationOutcome: PersistentModelConstructor<MutationOutcome>,
		private readonly modelInstanceCreator: ModelInstanceCreator,
		private readonly ownSymbol: symbol,
	) {}

	/**
	 * The head of the queue is never merged into. A sender (in this tab or in
	 * another tab on the same origin) may be sending it, and a merge would
	 * change the event under that request. A later change to the same record
	 * is queued as its own event behind the head.
	 */
	public async enqueue(
		storage: Storage,
		mutationEvent: MutationEvent,
	): Promise<void> {
		await storage.runExclusive(async s => {
			const head = await s.queryOne(this._MutationEvent, QueryOne.FIRST);

			// `id` is the key for the record in the mutationEvent;
			// `modelId` is the key for the actual record that was mutated
			const predicate = this.othersForModel(mutationEvent.modelId, head?.id);

			// Check if there are any other records with same id
			const [first] = await s.query(this._MutationEvent, predicate);

			// No other record with same modelId, so enqueue
			if (first === undefined) {
				await s.save(mutationEvent, undefined, this.ownSymbol);

				return;
			}

			// There was an enqueued mutation for the modelId, so continue
			const { operation: incomingMutationType } = mutationEvent;

			if (first.operation === TransformerMutationType.CREATE) {
				if (incomingMutationType === TransformerMutationType.DELETE) {
					// Ranchr fork (-r13): the record is deleted before its CREATE
					// was sent. Every queued row of the record goes, and the DELETE
					// is not queued. Each row gets an outcome, so the app learns
					// that the saves it carried never reach the server.
					const cancelled = await s.query(this._MutationEvent, predicate);
					for (const row of cancelled) {
						await this.settle(s, row, { exit: 'cancelled' });
					}
				} else {
					// first gets updated with the incoming mutation's data, condition intentionally skipped

					// we need to merge the fields for a create and update mutation to prevent
					// data loss, since update mutations only include changed fields
					const merged = this.mergeUserFields(first, mutationEvent);
					await s.save(
						this._MutationEvent.copyOf(first, draft => {
							draft.data = merged.data;
							draft.saves = merged.saves;
						}),
						undefined,
						this.ownSymbol,
					);
				}
			} else {
				const { condition: incomingConditionJSON } = mutationEvent;
				const incomingCondition = JSON.parse(incomingConditionJSON);
				let merged: MutationEvent;

				// If no condition
				if (Object.keys(incomingCondition).length === 0) {
					merged = this.mergeUserFields(first, mutationEvent);

					// delete all for model
					await s.delete(this._MutationEvent, predicate);
				}

				merged = merged! || mutationEvent;

				// Enqueue new one
				await s.save(merged, undefined, this.ownSymbol);
			}
		});
	}

	/**
	 * Removes the head of the queue after the server answered for it, and
	 * writes its outcome first (Ranchr fork, -r13).
	 *
	 * The head is read one time. When `expectedId` is given and the head is a
	 * different event (another tab on the same outbox removed the sent event
	 * already), nothing is removed and `undefined` is returned: the event now
	 * at the head was never sent.
	 */
	public async dequeue(
		storage: StorageClass,
		record?: PersistentModel,
		recordOp?: TransformerMutationType,
		expectedId?: string,
		info: OutboxExitInfo = { exit: 'processed' },
	): Promise<MutationEvent | undefined> {
		const head = await this.peek(storage);

		if (!head || (expectedId !== undefined && head.id !== expectedId)) {
			return undefined;
		}

		if (record) {
			await this.syncOutboxVersionsOnDequeue(storage, record, head, recordOp!);
		}

		await this.settle(storage, head, info, record);

		return head;
	}

	/**
	 * Ranchr fork (-r13): removes one row on the app's request
	 * (`DataStore.dropOutboxRow`), with the outcome `dropped`. Returns the row,
	 * or `undefined` when no row has that id.
	 */
	public async drop(
		storage: StorageClass,
		id: string,
	): Promise<MutationEvent | undefined> {
		const mutationEventModelDefinition =
			this.schema.namespaces[SYNC].models.MutationEvent;
		const [row] = await storage.query(
			this._MutationEvent,
			ModelPredicateCreator.createFromAST<MutationEvent>(
				mutationEventModelDefinition,
				{ and: [{ id: { eq: id } }] },
			),
		);

		if (!row) {
			return undefined;
		}

		await this.settle(storage, row, { exit: 'dropped' });

		return row;
	}

	public async peek(storage: StorageFacade): Promise<MutationEvent> {
		const head = await storage.queryOne(this._MutationEvent, QueryOne.FIRST);

		return head!;
	}

	/**
	 * Ranchr fork (-r13): the outcome first, then the row, inside the caller's
	 * exclusive section. A stop between the two leaves the row (it is sent
	 * again, and the second exit writes a second outcome), never an exit with
	 * no outcome.
	 */
	private async settle(
		storage: StorageClass,
		row: MutationEvent,
		info: OutboxExitInfo,
		record?: PersistentModel,
	): Promise<void> {
		const notApplied =
			info.exit === 'processed' &&
			record &&
			row.operation === TransformerMutationType.UPDATE
				? this.fieldsNotApplied(row, record)
				: undefined;
		await writeOutcome(
			storage,
			this._MutationOutcome,
			this.modelInstanceCreator,
			this.ownSymbol,
			row,
			notApplied && notApplied.length > 0 ? { ...info, notApplied } : info,
			record,
		);
		await storage.delete(row);

		this.exitsSincePrune++;
		if (this.exitsSincePrune >= PRUNE_EVERY) {
			this.exitsSincePrune = 0;
			await pruneOutcomes(storage, this._MutationOutcome, this.ownSymbol);
		}
	}

	/**
	 * Ranchr fork (-r13): the fields an UPDATE sent whose value the server's
	 * answer does not hold. AppSync's Automerge keeps the server's value of a
	 * conflicting scalar and still answers with success, so "processed" alone
	 * does not say the change was kept. The sync and timestamp fields do not
	 * count (the same rule as `syncOutboxVersionsOnDequeue`).
	 */
	private fieldsNotApplied(
		row: MutationEvent,
		record: PersistentModel,
	): string[] {
		let sent: Record<string, any>;
		try {
			sent = JSON.parse(row.data) ?? {};
		} catch {
			return [];
		}
		const {
			_version: __version,
			_lastChangedAt: __lastChangedAt,
			_deleted: __deleted,
			...outgoing
		} = sent;
		const compared = this.removeTimestampFields(row.model, { ...outgoing });
		const normalize = (value: unknown) =>
			JSON.stringify(value === undefined ? null : value);

		return Object.keys(compared).filter(
			key =>
				key !== 'id' &&
				normalize((compared as Record<string, any>)[key]) !==
					normalize((record as Record<string, any>)[key]),
		);
	}

	/**
	 * Every queued event for a record, except `exceptId`.
	 */
	private othersForModel(modelId: string, exceptId?: string) {
		const mutationEventModelDefinition =
			this.schema.namespaces[SYNC].models.MutationEvent;

		return ModelPredicateCreator.createFromAST<MutationEvent>(
			mutationEventModelDefinition,
			{
				and: [
					{ modelId: { eq: modelId } },
					...(exceptId !== undefined ? [{ id: { ne: exceptId } }] : []),
				],
			},
		);
	}

	public async getForModel<T extends PersistentModel>(
		storage: StorageFacade,
		model: T,
		userModelDefinition: SchemaModel,
	): Promise<MutationEvent[]> {
		const mutationEventModelDefinition =
			this.schema.namespaces[SYNC].models.MutationEvent;

		const modelId = getIdentifierValue(userModelDefinition, model);

		const mutationEvents = await storage.query(
			this._MutationEvent,
			ModelPredicateCreator.createFromAST(mutationEventModelDefinition, {
				and: { modelId: { eq: modelId } },
			}),
		);

		return mutationEvents;
	}

	public async getModelIds(storage: StorageFacade): Promise<Set<string>> {
		const mutationEvents = await storage.query(this._MutationEvent);

		const result = new Set<string>();

		mutationEvents.forEach(({ modelId }) => result.add(modelId));

		return result;
	}

	// applies _version from the AppSync mutation response to other items
	// in the mutation queue with the same id
	// see https://github.com/aws-amplify/amplify-js/pull/7354 for more details
	private async syncOutboxVersionsOnDequeue(
		storage: StorageClass,
		record: PersistentModel,
		head: PersistentModel,
		recordOp: string,
	): Promise<void> {
		if (head?.operation !== recordOp) {
			return;
		}

		const { _version, _lastChangedAt, _deleted, ..._incomingData } = record;
		const incomingData = this.removeTimestampFields(head.model, _incomingData);

		const data = JSON.parse(head.data);

		if (!data) {
			return;
		}

		const {
			_version: __version,
			_lastChangedAt: __lastChangedAt,
			_deleted: __deleted,
			..._outgoingData
		} = data;
		const outgoingData = this.removeTimestampFields(head.model, _outgoingData);

		// Don't sync the version when the data in the response does not match the data
		// in the request, i.e., when there's a handled conflict
		//
		// NOTE: `incomingData` contains all the fields in the record received from AppSync
		// and `outgoingData` only contains updated fields sent to AppSync
		// If all send data isn't matched in the returned data then the update was rejected
		// by AppSync and we should not update the version on other outbox entries for this
		// object
		if (!directedValueEquality(outgoingData, incomingData, true)) {
			return;
		}

		const userModelDefinition = this.schema.namespaces.user.models[head.model];

		const recordId = getIdentifierValue(userModelDefinition, record);

		const predicate = this.othersForModel(recordId, head.id);

		const outdatedMutations = await storage.query(
			this._MutationEvent,
			predicate,
		);

		if (!outdatedMutations.length) {
			return;
		}

		const reconciledMutations = outdatedMutations.map(m => {
			const oldData = JSON.parse(m.data);

			const newData = { ...oldData, _version, _lastChangedAt };

			return this._MutationEvent.copyOf(m, draft => {
				draft.data = JSON.stringify(newData);
			});
		});

		await storage.delete(this._MutationEvent, predicate);

		await Promise.all(
			reconciledMutations.map(async m =>
				storage.save(m, undefined, this.ownSymbol),
			),
		);
	}

	private mergeUserFields(
		previous: MutationEvent,
		current: MutationEvent,
	): MutationEvent {
		const { _version, _lastChangedAt, _deleted, ...previousData } = JSON.parse(
			previous.data,
		);

		const {
			_version: __version,
			_lastChangedAt: __lastChangedAt,
			_deleted: __deleted,
			...currentData
		} = JSON.parse(current.data);

		const data = JSON.stringify({
			_version,
			_lastChangedAt,
			_deleted,
			...previousData,
			...currentData,
		});

		return this.modelInstanceCreator(this._MutationEvent, {
			...current,
			data,
			// Ranchr fork (-r13): the merged row answers for the saves of both
			saves: (previous.saves ?? 1) + (current.saves ?? 1),
		});
	}

	/*
	if a model is using custom timestamp fields
	the custom field names will be stored in the model attributes

	e.g.
	"attributes": [
    {
			"type": "model",
			"properties": {
				"timestamps": {
					"createdAt": "createdOn",
					"updatedAt": "updatedOn"
				}
			}
    }
	]
	*/
	private removeTimestampFields(
		model: string,
		record: PersistentModel,
	): PersistentModel {
		const CREATED_AT_DEFAULT_KEY = 'createdAt';
		const UPDATED_AT_DEFAULT_KEY = 'updatedAt';

		let createdTimestampKey = CREATED_AT_DEFAULT_KEY;
		let updatedTimestampKey = UPDATED_AT_DEFAULT_KEY;

		const modelAttributes = this.schema.namespaces[USER].models[
			model
		].attributes?.find(attr => attr.type === 'model');
		const timestampFieldsMap = modelAttributes?.properties?.timestamps;

		if (timestampFieldsMap) {
			createdTimestampKey = timestampFieldsMap[CREATED_AT_DEFAULT_KEY];
			updatedTimestampKey = timestampFieldsMap[UPDATED_AT_DEFAULT_KEY];
		}

		delete (record as Record<string, any>)[createdTimestampKey];
		delete (record as Record<string, any>)[updatedTimestampKey];

		return record;
	}
}

export { MutationEventOutbox };
