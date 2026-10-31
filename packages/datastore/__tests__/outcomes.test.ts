import { getDataStore, pause, warpTime, unwarpTime } from './helpers';
import { pruneOutcomes } from '../src/sync/outbox';

/**
 * In this testing environment, both `isNode` and `isBrowser` incorrectly
 * evaluate to `true`. Here we set `isNode` to `false`, without mocking the
 * other utils. (Same as unsentWrites.test.ts.)
 */
jest.mock('../src/datastore/utils', () => {
	const originalModule = jest.requireActual('../src/datastore/utils');

	return {
		__esModule: true,
		...originalModule,
		isNode: () => false,
	};
});

const isMutation = request => /^\s*mutation/.test(String(request.query));
const isUpdate = request => /^\s*mutation\s+operation\s*\(.*\)\s*\{\s*updatePost/.test(String(request.query)) || /updatePost/.test(String(request.query));

/**
 * Ranchr fork (-r13): every way a row leaves the outbox writes a durable
 * outcome, inside the exclusive section that deletes the row and before the
 * delete. The app reads the outcomes with `DataStore.readOutbox()`. No test
 * here listens to the Hub: the outcome is the fact, the events are not.
 */
describe('outbox outcomes (Ranchr fork -r13)', () => {
	let { DataStore, errorHandler, Post, graphqlService } = getDataStore({
		online: true,
		isNode: false,
	});
	let errors: any[];

	const cloudPosts = () => graphqlService.tables.get('Post')!;

	const waitFor = async (predicate: () => boolean, timeoutMs = 120000) => {
		const started = Date.now();
		while (!predicate()) {
			if (Date.now() - started > timeoutMs) {
				throw new Error(`waitFor: not true after ${timeoutMs} ms`);
			}
			await pause(100);
		}

		return Date.now() - started;
	};

	const waitForOutbox = async (
		predicate: (facts: { rows: any[]; outcomes: any[] }) => boolean,
		timeoutMs = 120000,
	) => {
		let facts = await DataStore.readOutbox();
		const started = Date.now();
		while (!predicate(facts)) {
			if (Date.now() - started > timeoutMs) {
				throw new Error(
					`waitForOutbox: not true after ${timeoutMs} ms: ${JSON.stringify(facts)}`,
				);
			}
			await pause(100);
			facts = await DataStore.readOutbox();
		}

		return facts;
	};

	/** Every mutation fails as the network would, until `networkUp` */
	let networkUp = true;
	const networkDown = (request, next) => {
		if (isMutation(request) && !networkUp) {
			const networkError: any = new Error('A network error has occurred.');
			networkError.name = 'NetworkError';
			throw {
				data: {},
				errors: [
					{
						message: 'A network error has occurred.',
						originalError: networkError,
					},
				],
			};
		}

		return next();
	};

	beforeEach(async () => {
		(console as any)._warn = console.warn;
		console.warn = () => {};

		({ DataStore, errorHandler, Post, graphqlService } = getDataStore({
			online: true,
			isNode: false,
		}) as any);

		errors = [];
		errorHandler.subscribe(error => errors.push(error));
		networkUp = true;

		await DataStore.start();
		warpTime();
	});

	afterEach(async () => {
		graphqlService.intercept = (request, next) => next();
		await DataStore.clear();
		unwarpTime();
		console.warn = (console as any)._warn;
	});

	test('a processed write leaves the outcome `processed` with the server version, and the row is gone', async () => {
		const post = await DataStore.save(new Post({ title: 'sent' }));

		const { rows, outcomes } = await waitForOutbox(
			facts => facts.outcomes.length === 1 && facts.rows.length === 0,
		);

		expect(rows).toEqual([]);
		expect(outcomes[0]).toMatchObject({
			model: 'Post',
			modelId: post.id,
			operation: 'Create',
			exit: 'processed',
			version: 1,
			saves: 1,
			errorType: null,
		});
		expect(JSON.parse(outcomes[0].data)).toMatchObject({
			id: post.id,
			title: 'sent',
		});
		expect(Number.isNaN(Date.parse(outcomes[0].at))).toBe(false);
		expect(cloudPosts().size).toEqual(1);
	});

	test('the outcome is written before the row is deleted, in the same exclusive section', async () => {
		const order: string[] = [];
		const storage = (DataStore as any).storage.storage;
		const originalSave = storage.save.bind(storage);
		const originalDelete = storage.delete.bind(storage);
		storage.save = async (model, ...rest) => {
			if (model?.constructor?.name === 'MutationOutcome') {
				order.push('outcome');
			}

			return originalSave(model, ...rest);
		};
		storage.delete = async (model, ...rest) => {
			if (model?.constructor?.name === 'MutationEvent') {
				order.push('row');
			}

			return originalDelete(model, ...rest);
		};

		await DataStore.save(new Post({ title: 'order' }));
		await waitForOutbox(facts => facts.outcomes.length === 1);

		expect(order).toEqual(['outcome', 'row']);
	});

	test('a refusal with no record (resolver Unauthorized) leaves the outcome `refused` with the error type', async () => {
		graphqlService.intercept = (request, next) => {
			if (isMutation(request)) {
				throw {
					data: { createPost: null },
					errors: [
						{
							path: ['createPost'],
							data: null,
							errorType: 'Unauthorized',
							message: 'Not Authorized to access createPost on type Mutation',
						},
					],
				};
			}

			return next();
		};

		const post = await DataStore.save(new Post({ title: 'not permitted' }));

		const { rows, outcomes } = await waitForOutbox(
			facts => facts.outcomes.length === 1,
		);

		expect(rows).toEqual([]);
		expect(outcomes[0]).toMatchObject({
			modelId: post.id,
			exit: 'refused',
			errorType: 'Unauthorized',
			version: null,
		});
		expect(cloudPosts().size).toEqual(0);
	});

	test('a 200 with no record and no error leaves the outcome `refused` with NullResponse (walk 9, MN1)', async () => {
		graphqlService.intercept = (request, next) => {
			if (isMutation(request)) {
				return { data: { createPost: null } };
			}

			return next();
		};

		const post = await DataStore.save(new Post({ title: 'null answer' }));

		const { outcomes } = await waitForOutbox(
			facts => facts.outcomes.length === 1,
		);

		expect(outcomes[0]).toMatchObject({
			modelId: post.id,
			exit: 'refused',
			errorType: 'NullResponse',
		});
		expect(errors.some(error => error.errorType === 'NullResponse')).toBe(
			true,
		);
	});

	test('a rejection that returns the server copy leaves the outcome `rejected` with that version', async () => {
		const post = await DataStore.save(new Post({ title: 'first' }));
		await waitForOutbox(facts => facts.outcomes.length === 1);

		graphqlService.intercept = (request, next) => {
			if (isMutation(request) && isUpdate(request)) {
				throw {
					data: { updatePost: null },
					errors: [
						{
							path: ['updatePost'],
							errorType: 'DynamoDB:ConditionalCheckFailedException',
							message: 'The conditional request failed',
							data: {
								...JSON.parse(JSON.stringify(post)),
								title: 'server copy',
								_version: 7,
								_lastChangedAt: Date.now(),
								_deleted: false,
							},
						},
					],
				};
			}

			return next();
		};

		await DataStore.save(Post.copyOf(post, draft => {
			draft.title = 'second';
		}));

		const { rows, outcomes } = await waitForOutbox(
			facts => facts.outcomes.length === 2 && facts.rows.length === 0,
		);

		expect(rows).toEqual([]);
		expect(outcomes[1]).toMatchObject({
			modelId: post.id,
			operation: 'Update',
			exit: 'rejected',
			version: 7,
		});
		expect(JSON.parse(outcomes[1].data)).toMatchObject({ title: 'second' });
	});

	test('a processed UPDATE whose answer keeps the server value of a field names it in `notApplied` (Automerge)', async () => {
		const post = await DataStore.save(new Post({ title: 'mine' }));
		await waitForOutbox(facts => facts.outcomes.length === 1);

		graphqlService.intercept = async (request, next) => {
			const result = await next();
			if (isMutation(request) && isUpdate(request) && result?.data?.updatePost) {
				result.data.updatePost = { ...result.data.updatePost, title: 'kept by the server' };
			}

			return result;
		};

		await DataStore.save(
			Post.copyOf(post, draft => {
				draft.title = 'mine 2';
			}),
		);
		const { outcomes } = await waitForOutbox(facts => facts.outcomes.length === 2);

		expect(outcomes[1]).toMatchObject({ exit: 'processed', operation: 'Update' });
		expect(JSON.parse(outcomes[1].notApplied!)).toEqual(['title']);
		expect(outcomes[0].notApplied).toBeNull();
	});

	test('a later save merged into a queued row: one outcome, `saves` 2, the merged data', async () => {
		networkUp = false;
		graphqlService.intercept = networkDown;

		// A is the head (never merged into); B's update folds into B's row
		const a = await DataStore.save(new Post({ title: 'A' }));
		const b = await DataStore.save(new Post({ title: 'B' }));
		await waitForOutbox(facts => facts.rows.length === 2);
		await DataStore.save(
			Post.copyOf(b, draft => {
				draft.title = 'B2';
			}),
		);

		const queued = await waitForOutbox(facts =>
			facts.rows.some(row => row.modelId === b.id && row.saves === 2),
		);
		expect(queued.rows.length).toEqual(2);

		networkUp = true;
		const { outcomes } = await waitForOutbox(
			facts => facts.outcomes.length === 2 && facts.rows.length === 0,
		);

		const forB = outcomes.find(outcome => outcome.modelId === b.id)!;
		expect(forB).toMatchObject({ exit: 'processed', saves: 2, operation: 'Create' });
		expect(JSON.parse(forB.data)).toMatchObject({ title: 'B2' });
		expect(outcomes.find(outcome => outcome.modelId === a.id)).toMatchObject({
			exit: 'processed',
			saves: 1,
		});
	});

	test('a DELETE of a record whose CREATE is still queued cancels the row with the outcome `cancelled`', async () => {
		networkUp = false;
		graphqlService.intercept = networkDown;

		const a = await DataStore.save(new Post({ title: 'A' }));
		const b = await DataStore.save(new Post({ title: 'B' }));
		await waitForOutbox(facts => facts.rows.length === 2);

		await DataStore.delete(b);

		const { rows, outcomes } = await waitForOutbox(
			facts => facts.outcomes.length === 1,
		);
		expect(rows.map(row => row.modelId)).toEqual([a.id]);
		expect(outcomes[0]).toMatchObject({
			modelId: b.id,
			operation: 'Create',
			exit: 'cancelled',
		});

		networkUp = true;
		await waitForOutbox(facts => facts.rows.length === 0);
		expect(cloudPosts().size).toEqual(1);
	});

	test('a row of a model this build does not have leaves as `stranded` and is reported one time', async () => {
		const { syncClasses } = require('../src/datastore/datastore');
		const { Hub } = require('@aws-amplify/core');
		const stranded: any[] = [];
		Hub.listen('datastore', ({ payload }) => {
			if (payload.event === 'outboxRowStranded') {
				stranded.push(payload.data);
			}
		});

		networkUp = false;
		graphqlService.intercept = networkDown;
		// A real row first, so that the sender runs; then the ghost row behind it
		const a = await DataStore.save(new Post({ title: 'A' }));
		await waitForOutbox(facts => facts.rows.length === 1);

		const MutationEventCtor = syncClasses.MutationEvent;
		await (DataStore as any).storage.save(
			new MutationEventCtor({
				model: 'Ghost',
				modelId: 'ghost-1',
				operation: 'Create',
				data: JSON.stringify({ id: 'ghost-1', title: 'boo' }),
				condition: '{}',
				saves: 1,
			}),
		);
		await waitForOutbox(facts => facts.rows.length === 2);

		networkUp = true;
		const { rows, outcomes } = await waitForOutbox(
			facts => facts.rows.length === 0 && facts.outcomes.length === 2,
		);

		expect(rows).toEqual([]);
		expect(outcomes.find(outcome => outcome.modelId === 'ghost-1')).toMatchObject({
			model: 'Ghost',
			exit: 'stranded',
		});
		expect(outcomes.find(outcome => outcome.modelId === a.id)).toMatchObject({
			exit: 'processed',
		});
		expect(stranded).toEqual([
			{ model: 'Ghost', operation: 'Create', modelId: 'ghost-1' },
		]);
		expect(cloudPosts().size).toEqual(1);
	});

	test('a `discarded` exit records the server version the conflict handler gave up to', async () => {
		const { syncClasses } = require('../src/datastore/datastore');
		const { MutationEventOutbox } = require('../src/sync/outbox');
		const post = await DataStore.save(new Post({ title: 'mine' }));
		await waitForOutbox(facts => facts.outcomes.length === 1);
		await DataStore.ackOutcomes(
			(await DataStore.readOutbox()).outcomes.map(outcome => outcome.id),
		);

		const exclusive = (DataStore as any).storage;
		const outbox = new MutationEventOutbox(
			(DataStore as any).storage.storage.schema,
			syncClasses.MutationEvent,
			syncClasses.MutationOutcome,
			exclusive.storage.modelInstanceCreator,
			Symbol('test'),
		);
		const row = new syncClasses.MutationEvent({
			model: 'Post',
			modelId: post.id,
			operation: 'Update',
			data: JSON.stringify({ id: post.id, title: 'mine 2', _version: 1 }),
			condition: '{}',
			saves: 1,
		});
		await exclusive.runExclusive(async s => {
			await s.save(row);
			const serverCopy = { ...JSON.parse(JSON.stringify(post)), title: 'theirs', _version: 5 };
			const dequeued = await outbox.dequeue(s, serverCopy, 'Update', row.id, {
				exit: 'discarded',
			});
			expect(dequeued?.id).toEqual(row.id);
		});

		const { rows, outcomes } = await DataStore.readOutbox();
		expect(rows).toEqual([]);
		expect(outcomes).toHaveLength(1);
		expect(outcomes[0]).toMatchObject({
			modelId: post.id,
			operation: 'Update',
			exit: 'discarded',
			version: 5,
		});
	});

	test('ackOutcomes removes the consumed outcomes and keeps the others', async () => {
		await DataStore.save(new Post({ title: 'one' }));
		await DataStore.save(new Post({ title: 'two' }));
		const { outcomes } = await waitForOutbox(
			facts => facts.outcomes.length === 2,
		);

		const removed = await DataStore.ackOutcomes([outcomes[0].id, 'no-such-id']);

		expect(removed).toEqual(1);
		const after = await DataStore.readOutbox();
		expect(after.outcomes.map(outcome => outcome.id)).toEqual([outcomes[1].id]);
	});

	test('dropOutboxRow removes a queued row with the outcome `dropped`, and the sender never sends it', async () => {
		networkUp = false;
		graphqlService.intercept = networkDown;

		const a = await DataStore.save(new Post({ title: 'stuck' }));
		const b = await DataStore.save(new Post({ title: 'behind it' }));
		const queued = await waitForOutbox(facts => facts.rows.length === 2);
		const rowA = queued.rows.find(row => row.modelId === a.id)!;

		expect(await DataStore.dropOutboxRow(rowA.id)).toBe(true);
		expect(await DataStore.dropOutboxRow(rowA.id)).toBe(false);

		const afterDrop = await DataStore.readOutbox();
		expect(afterDrop.rows.map(row => row.modelId)).toEqual([b.id]);
		expect(afterDrop.outcomes).toHaveLength(1);
		expect(afterDrop.outcomes[0]).toMatchObject({
			modelId: a.id,
			exit: 'dropped',
		});

		networkUp = true;
		await waitForOutbox(facts => facts.rows.length === 0);
		expect(cloudPosts().size).toEqual(1);
		expect(cloudPosts().has(JSON.stringify([b.id]))).toBe(true);
	});

	test('readOutbox opens the store without starting the sync', async () => {
		await DataStore.save(new Post({ title: 'before stop' }));
		await waitForOutbox(facts => facts.outcomes.length === 1);
		await DataStore.stop();

		const { outcomes } = await DataStore.readOutbox();

		expect(outcomes).toHaveLength(1);
		expect((DataStore as any).sync).toBeUndefined();
	});

	test('readLocal reads records by id in one lock turn without starting the sync', async () => {
		const a = await DataStore.save(new Post({ title: 'local a' }));
		await waitForOutbox(facts => facts.outcomes.length === 1);
		await DataStore.stop();

		const found = await DataStore.readLocal(Post, [a.id, 'no-such-id']);

		expect(found[a.id]).toMatchObject({ id: a.id, title: 'local a' });
		expect(found['no-such-id']).toBeNull();
		expect((DataStore as any).sync).toBeUndefined();
	});

	test('pruneOutcomes keeps the newest outcomes up to the limit', async () => {
		const { syncClasses } = require('../src/datastore/datastore');
		await DataStore.save(new Post({ title: 'one' }));
		await DataStore.save(new Post({ title: 'two' }));
		await DataStore.save(new Post({ title: 'three' }));
		const { outcomes } = await waitForOutbox(
			facts => facts.outcomes.length === 3,
		);

		const removed = await (DataStore as any).storage.runExclusive(s =>
			pruneOutcomes(s, syncClasses.MutationOutcome, Symbol('test'), 2),
		);

		expect(removed).toEqual(1);
		const after = await DataStore.readOutbox();
		expect(after.outcomes.map(outcome => outcome.id)).toEqual([
			outcomes[1].id,
			outcomes[2].id,
		]);
	});
});
