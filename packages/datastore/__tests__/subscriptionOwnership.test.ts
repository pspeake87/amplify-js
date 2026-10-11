/**
 * Subscription ownership (bug-realtime-subscription-ownership.md Part A).
 *
 * The sync engine starts its realtime round once per DataStore start and ends
 * it once per stop or clear. A connectivity change never touches the
 * subscriptions: the AppSync realtime provider keeps every one across a
 * socket loss and replays it on the next link. Before, every offline→online
 * transition tore all of them down and started a new round on the same live
 * socket; a burst of transitions stacked rounds on one connection until
 * AppSync refused the 201st.
 */
import { Observable } from 'rxjs';
import {
	pause,
	getDataStore,
	waitForEmptyOutbox,
	waitForDataStoreReady,
	waitForSyncQueriesReady,
	warpTime,
	unwarpTime,
} from './helpers';

jest.mock('../src/datastore/utils', () => {
	const originalModule = jest.requireActual('../src/datastore/utils');

	return {
		__esModule: true,
		...originalModule,
		isNode: () => false,
	};
});

describe('realtime subscription ownership', () => {
	let {
		DataStore,
		errorHandler,
		Post,
		graphqlService,
		simulateConnect,
		simulateDisconnect,
		simulateDisruption,
		simulateDisruptionEnd,
	} = getDataStore({ online: true, isNode: false });

	const subscriptionRequests = () =>
		graphqlService.requests.filter(r => r.operation === 'subscription');
	const syncRequests = () =>
		graphqlService.requests.filter(
			r => r.operation === 'query' && r.type === 'sync',
		);
	const openObservers = () =>
		[...graphqlService.observers.values()]
			.flat()
			.filter(observer => !observer.closed);

	beforeEach(async () => {
		(console as any)._warn = console.warn;
		console.warn = () => {};
		({
			DataStore,
			errorHandler,
			Post,
			graphqlService,
			simulateConnect,
			simulateDisconnect,
			simulateDisruption,
			simulateDisruptionEnd,
		} = getDataStore({ online: true, isNode: false }));
		warpTime();
		await DataStore.start();
		await waitForSyncQueriesReady();
	});

	afterEach(async () => {
		await DataStore.clear();
		unwarpTime();
		console.warn = (console as any)._warn;
	});

	test('one round per DataStore start: ten offline/online cycles start no new subscription', async () => {
		const round = subscriptionRequests().length;
		expect(round).toBeGreaterThan(0);
		const observersBefore = openObservers().length;

		for (let i = 0; i < 10; i += 1) {
			await simulateDisconnect();
			await pause(1);
			await simulateConnect();
			await pause(1);
		}

		expect(subscriptionRequests()).toHaveLength(round);
		// Nothing was unsubscribed either: the same observers are still open.
		expect(openObservers()).toHaveLength(observersBefore);
	});

	test('the sync queries still run on every return to online', async () => {
		const syncsBefore = syncRequests().length;

		await simulateDisconnect();
		await pause(1);
		await simulateConnect();
		await waitForSyncQueriesReady();

		expect(syncRequests().length).toBeGreaterThan(syncsBefore);
		expect(subscriptionRequests()).toHaveLength(subscriptionRequests().length);
	});

	test('a save made while offline is sent after the return to online', async () => {
		await simulateDisconnect();
		const outboxEmpty = waitForEmptyOutbox();
		const post = await DataStore.save(new Post({ title: 'offline save' }));
		await pause(1);
		await simulateConnect();
		await outboxEmpty;

		const table = graphqlService.tables.get('Post')!;
		expect(table.get(JSON.stringify([post.id]))).toBeDefined();
	});

	test('a realtime disruption starts no new subscription and still triggers the catch-up sync', async () => {
		const round = subscriptionRequests().length;
		const syncsBefore = syncRequests().length;

		await simulateDisruption();
		await pause(1);
		await simulateDisruptionEnd();
		await waitForSyncQueriesReady();

		expect(subscriptionRequests()).toHaveLength(round);
		expect(syncRequests().length).toBeGreaterThan(syncsBefore);
	});

	test('a new DataStore start after a clear starts exactly one more round', async () => {
		const round = subscriptionRequests().length;

		await DataStore.clear();
		await DataStore.start();
		await waitForSyncQueriesReady();

		expect(subscriptionRequests()).toHaveLength(round * 2);
	});

	test('clear() waits for the realtime provider to finish the teardown of the round', async () => {
		let release!: () => void;
		const settle = jest.fn(
			() =>
				new Promise<void>(resolve => {
					release = resolve;
				}),
		);
		(graphqlService as any).settleRealtimeTeardowns = settle;

		let cleared = false;
		const clearing = DataStore.clear().then(() => {
			cleared = true;
		});
		await pause(5);
		expect(settle).toHaveBeenCalledTimes(1);
		expect(cleared).toBe(false);

		release();
		await clearing;
		expect(cleared).toBe(true);
		delete (graphqlService as any).settleRealtimeTeardowns;
	});

	test('a start() issued while a clear() runs is refused with the DataStoreStateError, never a second round', async () => {
		const round = subscriptionRequests().length;

		// The documented contract (sanityCheck.test.ts): a start during a clear
		// or a stop rejects, and the caller retries once the clear is done. The
		// web app awaits the clear before it starts, so it never sees this.
		const clearing = DataStore.clear();
		await expect(DataStore.start()).rejects.toThrow(
			/DataStoreStateError:.+`DataStore\.start\(\)`.+Clearing/,
		);
		await clearing;
		expect(subscriptionRequests()).toHaveLength(round);

		await DataStore.start();
		await waitForSyncQueriesReady();
		expect(subscriptionRequests()).toHaveLength(round * 2);
		const outboxEmpty = waitForEmptyOutbox();
		const post = await DataStore.save(new Post({ title: 'after the clear' }));
		await outboxEmpty;
		expect(
			graphqlService.tables.get('Post')!.get(JSON.stringify([post.id])),
		).toBeDefined();
	});

	test('after the round is connected, one subscription error drops that one operation and keeps the others', async () => {
		const observersBefore = openObservers().length;
		expect(observersBefore).toBeGreaterThan(1);
		const errors: any[] = [];
		errorHandler.subscribe(error => errors.push(error));

		// The service ends one running subscription with a reason no replay
		// can fix.
		const [victim] = graphqlService.getObservers('Post', 'create');
		victim.error({
			errors: [
				{
					message:
						'Connection failed: {"errors":[{"errorType":"BadRequestException","message":"Validation error"}]}',
				},
			],
		});
		await pause(5);

		expect(errors).toHaveLength(1);
		expect(errors[0].model).toBe('Post');
		expect(errors[0].process).toBe('subscribe');
		expect(openObservers()).toHaveLength(observersBefore - 1);

		// The engine is still running: a save still reaches the server.
		const outboxEmpty = waitForEmptyOutbox();
		const post = await DataStore.save(new Post({ title: 'still running' }));
		await outboxEmpty;
		expect(
			graphqlService.tables.get('Post')!.get(JSON.stringify([post.id])),
		).toBeDefined();
	});
});
