import 'fake-indexeddb/auto';
import { DataStore as DataStoreType } from '../src/datastore/datastore';
import { PersistentModelConstructor } from '../src/types';
import { Model, getDataStore } from './helpers';

// Every tab of a site shares one IndexedDB database. The browser sends each
// open connection `versionchange` when another tab deletes or upgrades the
// database, and waits for all of them to close; a delete that meets a
// connection that stays open reports `blocked`. Upstream listened for
// neither, so one tab held a delete for ever and every later open, in every
// tab, queued behind it (ranchr web, 2026-10-16). The adapter now closes and
// forgets its connection on `versionchange` (Hub `storageReleased`) and
// reports a blocked delete (Hub `storageBlocked`).

const DB_NAME = 'amplify-datastore';

// getDataStore() resets the module registry, so the Hub the adapter
// dispatches on is the one required after it, not the one imported at the top
const hubEvents = (): { events: string[]; stop: () => void } => {
	const { Hub } = require('@aws-amplify/core');
	const events: string[] = [];
	const stop = Hub.listen('datastore', ({ payload }) => {
		events.push(payload.event);
	});

	return { events, stop };
};

const rawOpen = (): Promise<IDBDatabase> =>
	new Promise((resolve, reject) => {
		const request = indexedDB.open(DB_NAME);
		request.onsuccess = () => resolve(request.result);
		request.onerror = () => reject(request.error);
	});

const rawDelete = (): { done: Promise<void>; blocked: Promise<void> } => {
	let onBlocked!: () => void;
	const blocked = new Promise<void>(resolve => {
		onBlocked = resolve;
	});
	const done = new Promise<void>((resolve, reject) => {
		const request = indexedDB.deleteDatabase(DB_NAME);
		request.onsuccess = () => resolve();
		request.onerror = () => reject(request.error);
		request.onblocked = () => onBlocked();
	});

	return { done, blocked };
};

describe('IndexedDBAdapter between tabs', () => {
	let DataStore: typeof DataStoreType;
	let ModelCtor: PersistentModelConstructor<Model>;

	beforeEach(async () => {
		({ DataStore, Model: ModelCtor } = getDataStore({
			storageAdapterFactory: () =>
				require('../src/storage/adapter/IndexedDBAdapter').default,
		}));
		await DataStore.save(new ModelCtor({ field1: 'one', dateCreated: new Date().toISOString() }));
	});

	afterEach(async () => {
		await DataStore.clear();
	});

	test('another tab deletes the database: this connection closes at once, the app hears storageReleased, and the next start opens it again', async () => {
		const { events, stop } = hubEvents();

		// the other tab's DataStore.clear()
		const { done } = rawDelete();
		await done;

		expect(events).toContain('storageReleased');

		// an operation on the closed connection fails with the browser's own error
		await expect(DataStore.query(ModelCtor)).rejects.toMatchObject({ name: 'InvalidStateError' });

		// the next start opens the database again (empty: the other tab deleted it)
		await DataStore.stop();
		const rows = await DataStore.query(ModelCtor);
		expect(rows).toHaveLength(0);

		stop();
	});

	test('a connection that does not close holds the delete: the app hears storageBlocked, and the clear completes when it closes', async () => {
		const { events, stop } = hubEvents();
		// an old bundle or a frozen tab: open, no versionchange handler
		const blocker = await rawOpen();

		let cleared = false;
		const clear = DataStore.clear().then(() => {
			cleared = true;
		});

		await new Promise(resolve => setTimeout(resolve, 50));
		try {
			expect(events).toContain('storageBlocked');
			expect(cleared).toBe(false);
		} finally {
			blocker.close();
		}
		await clear;

		expect(cleared).toBe(true);
		stop();
	});

	test('a clear with no other connection reports nothing', async () => {
		const { events, stop } = hubEvents();

		await DataStore.clear();

		expect(events).not.toContain('storageBlocked');
		expect(events).not.toContain('storageReleased');
		stop();
	});
});
