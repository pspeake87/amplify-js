import AsyncStorageDatabase from '../src/storage/adapter/AsyncStorageDatabase';

// Ranchr fork: every save of an existing record must reuse its storage key.
// Upstream looked the key up by the index NAME instead of the key VALUE, so a
// second save wrote a new key and kept the old one. The orphan copy of a merged
// outbox entry was sent again at the next launch (kick-the-tires walk 1, MA6).

const STORE = 'user_Cattle';
const keysOf = (db: AsyncStorageDatabase) =>
	Array.from(((db as any).storage.db as Map<string, string>).keys()).filter(k =>
		k.includes(`::${STORE}::Data::`),
	);
const valueOf = (db: AsyncStorageDatabase, key: string) =>
	JSON.parse(((db as any).storage.db as Map<string, string>).get(key)!);

describe('AsyncStorageDatabase keys', () => {
	it('two saves of one record leave one storage key that holds the last state', async () => {
		const db = new AsyncStorageDatabase();
		await db.init();

		await db.save({ id: 'cow-1', status: 'Active' }, STORE, ['id'], 'cow-1');
		await db.save({ id: 'cow-1', status: 'Deleted' }, STORE, ['id'], 'cow-1');

		const keys = keysOf(db);
		expect(keys).toHaveLength(1);
		expect(valueOf(db, keys[0]).status).toBe('Deleted');
		expect(await db.getAll(STORE)).toEqual([{ id: 'cow-1', status: 'Deleted' }]);
	});

	it('init() keeps the newest copy of a record that an older build stored more than once', async () => {
		const db = new AsyncStorageDatabase();
		const storage = (db as any).storage.db as Map<string, string>;
		const prefix = `@AmplifyDatastore::${STORE}::Data`;
		// ULIDs sort by creation time: the larger one is the newer save
		storage.set(`${prefix}::01J0000000000000000000000A::cow-1`, JSON.stringify({ id: 'cow-1', status: 'Active' }));
		storage.set(`${prefix}::01J0000000000000000000000C::cow-1`, JSON.stringify({ id: 'cow-1', status: 'Deleted' }));
		storage.set(`${prefix}::01J0000000000000000000000B::cow-1`, JSON.stringify({ id: 'cow-1', status: 'Sold' }));
		storage.set(`${prefix}::01J0000000000000000000000D::cow-2`, JSON.stringify({ id: 'cow-2', status: 'Active' }));

		await db.init();

		expect(keysOf(db).sort()).toEqual([
			`${prefix}::01J0000000000000000000000C::cow-1`,
			`${prefix}::01J0000000000000000000000D::cow-2`,
		]);
		expect(await db.getAll(STORE)).toEqual(
			expect.arrayContaining([
				{ id: 'cow-1', status: 'Deleted' },
				{ id: 'cow-2', status: 'Active' },
			]),
		);
		await db.delete('cow-1', STORE);
		expect(keysOf(db)).toEqual([`${prefix}::01J0000000000000000000000D::cow-2`]);
	});
});

describe('AsyncStorageDatabase damaged records (Ranchr fork, kick-the-tires walk 8, MM5)', () => {
	it('getAll skips a record whose stored value is not JSON and returns the others', async () => {
		const db = new AsyncStorageDatabase();
		const storage = (db as any).storage.db as Map<string, string>;
		const prefix = `@AmplifyDatastore::${STORE}::Data`;
		storage.set(`${prefix}::01J0000000000000000000000A::cow-1`, JSON.stringify({ id: 'cow-1', status: 'Active' }));
		storage.set(`${prefix}::01J0000000000000000000000B::cow-2`, '{"id":"cow-2","status":"Act');
		storage.set(`${prefix}::01J0000000000000000000000C::cow-3`, JSON.stringify({ id: 'cow-3', status: 'Sold' }));
		await db.init();

		const all = await db.getAll<any>(STORE);

		expect(all.map(r => r.id).sort()).toEqual(['cow-1', 'cow-3']);
		expect(await db.get<any>('cow-2', STORE)).toBeUndefined();
		expect(await db.get<any>('cow-3', STORE)).toEqual({ id: 'cow-3', status: 'Sold' });
	});
});
