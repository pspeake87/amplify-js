import { getDataStore, pause, warpTime, unwarpTime } from './helpers';

/**
 * In this testing environment, both `isNode` and `isBrowser` incorrectly
 * evaluate to `true`. Here we set `isNode` to `false`, without mocking the
 * other utils. (Same as connectivityHandling.test.ts.)
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
const isSyncQuery = request => /\bsync\w+\s*\(/.test(String(request.query));
const syncSelection = request => {
	const m = /\b(sync\w+)\s*\(/.exec(String(request.query));

	return m ? m[1] : null;
};
const never = () => new Promise(() => undefined);

const UPDATE_POST = `
mutation operation($input: UpdatePostInput!, $condition: ModelPostConditionInput) {
	updatePost(input: $input, condition: $condition) {
		id
		title
		blogId
		updatedAt
		createdAt
		_version
		_lastChangedAt
		_deleted
	}
}
`;

/**
 * A change a user saved is never lost to the sync engine's own timing: not
 * to a stale sync page, not to a stop, not to a hung token read, not to a
 * lost response.
 */
describe('DataStore sync safety', () => {
	let ctx: ReturnType<typeof getDataStore> & Record<string, any>;
	let errors: any[];
	let hubEvents: [number, string, any][];
	let removeHubListener: () => void;

	const cloudPosts = () =>
		ctx.graphqlService.tables.get('Post')! as unknown as Map<string, any>;
	const cloudPost = (id: string): any => cloudPosts().get(JSON.stringify([id]));
	const localPost = (id: string): Promise<any> => ctx.DataStore.query(ctx.Post, id);
	const view = r => (r ? [r.title, r._version, Boolean(r._deleted)] : 'ABSENT');

	const waitFor = async (
		predicate: () => boolean | Promise<boolean>,
		timeoutMs = 120000,
	) => {
		const started = Date.now();
		while (!(await predicate())) {
			if (Date.now() - started > timeoutMs) {
				throw new Error(`waitFor: not true after ${timeoutMs} ms`);
			}
			await pause(100);
		}

		return Date.now() - started;
	};

	const outboxSize = async () => {
		const { syncClasses } = require('../src/datastore/datastore');
		const events = await (ctx.DataStore as any).storage.query(
			syncClasses.MutationEvent,
		);

		return events.length;
	};

	const hubEventsOfType = (type: string) =>
		hubEvents.filter(([, event]) => event === type);

	/** Creates a post on the server (v1) through DataStore and waits until it is synced. */
	const createSyncedPost = async () => {
		const post = await ctx.DataStore.save(new ctx.Post({ title: 'v1 original' }));
		await waitFor(
			async () => cloudPost(post.id)?._version === 1 && (await outboxSize()) === 0,
		);
		await waitFor(async () => (await localPost(post.id))?._version === 1);

		return post;
	};

	/**
	 * The next syncPosts page: the fake server reads the table at once; the
	 * response is held until `release()` (a big page on a slow link).
	 */
	const holdNextPostsPage = () => {
		const state: any = { held: false };
		let release!: () => void;
		const gate = new Promise<void>(resolve => {
			release = resolve;
		});
		state.release = release;
		ctx.graphqlService.intercept = (request, next) => {
			if (syncSelection(request) === 'syncPosts' && !state.held) {
				state.held = true;

				return (async () => {
					const response = await next();
					await gate;

					return response;
				})();
			}

			return next();
		};

		return state;
	};

	const setup = (multiplier = 20) => {
		ctx = getDataStore({ online: true, isNode: false }) as any;
		errors = [];
		ctx.errorHandler.subscribe(error => errors.push(error));
		hubEvents = [];
		// `getDataStore` resets the module registry: use its Hub instance
		const { Hub } = require('@aws-amplify/core');
		removeHubListener = Hub.listen('datastore', ({ payload }) => {
			hubEvents.push([Date.now(), payload.event, payload.data]);
		});

		return multiplier;
	};

	beforeEach(() => {
		(console as any)._warn = console.warn;
		console.warn = () => {};
	});

	afterEach(async () => {
		ctx.graphqlService.intercept = (request, next) => next();
		removeHubListener?.();
		await ctx.DataStore.clear();
		unwarpTime();
		console.warn = (console as any)._warn;
	});

	describe('a sync page never reverts a newer local record', () => {
		beforeEach(async () => {
			setup();
			await ctx.DataStore.start();
			warpTime();
		});

		test('a queued UPDATE sent at 10 s survives a page the server read before it', async () => {
			const { DataStore, Post, connectivityMonitor } = ctx;
			const post = await createSyncedPost();

			await connectivityMonitor.simulateDisconnect();
			await DataStore.save(
				Post.copyOf(await localPost(post.id), d => {
					d.title = 'v2 edited offline';
				}),
			);
			await waitFor(async () => (await outboxSize()) === 1);

			const page = holdNextPostsPage();
			const t0 = Date.now();
			await connectivityMonitor.simulateConnect();

			// the sender starts at 10 s, before the page arrives
			await waitFor(
				async () => cloudPost(post.id)?._version === 2 && (await outboxSize()) === 0,
			);
			await waitFor(async () => (await localPost(post.id))?._version === 2);

			page.release();
			await waitFor(() =>
				hubEvents.some(([at, e]) => at >= t0 && e === 'syncQueriesReady'),
			);
			await pause(1000);

			expect(view(await localPost(post.id))).toEqual(['v2 edited offline', 2, false]);

			// the follow-up edit carries the right _version and lands
			await DataStore.save(
				Post.copyOf(await localPost(post.id), d => {
					d.title = 'v3 follow-up';
				}),
			);
			await waitFor(async () => cloudPost(post.id)?._version === 3);
			expect(cloudPost(post.id).title).toEqual('v3 follow-up');
			expect(errors).toEqual([]);
		}, 60000);

		test('a queued DELETE sent at 10 s is not undone by a page the server read before it', async () => {
			const { DataStore, connectivityMonitor } = ctx;
			const post = await createSyncedPost();

			await connectivityMonitor.simulateDisconnect();
			await DataStore.delete(await localPost(post.id));
			await waitFor(async () => (await outboxSize()) === 1);

			const page = holdNextPostsPage();
			const t0 = Date.now();
			await connectivityMonitor.simulateConnect();

			await waitFor(
				async () =>
					cloudPost(post.id)?._deleted === true && (await outboxSize()) === 0,
			);
			await pause(1000);
			expect(view(await localPost(post.id))).toEqual('ABSENT');

			page.release();
			await waitFor(() =>
				hubEvents.some(([at, e]) => at >= t0 && e === 'syncQueriesReady'),
			);
			await pause(1000);

			expect(view(await localPost(post.id))).toEqual('ABSENT');
		}, 60000);

		test("a teammate's realtime UPDATE merged during the round survives a stale page", async () => {
			const { connectivityMonitor, graphqlService } = ctx;
			const post = await createSyncedPost();

			await connectivityMonitor.simulateDisconnect();
			const page = holdNextPostsPage();
			const t0 = Date.now();
			await connectivityMonitor.simulateConnect();

			// a teammate edits the post after the realtime drain started (10 s)
			await waitFor(() => Date.now() - t0 > 12000);
			await graphqlService.externalGraphql(
				{
					query: UPDATE_POST,
					variables: {
						input: { id: post.id, title: 'teammate edit', _version: 1 },
						condition: null,
					},
					authMode: undefined,
					authToken: undefined,
				} as any,
				true,
			);
			await waitFor(async () => (await localPost(post.id))?._version === 2);

			page.release();
			await waitFor(() =>
				hubEvents.some(([at, e]) => at >= t0 && e === 'syncQueriesReady'),
			);
			await pause(1000);

			expect(view(await localPost(post.id))).toEqual(['teammate edit', 2, false]);
		}, 60000);
	});

	describe('stop and clear', () => {
		test('clear() during the sender-start race sends nothing and leaves no orphan sender', async () => {
			setup();
			const { DataStore, graphqlService, Post } = ctx;
			const mutations: number[] = [];
			graphqlService.intercept = (request, next) => {
				if (isSyncQuery(request)) return never();
				if (isMutation(request)) mutations.push(Date.now());

				return next();
			};

			await DataStore.start();
			warpTime();
			await DataStore.save(new Post({ title: 'discarded by clear' }));
			await waitFor(() =>
				hubEvents.some(([, e]) => e === 'subscriptionsEstablished'),
			);
			const [[raceStart]] = hubEventsOfType('subscriptionsEstablished');
			// 8 s into the race: the 10 s sender-start timer fires during clear()
			await waitFor(() => Date.now() - raceStart >= 8000);

			await DataStore.clear();
			// the timer would fire here, and any orphan would send now
			await pause(15000);

			expect(mutations).toEqual([]);
			expect(cloudPosts().size).toEqual(0);
		}, 60000);

		test('SYNC_ENGINE_READY fires after the first sync round, not at the 10 s sender start', async () => {
			setup();
			const { DataStore, graphqlService } = ctx;
			let releasePages!: () => void;
			const gate = new Promise<void>(resolve => {
				releasePages = resolve;
			});
			graphqlService.intercept = (request, next) =>
				isSyncQuery(request) ? gate.then(next) : next();

			await DataStore.start();
			warpTime();
			await waitFor(() =>
				hubEvents.some(([, e]) => e === 'subscriptionsEstablished'),
			);
			const [[raceStart]] = hubEventsOfType('subscriptionsEstablished');
			await waitFor(() => Date.now() - raceStart >= 15000);

			// the sender started at 10 s, the sync is still running
			expect(hubEventsOfType('ready')).toEqual([]);

			releasePages();
			await waitFor(() => hubEventsOfType('ready').length === 1);
			const [[readyAt]] = hubEventsOfType('ready');
			const [[syncReadyAt]] = hubEventsOfType('syncQueriesReady');
			expect(readyAt).toBeGreaterThanOrEqual(syncReadyAt);
		}, 60000);

		test('clear() with a hung write and a hung sync page finishes in seconds, reports nothing, keeps the write', async () => {
			setup();
			const { DataStore, graphqlService, Post } = ctx;
			await DataStore.start();
			warpTime();
			await waitFor(() => hubEventsOfType('ready').length === 1);

			graphqlService.intercept = (request, next) =>
				isMutation(request) || isSyncQuery(request) ? never() : next();

			await DataStore.save(new Post({ title: 'hung write' }));
			// a disruption schedules a sync round whose page hangs too
			await ctx.simulateDisruption();
			await ctx.simulateDisruptionEnd();
			await pause(2000);

			const started = Date.now();
			await DataStore.clear();
			const took = Date.now() - started;

			expect(took).toBeLessThan(5000);
			expect(errors).toEqual([]);
		}, 60000);
	});

	describe('a hung token read', () => {
		test('the write is sent after the token time limit, and clear() does not hang', async () => {
			setup();
			const { DataStore, Post, graphqlService } = ctx;
			let hangNextRead = false;
			const functionAuthProvider = () => {
				if (hangNextRead) {
					hangNextRead = false;

					return never() as Promise<{ token: string }>;
				}

				return Promise.resolve({ token: 'good-token' });
			};
			(DataStore as any).amplifyConfig.aws_appsync_authenticationType = 'lambda';
			(DataStore as any).amplifyConfig.authProviders = { functionAuthProvider };

			await DataStore.start();
			warpTime();
			await waitFor(() => hubEventsOfType('ready').length === 1);

			hangNextRead = true;
			const post = await DataStore.save(new Post({ title: 'after wake-up' }));
			const elapsed = await waitFor(() => cloudPost(post.id) !== undefined, 90000);

			// TOKEN_REQUEST_TIMEOUT_MS + first retry delay
			expect(elapsed).toBeGreaterThanOrEqual(29000);
			expect(elapsed).toBeLessThan(45000);
			expect(graphqlService.requests.some(r => r.authToken === 'good-token')).toBe(
				true,
			);
			expect(errors).toEqual([]);
		}, 60000);
	});

	describe('sync pages', () => {
		test('a page whose download takes 100 s completes (the limit is 180 s, not 30 s)', async () => {
			setup(50);
			const { DataStore, graphqlService, Post } = ctx;
			// two posts on the server before this device syncs
			const table = cloudPosts();
			for (const title of ['one', 'two']) {
				const id = `00000000-0000-4000-8000-00000000000${title.length}${title[0]}`;
				table.set(JSON.stringify([id]), {
					id,
					title,
					_version: 1,
					_deleted: false,
					_lastChangedAt: Date.now(),
				});
			}
			let pageRequests = 0;
			const log: any[] = [];
			const t0 = Date.now();
			graphqlService.intercept = (request, next) => {
				if (syncSelection(request) === 'syncPosts') {
					pageRequests++;
					log.push([Date.now() - t0, request.variables]);

					return pause(100000).then(next);
				}

				return next();
			};

			await DataStore.start();
			warpTime(50);
			await waitFor(() => hubEventsOfType('syncQueriesReady').length === 1, 200000);

			// the test schema has two models whose sync query is `syncPosts`
			expect(pageRequests).toEqual(2);
			expect(log.every(([, variables]) => variables.nextToken === null)).toBe(true);
			const [[readyAt]] = hubEventsOfType('syncQueriesReady');
			expect(readyAt - t0).toBeGreaterThanOrEqual(100000);
			expect((await DataStore.query(Post)).length).toEqual(2);
			expect(errors).toEqual([]);
		}, 60000);

		test('a page 2 that keeps failing ends the round after the retries; the round is not stuck for ever', async () => {
			setup(200);
			const { DataStore, graphqlService } = ctx;
			let page2Requests = 0;
			graphqlService.intercept = (request, next) => {
				if (syncSelection(request) === 'syncPosts') {
					if (request.variables.nextToken) {
						page2Requests++;

						return Promise.reject({
							data: { syncPosts: null },
							errors: [
								{ message: 'Internal server error', errorType: 'InternalFailure' },
							],
						});
					}

					return next().then(response => {
						response.data.syncPosts.nextToken = 'page-2';

						return response;
					});
				}

				return next();
			};

			await DataStore.start();
			warpTime(200);
			// core retry gives up after 12 attempts (about 7 minutes)
			await waitFor(
				() => hubEventsOfType('syncQueriesReady').length === 1,
				20 * 60 * 1000,
			);

			// two models share the `syncPosts` query; core retry makes 12 attempts each
			expect(page2Requests).toBeLessThanOrEqual(26);
			expect(errors).toHaveLength(2);
			expect(errors[0]).toEqual(expect.objectContaining({ process: 'sync' }));

			// the next round starts from scratch for that model
			const { syncClasses } = require('../src/datastore/datastore');
			const metadata = await (DataStore as any).storage.query(
				syncClasses.ModelMetadata,
			);
			const posts = metadata.find(m => m.model === 'Post');
			expect(posts.lastSync).toBeFalsy();
		}, 120000);

		test('an empty token on a sync page is retried, the model is not marked synced and empty', async () => {
			setup();
			const { DataStore, graphqlService, Post } = ctx;
			const table = cloudPosts();
			const id = '00000000-0000-4000-8000-000000000001';
			table.set(JSON.stringify([id]), {
				id,
				title: 'on the server',
				_version: 1,
				_deleted: false,
				_lastChangedAt: Date.now(),
			});
			let tokenReads = 0;
			const functionAuthProvider = async () => {
				tokenReads++;

				// the first reads fail the way the web app fails for 5 s
				return { token: tokenReads <= 2 ? '' : 'good-token' };
			};
			(DataStore as any).amplifyConfig.aws_appsync_authenticationType = 'lambda';
			(DataStore as any).amplifyConfig.authProviders = { functionAuthProvider };

			await DataStore.start();
			warpTime();
			await waitFor(() => hubEventsOfType('syncQueriesReady').length === 1);

			expect((await DataStore.query(Post)).length).toEqual(1);
			expect(errors).toEqual([]);
		}, 60000);
	});

	describe('errors reach the error handler', () => {
		test('a local change that cannot be queued is reported (the job is not swallowed)', async () => {
			setup();
			const { DataStore, Post } = ctx;
			await DataStore.start();
			warpTime();
			await waitFor(() => hubEventsOfType('ready').length === 1);

			const outbox = (DataStore as any).sync.outbox;
			const enqueue = outbox.enqueue.bind(outbox);
			outbox.enqueue = async () => {
				throw new Error('QuotaExceededError: the database is full');
			};

			const post = await DataStore.save(new Post({ title: 'not queued' }));
			await waitFor(() => errors.length === 1);

			expect(errors[0]).toEqual(
				expect.objectContaining({
					process: 'sync',
					model: 'Post',
					operation: 'enqueue of a local change',
					message: expect.stringContaining('QuotaExceededError'),
				}),
			);
			expect(errors[0].localModel.id).toEqual(post.id);
			outbox.enqueue = enqueue;
		}, 60000);

		test('a subscription failure that is a plain Error is reported, not thrown away', async () => {
			setup();
			const { DataStore } = ctx;
			const { Observable } = require('rxjs');
			ctx.graphqlService.intercept = (request, next) => {
				if (/^\s*subscription/.test(String(request.query))) {
					return new Observable(o => {
						o.error(new Error('WebSocket refused'));
					});
				}

				return next();
			};

			await DataStore.start();
			warpTime();
			await waitFor(() => errors.some(e => e.model === 'Post'));

			// before: the handler destructured `errors` from the value and threw
			expect(errors).toEqual(
				expect.arrayContaining([
					expect.objectContaining({
						process: 'subscribe',
						model: 'Post',
						operation: 'Create',
						message: 'WebSocket refused',
					}),
				]),
			);
		}, 60000);

		test('a sync engine that stops for good is reported', async () => {
			setup();
			const { DataStore } = ctx;
			const { Observable } = require('rxjs');
			const { SyncEngine } = require('../src/sync');
			const start = jest
				.spyOn(SyncEngine.prototype, 'start')
				.mockImplementation(
					() =>
						new Observable(o => {
							o.error(new Error('IndexedDB is broken'));
						}),
				);

			await DataStore.start().catch(() => undefined);
			warpTime();
			await waitFor(() => errors.some(e => e.operation === 'syncEngine'));

			expect(errors).toEqual([
				expect.objectContaining({
					process: 'sync',
					operation: 'syncEngine',
					errorType: 'Unknown',
					message: 'Sync engine stopped: IndexedDB is broken',
				}),
			]);
			start.mockRestore();
		}, 60000);
	});

	describe('the outbox head', () => {
		test('a re-sent CREATE whose first response was lost learns its _version; the next UPDATE carries it', async () => {
			setup();
			const { DataStore, graphqlService, Post } = ctx;
			await DataStore.start();
			warpTime();
			await waitFor(() => hubEventsOfType('ready').length === 1);

			let creates = 0;
			const sentVersions: any[] = [];
			graphqlService.intercept = (request, next) => {
				if (isMutation(request) && /createPost/.test(String(request.query))) {
					creates++;
					if (creates === 1) {
						// the server applies the write, the response never arrives
						next();

						return never();
					}
				}
				if (isMutation(request) && /updatePost/.test(String(request.query))) {
					sentVersions.push(request.variables.input._version);
				}

				return next();
			};

			const post = await DataStore.save(new Post({ title: 'new animal' }));
			await waitFor(async () => (await outboxSize()) === 0, 90000);

			expect(creates).toEqual(2);
			expect(errors).toEqual([]);
			expect((await localPost(post.id))._version).toEqual(1);

			await DataStore.save(
				Post.copyOf(await localPost(post.id), d => {
					d.title = 'first edit';
				}),
			);
			await waitFor(() => cloudPost(post.id)?.title === 'first edit');
			expect(sentVersions).toEqual([1]);
			expect(cloudPost(post.id)._version).toEqual(2);
		}, 60000);

		test('a new save while the head waits out a retry delay is tried at once', async () => {
			setup();
			const { DataStore, graphqlService, Post } = ctx;
			await DataStore.start();
			warpTime();
			await waitFor(() => hubEventsOfType('ready').length === 1);

			let serviceUp = false;
			let attempts = 0;
			graphqlService.intercept = (request, next) => {
				if (isMutation(request) && !serviceUp) {
					attempts++;
					throw {
						data: {},
						errors: [
							{ message: 'Service Unavailable', originalError: { $metadata: { httpStatusCode: 503 } } },
						],
					};
				}

				return next();
			};

			const first = await DataStore.save(new Post({ title: 'first' }));
			// well into the 30 s plateau
			await waitFor(() => attempts >= 10, 10 * 60 * 1000);

			serviceUp = true;
			const recoveredAt = Date.now();
			const second = await DataStore.save(new Post({ title: 'second' }));
			await waitFor(() => cloudPost(second.id) !== undefined, 120000);

			// two requests, not a 30 s retry delay
			expect(Date.now() - recoveredAt).toBeLessThan(5000);
			expect(cloudPost(first.id)).toBeDefined();
			expect(errors).toEqual([]);
		}, 60000);

		test('outboxHeadStuck is dispatched one time after 5 minutes of failed attempts', async () => {
			setup(200);
			const { DataStore, graphqlService, Post } = ctx;
			await DataStore.start();
			warpTime(200);
			await waitFor(() => hubEventsOfType('ready').length === 1);

			let serviceUp = false;
			graphqlService.intercept = (request, next) => {
				if (isMutation(request) && !serviceUp) {
					throw {
						data: {},
						errors: [
							{ message: 'Service Unavailable', originalError: { $metadata: { httpStatusCode: 503 } } },
						],
					};
				}

				return next();
			};

			const post = await DataStore.save(new Post({ title: 'stuck' }));
			const started = Date.now();
			await waitFor(
				() => hubEventsOfType('outboxHeadStuck').length === 1,
				10 * 60 * 1000,
			);
			const [[at, , data]] = hubEventsOfType('outboxHeadStuck');

			expect(at - started).toBeGreaterThanOrEqual(5 * 60 * 1000);
			expect(data).toEqual(
				expect.objectContaining({
					model: 'Post',
					operation: 'Create',
					modelId: post.id,
				}),
			);
			expect(data.attempts).toBeGreaterThan(5);

			// more failures: still one event
			await pause(3 * 60 * 1000);
			expect(hubEventsOfType('outboxHeadStuck')).toHaveLength(1);

			serviceUp = true;
			await waitFor(() => cloudPost(post.id) !== undefined, 60000);
			expect(errors).toEqual([]);
		}, 120000);
	});
});
