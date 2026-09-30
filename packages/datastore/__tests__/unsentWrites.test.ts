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
const never = () => new Promise(() => undefined);

/**
 * A write leaves the outbox only after a definitive answer from the server
 * for that record. Transport failures, time limits, an empty token, and an
 * auth rejection (HTTP 401) keep the write in the outbox.
 */
describe('DataStore keeps unsent writes', () => {
	let { DataStore, errorHandler, Post, graphqlService, connectivityMonitor } =
		getDataStore({ online: true, isNode: false });
	let simulateConnect: () => Promise<void>;
	let simulateDisconnect: () => Promise<void>;
	let errors: any[];

	const cloudPosts = () => graphqlService.tables.get('Post')!;

	/**
	 * Waits (in warped time) until the predicate is true.
	 * @returns elapsed fake milliseconds
	 */
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

	const outboxSize = async () => {
		const { syncClasses } = require('../src/datastore/datastore');
		const events = await (DataStore as any).storage.query(
			syncClasses.MutationEvent,
		);

		return events.length;
	};

	beforeEach(async () => {
		(console as any)._warn = console.warn;
		console.warn = () => {};

		({
			DataStore,
			errorHandler,
			Post,
			graphqlService,
			connectivityMonitor,
			simulateConnect,
			simulateDisconnect,
		} = getDataStore({ online: true, isNode: false }) as any);

		errors = [];
		errorHandler.subscribe(error => errors.push(error));

		await DataStore.start();
		warpTime();
	});

	afterEach(async () => {
		graphqlService.intercept = (request, next) => next();
		await DataStore.clear();
		unwarpTime();
		console.warn = (console as any)._warn;
	});

	test("a write that gets 'A network error has occurred.' stays in the outbox and is sent when the network returns", async () => {
		let networkUp = false;
		let failures = 0;
		graphqlService.intercept = (request, next) => {
			if (isMutation(request) && !networkUp) {
				failures++;
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

		const post = await DataStore.save(new Post({ title: 'network down' }));

		await waitFor(() => failures >= 3);
		expect(cloudPosts().size).toEqual(0);
		expect(await outboxSize()).toEqual(1);

		networkUp = true;
		await waitFor(() => cloudPosts().size === 1);

		expect(cloudPosts().has(JSON.stringify([post.id]))).toBe(true);
		expect(errors).toEqual([]);
	});

	test('a write with an empty token stays in the outbox and is sent when the token returns', async () => {
		let tokenAvailable = false;
		let failures = 0;
		graphqlService.intercept = (request, next) => {
			if (isMutation(request) && !tokenAvailable) {
				failures++;
				// thrown by api-graphql `headerBasedAuth`: not in GraphQLError format
				const noToken: any = new Error('No auth token specified');
				noToken.name = 'NoAuthorizationHeader';
				throw noToken;
			}

			return next();
		};

		const post = await DataStore.save(new Post({ title: 'no token' }));

		await waitFor(() => failures >= 3);
		expect(cloudPosts().size).toEqual(0);
		expect(await outboxSize()).toEqual(1);

		tokenAvailable = true;
		await waitFor(() => cloudPosts().size === 1);

		expect(cloudPosts().has(JSON.stringify([post.id]))).toBe(true);
		expect(errors).toEqual([]);
	});

	test('a write that gets HTTP 401 stays in the outbox and the error handler is called one time', async () => {
		let tokenAccepted = false;
		let failures = 0;
		graphqlService.intercept = (request, next) => {
			if (isMutation(request) && !tokenAccepted) {
				failures++;
				throw {
					data: {},
					errors: [
						{
							message: 'Unauthorized',
							originalError: {
								name: 'UnauthorizedException',
								$metadata: { httpStatusCode: 401 },
							},
						},
					],
				};
			}

			return next();
		};

		const post = await DataStore.save(new Post({ title: 'rejected token' }));

		await waitFor(() => failures >= 4);
		expect(cloudPosts().size).toEqual(0);
		expect(await outboxSize()).toEqual(1);

		tokenAccepted = true;
		await waitFor(() => cloudPosts().size === 1);

		expect(cloudPosts().has(JSON.stringify([post.id]))).toBe(true);
		expect(errors).toHaveLength(1);
		expect(errors[0]).toEqual(
			expect.objectContaining({ errorType: 'Unauthorized', process: 'mutate' }),
		);
	});

	test.each([429, 500, 503])(
		'a write that gets HTTP %i stays in the outbox',
		async status => {
			let failures = 0;
			let serviceUp = false;
			graphqlService.intercept = (request, next) => {
				if (isMutation(request) && !serviceUp) {
					failures++;
					throw {
						data: {},
						errors: [
							{
								message: 'Unknown error',
								originalError: { $metadata: { httpStatusCode: status } },
							},
						],
					};
				}

				return next();
			};

			await DataStore.save(new Post({ title: `status ${status}` }));

			await waitFor(() => failures >= 3);
			expect(await outboxSize()).toEqual(1);

			serviceUp = true;
			await waitFor(() => cloudPosts().size === 1);
			expect(errors).toEqual([]);
		},
	);

	test('a definitive server answer (resolver Unauthorized) still removes the write', async () => {
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

		await DataStore.save(new Post({ title: 'not permitted' }));

		await waitFor(() => errors.length > 0);
		let size = await outboxSize();
		for (let i = 0; i < 20 && size > 0; i++) {
			await pause(100);
			size = await outboxSize();
		}
		expect(size).toEqual(0);
		expect(cloudPosts().size).toEqual(0);
	});

	test('a write request that does not settle is cancelled at 30 seconds and the next attempt succeeds', async () => {
		// the production cancel path: the GraphQL API singleton's registry
		const { InternalGraphQLAPI } = require('@aws-amplify/api-graphql/internals');
		const cancel = jest
			.spyOn(InternalGraphQLAPI, 'cancel')
			.mockImplementation(() => true);

		let attempts = 0;
		const hung: any[] = [];
		graphqlService.intercept = (request, next) => {
			if (isMutation(request)) {
				attempts++;
				if (attempts === 1) {
					const request = never();
					hung.push(request);

					return request;
				}
			}

			return next();
		};

		const post = await DataStore.save(new Post({ title: 'hung write' }));

		const elapsed = await waitFor(() => cloudPosts().size === 1);

		expect(attempts).toEqual(2);
		expect(elapsed).toBeGreaterThanOrEqual(29000);
		expect(elapsed).toBeLessThan(40000);
		expect(cancel).toHaveBeenCalledTimes(1);
		expect(cancel).toHaveBeenCalledWith(hung[0], 'RequestTimeoutError');
		expect(cloudPosts().has(JSON.stringify([post.id]))).toBe(true);
		expect(errors).toEqual([]);
		cancel.mockRestore();
	});

	test('sync requests that do not settle after offline -> online: a save reaches the server in 45 seconds or less', async () => {
		await simulateDisconnect();

		graphqlService.intercept = (request, next) =>
			isSyncQuery(request) ? never() : next();

		await simulateConnect();
		const post = await DataStore.save(new Post({ title: 'after wake-up' }));

		const elapsed = await waitFor(() => cloudPosts().size === 1, 60000);

		expect(elapsed).toBeLessThanOrEqual(45000);
		expect(cloudPosts().has(JSON.stringify([post.id]))).toBe(true);
	});

	test('two restarts in a row do not produce two concurrent sender loops', async () => {
		let inFlight = 0;
		let maxInFlight = 0;
		const sentIds: string[] = [];
		const slowMutation = async (request, next) => {
			inFlight++;
			maxInFlight = Math.max(maxInFlight, inFlight);
			sentIds.push(request.variables.input.id);
			try {
				// slow enough that a request is in flight across the restarts
				await pause(3000);

				return await next();
			} finally {
				inFlight--;
			}
		};
		graphqlService.intercept = (request, next) =>
			isMutation(request) ? slowMutation(request, next) : next();

		const posts = [await DataStore.save(new Post({ title: 'post 0' }))];
		await waitFor(() => inFlight === 1);

		// the fake service stays reachable: only the sync engine restarts
		connectivityMonitor.simulateDisconnect();
		connectivityMonitor.simulateConnect();
		await pause(1);
		connectivityMonitor.simulateDisconnect();
		connectivityMonitor.simulateConnect();

		for (let i = 1; i <= 3; i++) {
			posts.push(await DataStore.save(new Post({ title: `post ${i}` })));
		}

		await waitFor(() => cloudPosts().size === posts.length);
		await waitFor(() => inFlight === 0);

		expect(maxInFlight).toEqual(1);
		// each write is sent one time
		expect([...sentIds].sort()).toEqual(posts.map(p => p.id).sort());
		expect(await outboxSize()).toEqual(0);
	});
});
