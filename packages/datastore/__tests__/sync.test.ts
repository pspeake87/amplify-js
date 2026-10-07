// These tests should be replaced once SyncEngine.partialDataFeatureFlagEnabled is removed.
import {
	AmplifyError,
	Category,
	DataStoreAction,
} from '@aws-amplify/core/internals/utils';
import { defaultAuthStrategy } from '../src/authModeStrategies';
let mockGraphQl;

const sessionStorageMock = (() => {
	let store = {};

	return {
		getItem(key) {
			return store[key] || null;
		},
		setItem(key, value) {
			store[key] = value.toString();
		},
		removeItem(key) {
			delete store[key];
		},
		clear() {
			store = {};
		},
	};
})();

Object.defineProperty(window, 'sessionStorage', {
	value: sessionStorageMock,
});
const defaultQuery = `query {
			syncPosts {
				items {
					id
					title
					count
					_version
					_lastChangedAt
					_deleted
				}
				nextToken
				startedAt
			}
		}`;
const defaultVariables = {};
const defaultOpName = 'syncPosts';
const defaultModelDefinition = { name: 'Post' };
const defaultAuthMode = 'userPool';

describe('Sync', () => {
	describe('jitteredRetry', () => {
		beforeEach(() => {
			window.sessionStorage.clear();
			jest.resetModules();
			jest.resetAllMocks();
		});

		it('should return all data', async () => {
			const resolveResponse = {
				data: {
					syncPosts: {
						items: [
							{
								id: '1',
								title: 'Item 1',
							},
							{
								id: '2',
								title: 'Item 2',
							},
						],
					},
				},
			};

			const SyncProcessor = jitteredRetrySyncProcessorSetup({
				resolveResponse,
			});

			const data = await SyncProcessor.jitteredRetry({
				query: defaultQuery,
				variables: defaultVariables,
				opName: defaultOpName,
				modelDefinition: defaultModelDefinition,
			});

			expect(data).toMatchSnapshot();
		});

		it('custom pk: should return all data', async () => {
			const resolveResponse = {
				data: {
					syncPosts: {
						items: [
							{
								postId: '1',
								title: 'Item 1',
							},
							{
								postId: '2',
								title: 'Item 2',
							},
						],
					},
				},
			};

			const SyncProcessor = jitteredRetrySyncProcessorSetup({
				resolveResponse,
			});

			const data = await SyncProcessor.jitteredRetry({
				query: defaultQuery,
				variables: defaultVariables,
				opName: defaultOpName,
				modelDefinition: defaultModelDefinition,
			});

			expect(data).toMatchSnapshot();
		});

		it('should return partial data and send Hub event', async () => {
			const rejectResponse = {
				data: {
					syncPosts: {
						items: [
							{
								id: '1',
								title: 'Item 1',
							},
							null,
							{
								id: '3',
								title: 'Item 3',
							},
						],
					},
				},
				errors: [
					{
						message: 'Item 2 error',
					},
				],
			};

			const hubDispatchMock = jest.fn();
			const coreMocks = {
				Hub: {
					dispatch: hubDispatchMock,
					listen: jest.fn(),
				},
			};

			const SyncProcessor = jitteredRetrySyncProcessorSetup({
				rejectResponse,
				coreMocks,
			});

			const data = await SyncProcessor.jitteredRetry({
				query: defaultQuery,
				variables: defaultVariables,
				opName: defaultOpName,
				modelDefinition: defaultModelDefinition,
				authMode: defaultAuthMode,
			});

			expect(data).toMatchSnapshot();

			expect(hubDispatchMock).toHaveBeenCalledWith('datastore', {
				event: 'nonApplicableDataReceived',
				data: {
					errors: [
						{
							message: 'Item 2 error',
						},
					],
					modelName: 'Post',
				},
			});
		});

		// TODO(v6) Re-enable test
		it('should throw error if no data is returned', async () => {
			const rejectResponse = {
				data: null,
				errors: [
					{
						message: 'General error',
					},
				],
			};

			const SyncProcessor = jitteredRetrySyncProcessorSetup({
				rejectResponse,
			});

			try {
				await SyncProcessor.jitteredRetry({
					query: defaultQuery,
					variables: defaultVariables,
					opName: defaultOpName,
					modelDefinition: defaultModelDefinition,
					authMode: defaultAuthMode,
				});
			} catch (e) {
				expect(e).toMatchSnapshot();
			}
		});

		[401, 403].forEach(status => {
			it(`should retry (TransientRequestError) for HTTP ${status}: the token is expected to work again`, async () => {
				const rejectResponse = {
					data: null,
					errors: [
						{
							originalError: {
								$metadata: {
									httpStatusCode: status,
								},
							},
						},
					],
				};

				const SyncProcessor = jitteredRetrySyncProcessorSetup({
					rejectResponse,
				});

				await expect(
					SyncProcessor.jitteredRetry({
						query: defaultQuery,
						variables: defaultVariables,
						opName: defaultOpName,
						modelDefinition: defaultModelDefinition,
						authMode: defaultAuthMode,
					}),
				).rejects.toMatchObject({ name: 'TransientRequestError' });
			});
		});

		[
			'No api-key configured',
			'No current user',
			'No credentials',
			'No federated jwt',
		].forEach(authError => {
			it(`should return NonRetryableError for client-side error: ${authError}`, async () => {
				const rejectResponse = {
					message: authError,
				};

				const SyncProcessor = jitteredRetrySyncProcessorSetup({
					rejectResponse,
				});

				try {
					await SyncProcessor.jitteredRetry({
						query: defaultQuery,
						variables: defaultVariables,
						opName: defaultOpName,
						modelDefinition: defaultModelDefinition,
						authMode: defaultAuthMode,
					});
				} catch (e) {
					// NonRetryableError has a `nonRetryable` property
					expect(e).toHaveProperty('nonRetryable');
				}
			});
		});

		it('should send datastore user agent details with graphql request', async () => {
			const resolveResponse = {
				data: {
					syncPosts: {
						items: [
							{
								id: '1',
								title: 'Item 1',
							},
							{
								id: '2',
								title: 'Item 2',
							},
						],
					},
				},
			};

			const SyncProcessor = jitteredRetrySyncProcessorSetup({
				resolveResponse,
			});

			await SyncProcessor.jitteredRetry({
				query: defaultQuery,
				variables: defaultVariables,
				opName: defaultOpName,
				modelDefinition: defaultModelDefinition,
			});

			expect(mockGraphQl).toHaveBeenCalledWith(expect.anything(), undefined, {
				category: Category.DataStore,
				action: DataStoreAction.GraphQl,
			});
		});
	});

	describe('error handler', () => {
		const errorHandler = jest.fn();
		const data = {
			syncPosts: {
				items: [
					{
						id: '1',
						title: 'Item 1',
					},
					null,
					{
						id: '3',
						title: 'Item 3',
					},
				],
			},
		};

		beforeEach(async () => {
			window.sessionStorage.clear();
			jest.resetModules();
			jest.resetAllMocks();
			errorHandler.mockClear();
		});

		test('bad record', async () => {
			const syncProcessor = jitteredRetrySyncProcessorSetup({
				errorHandler,
				rejectResponse: {
					data,
					errors: [
						{
							message: 'Cannot return boolean for string type',
						},
					],
				},
			});

			await syncProcessor.jitteredRetry({
				query: defaultQuery,
				variables: defaultVariables,
				opName: defaultOpName,
				modelDefinition: defaultModelDefinition,
			});

			expect(errorHandler).toHaveBeenCalledWith(
				expect.objectContaining({
					operation: 'syncPosts',
					process: 'sync',
					errorType: 'BadRecord',
				}),
			);
		});

		test('connection timeout', async () => {
			const syncProcessor = jitteredRetrySyncProcessorSetup({
				errorHandler,
				rejectResponse: {
					data,
					errors: [
						{
							message: 'Connection failed: Connection Timeout',
						},
					],
				},
			});

			await syncProcessor.jitteredRetry({
				query: defaultQuery,
				variables: defaultVariables,
				opName: defaultOpName,
				modelDefinition: defaultModelDefinition,
			});

			expect(errorHandler).toHaveBeenCalledWith(
				expect.objectContaining({
					operation: 'syncPosts',
					process: 'sync',
					errorType: 'Transient',
				}),
			);
		});

		test('server error', async () => {
			const syncProcessor = jitteredRetrySyncProcessorSetup({
				errorHandler,
				rejectResponse: {
					data,
					errors: [
						{
							originalError: {
								$metadata: {
									httpStatusCode: 500,
								},
							},
						},
					],
				},
			});

			await syncProcessor.jitteredRetry({
				query: defaultQuery,
				variables: defaultVariables,
				opName: defaultOpName,
				modelDefinition: defaultModelDefinition,
			});

			expect(errorHandler).toHaveBeenCalledWith(
				expect.objectContaining({
					operation: 'syncPosts',
					process: 'sync',
					errorType: 'Transient',
				}),
			);
		});
	});

	/**
	 * A connection-level realtime failure is one failure, not one per model and
	 * operation. The provider decides when an episode is worth reporting; the
	 * sync engine turns that one decision into one error handler call.
	 */
	describe('realtime failure reports', () => {
		let Hub: any;
		let AMPLIFY_SYMBOL: symbol;
		let SyncEngine: any;
		let apiGraphql: any;
		let errorHandler: jest.Mock;
		let scheduleSync: jest.Mock;
		let engine: any;
		let stopListener: () => void;

		// The listener accepts reports from the AppSync realtime provider only,
		// so every payload carries its name unless a test says otherwise.
		const dispatch = (event: string, data: any, source = 'PubSub') => {
			const payload =
				data && typeof data === 'object' && !('providerName' in data)
					? { providerName: 'AWSAppSyncRealTimeProvider', ...data }
					: data;
			Hub.dispatch('api', { event, data: payload }, source, AMPLIFY_SYMBOL);
		};

		/** Drains the microtask queue the async report runs on. */
		const flush = async () => {
			for (let i = 0; i < 10; i++) {
				await Promise.resolve();
			}
		};

		const authFailure = {
			report: 'open',
			reason: 'no-auth-token',
			isAuthFailure: true,
			message:
				'Connection failed: UnauthorizedException: no auth token was available',
			diagnostics: {
				episodeId: 1,
				consecutiveFailures: 2,
				socketsAttempted: 0,
				socketStatus: 'CLOSED',
				subscriptions: 45,
			},
		};

		beforeEach(() => {
			jest.resetModules();

			Hub = require('@aws-amplify/core').Hub;
			AMPLIFY_SYMBOL =
				require('@aws-amplify/core/internals/utils').AMPLIFY_SYMBOL;
			apiGraphql = require('@aws-amplify/api-graphql');
			SyncEngine = require('../src/sync').SyncEngine;

			errorHandler = jest.fn();
			scheduleSync = jest.fn();

			// The listener and the report are instance methods of SyncEngine. A
			// whole engine is not needed to prove what the listener does with one
			// payload, so they run against the three fields they use.
			engine = {
				errorHandler,
				scheduleSync,
				connectionDisrupted: false,
				reportRealtimeFailure: SyncEngine.prototype.reportRealtimeFailure,
			};
			stopListener = SyncEngine.prototype.startDisruptionListener.call(engine);
		});

		afterEach(() => {
			stopListener?.();
		});

		test('a report from another provider is ignored', async () => {
			dispatch(apiGraphql.REALTIME_CONNECTION_FAILURE, {
				...authFailure,
				providerName: 'AWSAppSyncEventsProvider',
			});
			await flush();

			expect(errorHandler).not.toHaveBeenCalled();
		});

		test('report open calls errorHandler once, errorType Unauthorized for an auth reason', async () => {
			dispatch(apiGraphql.REALTIME_CONNECTION_FAILURE, authFailure);
			await flush();

			expect(errorHandler).toHaveBeenCalledTimes(1);
			expect(errorHandler).toHaveBeenCalledWith(
				expect.objectContaining({
					errorType: 'Unauthorized',
					operation: 'Connect',
					process: 'subscribe',
					model: undefined,
					message: authFailure.message,
				}),
			);

			const call = errorHandler.mock.calls[0][0];
			expect(JSON.parse(call.errorInfo)).toMatchObject({
				reason: 'no-auth-token',
				report: 'open',
				socketsAttempted: 0,
				subscriptions: 45,
			});
			// anything that classifies by message instead of errorType gets the
			// same answer (subscriptionErrorMap.Unauthorized)
			expect(/Connection failed.+Unauthorized/.test(call.message)).toBe(true);
		});

		test('errorType is Transient for any other reason', async () => {
			dispatch(apiGraphql.REALTIME_CONNECTION_FAILURE, {
				report: 'open',
				reason: 'connect-deadline',
				isAuthFailure: false,
				message: 'Connection failed: Realtime open did not finish in 15000 ms',
				diagnostics: { socketsOpened: 1, socketStatus: 'CONNECTING' },
			});
			await flush();

			expect(errorHandler).toHaveBeenCalledTimes(1);
			expect(errorHandler).toHaveBeenCalledWith(
				expect.objectContaining({
					errorType: 'Transient',
					operation: 'Connect',
					process: 'subscribe',
				}),
			);
			expect(
				JSON.parse(errorHandler.mock.calls[0][0].errorInfo),
			).toMatchObject({ reason: 'connect-deadline', socketsOpened: 1 });
		});

		test('a repeat report is still one call', async () => {
			dispatch(apiGraphql.REALTIME_CONNECTION_FAILURE, {
				...authFailure,
				report: 'repeat',
			});
			await flush();

			expect(errorHandler).toHaveBeenCalledTimes(1);
			expect(
				JSON.parse(errorHandler.mock.calls[0][0].errorInfo).report,
			).toEqual('repeat');
		});

		test('report none calls no handler', async () => {
			dispatch(apiGraphql.REALTIME_CONNECTION_FAILURE, {
				...authFailure,
				report: 'none',
			});
			dispatch(apiGraphql.REALTIME_CONNECTION_FAILURE, {
				...authFailure,
				report: undefined,
			});
			dispatch(apiGraphql.REALTIME_CONNECTION_FAILURE, undefined);
			await flush();

			expect(errorHandler).not.toHaveBeenCalled();
		});

		test('realtimeRecovered calls no handler, and the next Connected still schedules the catch-up sync', async () => {
			dispatch(apiGraphql.REALTIME_RECOVERED, {});
			await flush();

			expect(errorHandler).not.toHaveBeenCalled();

			dispatch(apiGraphql.CONNECTION_STATE_CHANGE, {
				connectionState: apiGraphql.ConnectionState.ConnectionDisrupted,
			});
			expect(engine.connectionDisrupted).toBe(true);
			expect(scheduleSync).not.toHaveBeenCalled();

			dispatch(apiGraphql.CONNECTION_STATE_CHANGE, {
				connectionState: apiGraphql.ConnectionState.Connected,
			});
			expect(scheduleSync).toHaveBeenCalledTimes(1);
			expect(engine.connectionDisrupted).toBe(false);
			expect(errorHandler).not.toHaveBeenCalled();
		});

		test('an api event from another source is ignored', async () => {
			dispatch(apiGraphql.REALTIME_CONNECTION_FAILURE, authFailure, 'Other');
			dispatch(
				apiGraphql.CONNECTION_STATE_CHANGE,
				{ connectionState: apiGraphql.ConnectionState.ConnectionDisrupted },
				'Other',
			);
			await flush();

			expect(errorHandler).not.toHaveBeenCalled();
			expect(engine.connectionDisrupted).toBe(false);
		});

		test('a throwing errorHandler does not escape the listener', async () => {
			errorHandler.mockImplementation(() => {
				throw new Error('host handler is broken');
			});

			expect(() =>
				dispatch(apiGraphql.REALTIME_CONNECTION_FAILURE, authFailure),
			).not.toThrow();
			await flush();

			expect(errorHandler).toHaveBeenCalledTimes(1);
		});
	});
});

function jitteredRetrySyncProcessorSetup({
	rejectResponse,
	resolveResponse,
	coreMocks,
	errorHandler = () => null,
}: {
	rejectResponse?: any;
	resolveResponse?: any;
	coreMocks?: object;
	errorHandler?: () => null;
}) {
	mockGraphQl = jest.fn(
		() =>
			new Promise((res, rej) => {
				if (resolveResponse) {
					res(resolveResponse);
				} else if (rejectResponse) {
					rej(rejectResponse);
				}
			}),
	);
	// mock graphql to return a mockable observable
	jest.mock('@aws-amplify/api/internals', () => {
		const actualInternalAPIModule = jest.requireActual(
			'@aws-amplify/api/internals',
		);
		const actualInternalAPIInstance = actualInternalAPIModule.InternalAPI;

		return {
			...actualInternalAPIModule,
			InternalAPI: {
				...actualInternalAPIInstance,
				graphql: mockGraphQl,
			},
		};
	});

	jest.mock('@aws-amplify/core/internals/utils', () => ({
		...jest.requireActual('@aws-amplify/core/internals/utils'),
		// No need to retry any thrown errors right now,
		// so we're overriding jitteredExponentialRetry
		jitteredExponentialRetry: (fn, args) => fn(...args),
	}));

	jest.mock('@aws-amplify/core', () => ({
		...jest.requireActual('@aws-amplify/core'),
		...coreMocks,
	}));

	const SyncProcessorClass =
		require('../src/sync/processors/sync').SyncProcessor;

	const testInternalSchema = {
		namespaces: {},
		version: '',
	};

	const SyncProcessor = new SyncProcessorClass(
		testInternalSchema,
		null, // syncPredicates
		{ aws_appsync_authenticationType: 'userPools' },
		defaultAuthStrategy,
		errorHandler,
		{},
	);

	return SyncProcessor;
}
