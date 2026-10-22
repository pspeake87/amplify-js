import { Observable } from 'rxjs';

let subscribed = 0;
let unsubscribed = 0;
const mockGraphQL = jest.fn(
	() =>
		new Observable(() => {
			subscribed++;

			return () => {
				unsubscribed++;
			};
		}),
);

import { SubscriptionProcessor } from '../src/sync/processors/subscription';
import { smallTestSchema } from './helpers';

// The device race: the per-model jobs are queued and wait on the auth modes
// when the stop lands. A slow read keeps them in that wait.
jest.mock('../src/sync/utils', () => {
	const actual = jest.requireActual('../src/sync/utils');

	return {
		...actual,
		getModelAuthModes: async (...args: unknown[]) => {
			await new Promise(resolve => setTimeout(resolve, 60));

			return (actual.getModelAuthModes as any)(...args);
		},
	};
});

jest.mock('@aws-amplify/api/internals', () => {
	const actual = jest.requireActual('@aws-amplify/api/internals');

	return {
		...actual,
		InternalAPI: { ...actual.InternalAPI, graphql: mockGraphQL },
	};
});

// Ranchr fork (kick-the-tires walk 1, MA3): a stop that lands while the
// round is still being built (the per-model jobs wait on the auth modes)
// found an empty handle list, and the subscriptions created afterwards were
// never unsubscribed: 42 live subscriptions on the socket after every held
// session, counted against AppSync's cap of 200 per connection.
describe('subscription processor: a round torn down during its start', () => {
	test('opens no subscription after the teardown, or closes the ones that slipped through', async () => {
		const { initSchema, DataStore } = require('../src/datastore/datastore');
		initSchema(smallTestSchema());
		await DataStore.start();
		const { schema } = (DataStore as any).storage.storage;

		const processor = new SubscriptionProcessor(
			schema,
			new WeakMap(),
			{
				aws_project_region: 'us-west-2',
				aws_appsync_graphqlEndpoint:
					'https://xxxxxxxxxxxxxxxxxxxxxx.appsync-api.us-west-2.amazonaws.com/graphql',
				aws_appsync_region: 'us-west-2',
				aws_appsync_authenticationType: 'AWS_LAMBDA',
				authProviders: { functionAuthProvider: async () => ({ token: 't' }) },
			},
			() => ['lambda'],
			() => null,
		);

		const [ctl] = processor.start();
		const handle = ctl.subscribe({});
		// The per-model jobs are queued now and wait on the auth modes
		await new Promise(resolve => setTimeout(resolve, 20));
		expect(subscribed).toBe(0);
		// The engine's stop: the round's handle goes first, then the processor
		// drains its jobs, which outlive the teardown.
		handle.unsubscribe();
		await processor.stop();

		// Let every queued job and its un-awaited retry settle
		for (let i = 0; i < 20; i++) {
			await new Promise(resolve => setTimeout(resolve, 10));
		}

		expect(unsubscribed).toBe(subscribed);
	});
});
