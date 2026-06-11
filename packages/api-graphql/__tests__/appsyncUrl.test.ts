import {
	additionalHeadersFromOptions,
	getRealtimeEndpointUrl,
} from '../src/Providers/AWSWebSocketProvider/appsyncUrl';

describe('getRealtimeEndpointUrl', () => {
	test('events', () => {
		const httpUrl =
			'https://abcdefghijklmnopqrstuvwxyz.appsync-api.us-east-1.amazonaws.com/event';

		const res = getRealtimeEndpointUrl(httpUrl).toString();

		expect(res).toEqual(
			'wss://abcdefghijklmnopqrstuvwxyz.appsync-realtime-api.us-east-1.amazonaws.com/event/realtime',
		);
	});
});

describe('additionalHeadersFromOptions', () => {
	test('string authToken takes precedence as Authorization header', async () => {
		const { additionalCustomHeaders } = await additionalHeadersFromOptions({
			additionalHeaders: { Authorization: 'stale', 'x-custom': 'value' },
			authToken: 'explicit-token',
		});

		expect(additionalCustomHeaders).toEqual({
			Authorization: 'explicit-token',
			'x-custom': 'value',
		});
	});

	test('function authToken is re-invoked on each call and resolves fresh', async () => {
		let calls = 0;
		const authToken = async () => `token-${++calls}`;

		const first = await additionalHeadersFromOptions({ authToken });
		const second = await additionalHeadersFromOptions({ authToken });

		expect((first.additionalCustomHeaders as any).Authorization).toEqual(
			'token-1',
		);
		expect((second.additionalCustomHeaders as any).Authorization).toEqual(
			'token-2',
		);
	});

	test('function authToken resolving undefined does not set Authorization', async () => {
		const { additionalCustomHeaders } = await additionalHeadersFromOptions({
			additionalHeaders: { 'x-custom': 'value' },
			authToken: async () => undefined,
		});

		expect(additionalCustomHeaders).toEqual({ 'x-custom': 'value' });
	});

	test('function authToken overrides function-provided Authorization header', async () => {
		const { additionalCustomHeaders } = await additionalHeadersFromOptions({
			additionalHeaders: async () => ({ Authorization: 'from-headers-fn' }),
			authToken: async () => 'from-auth-token-fn',
		});

		expect((additionalCustomHeaders as any).Authorization).toEqual(
			'from-auth-token-fn',
		);
	});
});
