// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
import { Observer } from 'rxjs';

import {
	ReconnectEvent,
	ReconnectionMonitor,
} from '../../src/utils/ReconnectionMonitor';
import * as constants from '../../src/Providers/constants';
import { delay } from '../realtimeHarness';

const observerOf = (next: () => void, complete = jest.fn()) =>
	({ next, complete, error: jest.fn() }) as unknown as Observer<void>;

describe('ReconnectionMonitor', () => {
	let monitor: ReconnectionMonitor;

	const observers = () => (monitor as any).reconnectObservers as unknown[];
	const trigger = () => (monitor as any)._triggerReconnect();

	beforeEach(() => {
		monitor = new ReconnectionMonitor();
	});

	afterEach(() => {
		monitor.record(ReconnectEvent.HALT_RECONNECT);
	});

	test('removeObserver stops an observer being alerted and shrinks the array', () => {
		const first = jest.fn();
		const second = jest.fn();
		const firstObserver = observerOf(first);
		monitor.addObserver(firstObserver);
		monitor.addObserver(observerOf(second));

		monitor.removeObserver(firstObserver);
		trigger();

		expect(observers()).toHaveLength(1);
		expect(first).not.toHaveBeenCalled();
		expect(second).toHaveBeenCalledTimes(1);
	});

	test('an observer that removes itself while being notified does not break the round', () => {
		const alerted: string[] = [];
		const selfRemoving: Observer<void> = observerOf(() => {
			alerted.push('first');
			monitor.removeObserver(selfRemoving);
		});
		monitor.addObserver(selfRemoving);
		monitor.addObserver(observerOf(() => alerted.push('second')));

		trigger();

		// Every observer of this round is still alerted, because the round walks a
		// copy. Removing from the live array would skip the next entry.
		expect(alerted).toEqual(['first', 'second']);
		expect(observers()).toHaveLength(1);
	});

	test('close() completes live observers and empties the array', () => {
		const complete = jest.fn();
		monitor.addObserver(observerOf(jest.fn(), complete));

		monitor.close();

		expect(complete).toHaveBeenCalledTimes(1);
		expect(observers()).toHaveLength(0);
	});

	test('close() completes every observer although each one removes itself on complete', () => {
		// The provider's rxjs teardown removes the observer when it completes,
		// so close() walks an array that shrinks under it. A walk over the live
		// array completed every second observer only.
		const completed: number[] = [];
		const selfRemoving = (index: number): Observer<void> => {
			const observer: Observer<void> = observerOf(
				jest.fn(),
				jest.fn(() => {
					completed.push(index);
					monitor.removeObserver(observer);
				}),
			);

			return observer;
		};
		[0, 1, 2, 3, 4, 5].forEach(index => {
			monitor.addObserver(selfRemoving(index));
		});

		monitor.close();

		expect(completed).toEqual([0, 1, 2, 3, 4, 5]);
		expect(observers()).toHaveLength(0);
	});

	test('a reconnect round keeps the two handles external diagnostics read', async () => {
		Object.defineProperty(constants, 'RECONNECT_DELAY', { value: 5 });
		const next = jest.fn();
		monitor.addObserver(observerOf(next));

		monitor.record(ReconnectEvent.START_RECONNECT);
		expect((monitor as any).reconnectSetTimeoutId).toBeDefined();

		await delay(15);
		expect(next).toHaveBeenCalledTimes(1);
		// Named, not renamed: the simulator's lens reads both handles to tell a
		// halted reconnect monitor from a merely slow one.
		expect((monitor as any).reconnectIntervalId).toBeDefined();

		monitor.record(ReconnectEvent.HALT_RECONNECT);
		expect((monitor as any).reconnectIntervalId).toBeUndefined();
		expect((monitor as any).reconnectSetTimeoutId).toBeUndefined();
	});
});
