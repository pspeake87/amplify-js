// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
import { Observer } from 'rxjs';

import { RECONNECT_DELAY, RECONNECT_INTERVAL } from '../Providers/constants';

export enum ReconnectEvent {
	START_RECONNECT = 'START_RECONNECT',
	HALT_RECONNECT = 'HALT_RECONNECT',
	/**
	 * Ranchr fork: the device's network is back after an outage. The first
	 * attempt runs now; RECONNECT_DELAY is for a server-side disruption, where
	 * a pause before the retry is right (kick-the-tires walk 1, MA8).
	 */
	NETWORK_RETURNED = 'NETWORK_RETURNED',
}

/**
 * Captures the reconnect event logic used to determine when to reconnect to PubSub providers.
 *   Reconnect attempts are delayed by 5 seconds to let the interface settle.
 *   Attempting to reconnect only once creates unrecoverable states when the network state isn't
 *   supported by the browser, so this keeps retrying every minute until halted.
 */
export class ReconnectionMonitor {
	private reconnectObservers: Observer<void>[] = [];
	private reconnectIntervalId?: ReturnType<typeof setInterval>;
	private reconnectSetTimeoutId?: ReturnType<typeof setTimeout>;

	/**
	 * Add reconnect observer to the list of observers to alert on reconnect
	 */
	addObserver(reconnectObserver: Observer<void>) {
		this.reconnectObservers.push(reconnectObserver);
	}

	/**
	 * Stop alerting an observer. rxjs makes an unsubscribed Subscriber a no-op,
	 * so a leftover entry was harmless — but the array was never pruned, so it
	 * grew by one entry per subscription per DataStore restart and every replay
	 * walked the whole history.
	 */
	removeObserver(reconnectObserver: Observer<void>) {
		const index = this.reconnectObservers.indexOf(reconnectObserver);
		if (index !== -1) {
			this.reconnectObservers.splice(index, 1);
		}
	}

	/**
	 * Given a reconnect event, start the appropriate behavior
	 */
	record(event: ReconnectEvent) {
		if (event === ReconnectEvent.NETWORK_RETURNED) {
			this.record(ReconnectEvent.HALT_RECONNECT);
			// One turn later, not inside the state change that records it: the
			// disrupted state must be observable before the new attempt's
			// Connecting state replaces it.
			this.reconnectSetTimeoutId = setTimeout(() => {
				this.reconnectSetTimeoutId = undefined;
				this._triggerReconnect();
				this.reconnectIntervalId = setInterval(() => {
					this._triggerReconnect();
				}, RECONNECT_INTERVAL);
			}, 0);

			return;
		}

		if (event === ReconnectEvent.START_RECONNECT) {
			// If the reconnection hasn't been started
			if (
				this.reconnectSetTimeoutId === undefined &&
				this.reconnectIntervalId === undefined
			) {
				this.reconnectSetTimeoutId = setTimeout(() => {
					// Reconnect now
					this._triggerReconnect();
					// Retry reconnect every periodically until it works
					this.reconnectIntervalId = setInterval(() => {
						this._triggerReconnect();
					}, RECONNECT_INTERVAL);
				}, RECONNECT_DELAY);
			}
		}

		if (event === ReconnectEvent.HALT_RECONNECT) {
			if (this.reconnectIntervalId) {
				clearInterval(this.reconnectIntervalId);
				this.reconnectIntervalId = undefined;
			}
			if (this.reconnectSetTimeoutId) {
				clearTimeout(this.reconnectSetTimeoutId);
				this.reconnectSetTimeoutId = undefined;
			}
		}
	}

	/**
	 * Complete all reconnect observers
	 */
	close() {
		// A copy: complete() runs the teardown that removes the observer, and a
		// forEach over the live array skips the element after every removal.
		this.reconnectObservers.slice().forEach(reconnectObserver => {
			reconnectObserver.complete?.();
		});
		this.reconnectObservers = [];
	}

	private _triggerReconnect() {
		// A copy: an observer may remove itself while it is being notified.
		this.reconnectObservers.slice().forEach(reconnectObserver => {
			reconnectObserver.next?.();
		});
	}
}
