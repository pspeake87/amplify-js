// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
import { Observable, Observer, SubscriptionLike } from 'rxjs';

import { ReachabilityMonitor } from './datastoreReachability';

interface ConnectionStatus {
	// Might add other params in the future
	online: boolean;
}

/**
 * The network as the platform reports it (browser online/offline events,
 * NetInfo on React Native), and nothing else. The realtime socket is not a
 * network signal: the AppSync realtime provider owns the socket and replays
 * every subscription on a new link itself. The synthetic offline-then-online
 * pulse this class used to emit after a socket error (`socketDisconnected`)
 * made the sync engine tear down and rebuild all of its subscriptions on a
 * live connection; a burst of such rebuilds reached the AppSync cap of 200
 * subscriptions per connection.
 */
export default class DataStoreConnectivity {
	private connectionStatus: ConnectionStatus;
	private observer!: Observer<ConnectionStatus>;
	private subscription!: SubscriptionLike;
	constructor() {
		this.connectionStatus = {
			online: false,
		};
	}

	status(): Observable<ConnectionStatus> {
		if (this.observer) {
			throw new Error('Subscriber already exists');
		}

		return new Observable(observer => {
			this.observer = observer;

			this.subscription = ReachabilityMonitor.subscribe(({ online }) => {
				this.connectionStatus.online = online;

				const observerResult = { ...this.connectionStatus }; // copyOf status

				observer.next(observerResult);
			});

			return () => {
				this.unsubscribe();
			};
		});
	}

	unsubscribe() {
		if (this.subscription) {
			this.subscription.unsubscribe();
		}
	}

	// for consistency with other background processors.
	async stop() {
		this.unsubscribe();
	}
}
