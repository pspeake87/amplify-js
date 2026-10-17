// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * The Hub events (channel 'datastore') the IndexedDB adapter dispatches
 * between tabs. Exported from the package, so an app that listens for them
 * fails to compile on a rename instead of going quiet.
 */

/** This page's connection was closed because another tab asked for the database. */
export const STORAGE_RELEASED_EVENT = 'storageReleased';

/** A delete of the database met a connection in another tab that stays open. */
export const STORAGE_BLOCKED_EVENT = 'storageBlocked';
