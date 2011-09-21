// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.editions.*;
import org.gridgain.grid.lang.*;

import java.util.*;

/**
 * TODO: Add interface description.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.5.0c.20092011
 */
public interface GridCacheSwapApi<K, V> {
    /**
     * Swaps cache entries with given keys.
     *
     * @param key Key of the entry to swap.
     * @param keys Optional additional keys to swap.
     * @return {@code true} if at least one key was swapped.
     * @throws GridException If failed.
     */
    public boolean swap(K key, K... keys) throws GridException;

    /**
     * Swaps given key-value pair. If entry is cached, then it's value will
     * be overridden with with provided value before moving it to swap storage.
     *
     * @param key Key to store in swap.
     * @param val Value to store in swap.
     * @throws GridException If failed.
     */
    public void swap(K key, V val) throws GridException;

    /**
     * Iterates through all entries in cache and swaps the ones that pass provided predicates.
     * If predicate is not provided, then all cached entries will be swapped.
     *
     * @param p Optional predicate(s) to select cached entries to swap.
     * @throws GridException If failed.
     */
    public void swapAll(GridPredicate<GridCacheEntry<K, V>>... p) throws GridException;

    /**
     * Swaps all provided key-value pairs. If any of the keys are currently cached, then
     * their values will be overridden with provided values prior to moving them to swap.
     *
     * @param map Key-value pairs to swap.
     * @throws GridException If failed.
     */
    public void swapAll(Map<K, V> map) throws GridException;

    /**
     * Swaps all entries in cache that serve as backups for other entries.
     *
     * @throws GridException If failed.
     */
    public void swapBackups() throws GridException;

    /**
     * Creates snapshot of the current state of swap storage. Snapshot name will be
     * constructed as follows: {@code "cacheName-{MMDDYY-mmss}"}, where {@code M} is month,
     * {@code D} is day, {@code Y} is year, and {@code m} is minutes and {@code s} is seconds.
     * <p>
     * The {@link GridCacheSwapSpace#readonly()} flag on returned swap snapshot is set to {@code true}.
     *
     * @return Snapshot name.
     * @throws GridException If failed.
     */
    @GridEnterpriseFeature
    public GridCacheSwapSpace<K, V> takeSwapSnapshot() throws GridException;

    /**
     * Gets read-only collection of currently available swap snapshots. Note that you can
     * still delete any of the snapshots by calling {@link GridCacheSwapSpace#delete()} method.
     *
     * @return Read-only collection of currently available snapshots.
     */
    @GridEnterpriseFeature
    public Collection<GridCacheSwapSpace<K, V>> swapSnapshots();

    /**
     * Provides a view on swap storage. All method on the map will write-through or read-through
     * to/from underlying swap storage. The map supports two iteration orders, ascending or descending,
     * based on the latest timestamp of the swapped entry.
     * <p>
     * The {@link GridCacheSwapSpace#readonly()} flag on returned swap snapshot is set to {@code false}.
     *
     * @return View on the underlying swap store.
     */
    @GridEnterpriseFeature
    public GridCacheSwapSpace<K, V> swapspace();

    /**
     * Provides read-onlhy view on a given swap store snapshot. This method is similar to {@link #swapspace()}
     * method, except that it provides a view over swap snapshot taken before and not over current swap
     * store.
     * <p>
     * The {@link GridCacheSwapSpace#readonly()} flag on returned swap snapshot is set to {@code true}.
     *
     * @param name Snapshot name.
     * @return View on the specified swap snapshot.
     */
    @GridEnterpriseFeature
    public GridCacheSwapSpace<K, V> swapSnapshot(String name);

    /**
     * Deletes all swap snapshots. To delete individual snapshot, get it from
     * {@link #swapSnapshot(String)} method and call {@link GridCacheSwapSpace#delete()} on it.
     *
     * @return {@code True} if all snapshots were deleted.
     * @throws GridException If failed.
     */
    @GridEnterpriseFeature
    public boolean deleteSwapSnapshots() throws GridException;
}
