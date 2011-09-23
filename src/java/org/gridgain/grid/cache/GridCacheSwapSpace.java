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
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * TODO: Add interface description.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.5.0c.22092011
 */
@GridEnterpriseFeature
public interface GridCacheSwapSpace<K, V> extends GridMetadataAware, Iterable<Map.Entry<K, V>> {
    /**
     * @return Swap snapshot name.
     */
    public String name();

    /**
     * Gets absolute path to the swap snapshot root folder.
     *
     * @return Absolute path to the swap snapshot root folder.
     */
    public String path();

    /**
     * Gets swap snapshot creation time.
     *
     * @return Swap snapshot creation time.
     */
    public long createTime();

    /**
     * Number of entries in this snapshot.
     *
     * @return Number of entries in this snapshot.
     */
    public int size();

    /**
     * Check if this space is read-only. Read-only flag is set to {@code true} for swap
     * snapshots and is usually {@code false} for currently active swap space.
     *
     * @return Read-only flag for this swap space.
     */
    public boolean readonly();

    /**
     * Iterator over swap snapshot in ascending order. If {@link #readonly()} value is
     * {@code false} then {@link Iterator#remove()} method is supported, otherwise it
     * will throw {@link UnsupportedOperationException}.
     * <p>
     * This method is identical to calling {@link #iterator(boolean) iterator(true)} method.
     *
     * @return Iterator over swap snapshot in ascending order.
     */
    @Override public Iterator<Map.Entry<K, V>> iterator();

    /**
     * Iterator over swap snapshot in ascending order. If {@link #readonly()} value is
     * {@code false} then {@link Iterator#remove()} method is supported, otherwise it
     * will throw {@link UnsupportedOperationException}.
     *
     * @param asc If {@code true} then iterator will be in ascending order, otherwise
     *      the iterator is in descending order.
     * @return Iterator over swap space.
     */
    public Iterator<Map.Entry<K, V>> iterator(boolean asc);

    /**
     * Loops over the swap space evaluating given predicate for all swapped entries.
     * If this predicate will return {@code false} at any point, then iteration will
     * stop and {@code false} will be returned out of this method.
     *
     * @param p Predicate to check.
     * @return {@code True} if predicate passed for all entries, {@code false} otherwise.
     * @throws GridException If failed.
     */
    public boolean forAll(GridPredicate<Map.Entry<K, V>> p) throws GridException;

    /**
     * Asynchronously loops over the swap space evaluating given predicate for all
     * swapped entries. If this predicate will return {@code false} at any point, then
     * iteration will stop and {@code false} will be returned out of the future.
     *
     * @param p Predicate to check.
     * @return Future for this iteration.
     */
    public GridFuture<Boolean> forAllAsync(GridPredicate<Map.Entry<K, V>> p);

    /**
     * Loops over the swap space visiting every swapped entry and passing it into
     * given closure.
     *
     * @param c Visitor closure.
     * @throws GridException If failed.
     */
    public void forEach(GridInClosure<Map.Entry<K, V>> c) throws GridException;

    /**
     * Asynchronously loops over the swap space visiting every swapped entry and passing it into
     * given closure.
     *
     * @param c Visitor closure.
     * @return Future for this iteration.
     */
    public GridFuture<?> forEachAsync(GridInClosure<Map.Entry<K, V>> c);

    /**
     * Gets swapped value for the key without un-swapping it to memory.
     *
     * @param key Key to lookup value for.
     * @return Value for the key, possibly {@code null}.
     * @throws GridException If failed.
     */
    @Nullable public V get(K key) throws GridException;

    /**
     * Asynchronously gets swapped value for the key without un-swapping it to memory.
     *
     * @param key Key to lookup value for.
     * @return Grid future containing the value for the given key.
     */
    public GridFuture<V> getAsync(K key);

    /**
     * Gets swapped values for all given keys.
     *
     * @param keys Keys to get values for.
     * @return Map containing swapped key-value pairs for given entries, possibly empty.
     * @throws GridException If failed.
     */
    public Map<K, V> getAll(Collection<K> keys) throws GridException;

    /**
     * Asynchronously gets swapped values for the given keys without un-swapping it to memory.
     *
     * @param keys Keys to lookup values for.
     * @return Grid future containing the value for the given key.
     */
    public GridFuture<Map<K, V>> getAllAsync(Collection<K> keys);

    /**
     * Checks if given key is currently swapped in this space.
     *
     * @param key Key to check.
     * @return {@code True} if key was found in this swap space.
     * @throws GridException If failed.
     */
    public boolean containsKey(K key) throws GridException;

    /**
     * Checks if all given keys are contained in this swap space.
     *
     * @param keys Keys to check.
     * @return {@code True} if all given keys are contained in this swap space.
     * @throws GridException If failed.
     */
    public boolean containsAllKeys(Collection<K> keys) throws GridException;

    /**
     * Checks if any of the given keys is contained in this swap space.
     *
     * @param keys Keys to check.
     * @return {@code True} if at least one key was found in this swap space.
     * @throws GridException If failed.
     */
    public boolean containsAnyKeys(Collection<K> keys) throws GridException;

    /**
     * Stores key-value pair in this swap space. If key is currently cached,
     * then its value will be updated with given value prior to swapping.
     * <p>
     * This method is only available if {@link #readonly()} flag is {@code false}.
     *
     * @param key Key to swap.
     * @param val Value to swap.
     * @throws GridException If failed.
     */
    public void put(K key, V val) throws GridException;

    /**
     * Asynchronously stores key-value pair in this swap space. If key is currently cached,
     * then its value will be updated with given value prior to swapping.
     * <p>
     * This method is only available if {@link #readonly()} flag is {@code false}.
     *
     * @param key Key to swap.
     * @param val Value to swap.
     * @return Future for this operation.
     */
    public GridFuture<?> putAsync(K key, V val);

    /**
     * Stores given key-value pairs in this swap space. If any of the keys is currently cached,
     * then its value will be updated with given value prior to swapping.
     * <p>
     * This method is only available if {@link #readonly()} flag is {@code false}.
     *
     * @param map Key-value pairs to swap.
     * @throws GridException If failed.
     */
    public void putAll(Map<K, V> map) throws GridException;

    /**
     * Asynchronously swaps all given key-value pairs in this swap space. If any key is
     * currently cached, then its value will be updated with given value prior to swapping.
     * <p>
     * This method is only available if {@link #readonly()} flag is {@code false}.
     *
     * @param map Map of key-value pairs to swap.
     * @return Future for this operation.
     */
    public GridFuture<?> putAllAsync(Map<K, V> map);

    /**
     * Deletes this snapshot.
     *
     * @return {@code True} if snapshot was removed by this call.
     */
    public boolean delete();
}
