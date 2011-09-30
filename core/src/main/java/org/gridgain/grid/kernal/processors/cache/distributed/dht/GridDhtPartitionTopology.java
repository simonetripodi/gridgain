// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.preloader.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * DHT partition topology.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.5.0c.22092011
 */
@GridToStringExclude
public interface GridDhtPartitionTopology<K, V> {
    /**
     * Locks the topology, usually during mapping on locks or transactions.
     */
    public void readLock();

    /**
     * Unlocks topology locked by {@link #readLock()} method.
     */
    public void readUnlock();

    /**
     * Updates join version.
     *
     * @param exchId Exchange ID.
     */
    public void updateJoinOrder(GridDhtPartitionExchangeId exchId);

    /**
     * Order of the node that joined last.
     *
     * @return Last joined order.
     */
    public long lastJoinOrder();

    /**
     * Pre-initializes this topology.
     *
     * @param exchId Exchange ID for this pre-initialization.
     * @throws GridException If failed.
     */
    public void beforeExchange(GridDhtPartitionExchangeId exchId) throws GridException;

    /**
     * Post-initializes this topology.
     *
     * @param exchId Exchange ID for this post-initialization.
     * @throws GridException If failed.
     */
    public void afterExchange(GridDhtPartitionExchangeId exchId) throws GridException;

    /**
     * @param topVer Topology version at the time of creation.
     * @param p Partition ID.
     * @param create If {@code true}, then partition will be created if it's not there.
     * @return Local partition.
     * @throws GridDhtInvalidPartitionException If partition is evicted or absent and
     *      does not belong to this node.
     */
    @Nullable public GridDhtLocalPartition<K, V> localPartition(int p, long topVer, boolean create)
        throws GridDhtInvalidPartitionException;

    /**
     * @param key Cache key.
     * @param create If {@code true}, then partition will be created if it's not there.
     * @return Local partition.
     * @throws GridDhtInvalidPartitionException If partition is evicted or absent and
     *      does not belong to this node.
     */
    @Nullable public GridDhtLocalPartition<K, V> localPartition(K key, boolean create)
        throws GridDhtInvalidPartitionException;

    /**
     * @return All local partitions.
     */
    public Collection<GridDhtLocalPartition<K, V>> localPartitions();

    /**
     * @return Local IDs.
     */
    public GridDhtPartitionMap localPartitionMap();

    /**
     * @return Current update sequence.
     */
    public long updateSequence();

    /**
     * @param p Partition ID.
     * @param topVer Topology version.
     * @return Collection of all nodes responsible for this partition with primary node being first.
     */
    public List<GridNode> nodes(int p, long topVer);

    /**
     * @param p Partition ID.
     * @return Collection of all nodes who {@code own} this partition.
     */
    public List<GridNode> owners(int p);

    /**
     * @param p Partition ID.
     * @return Collection of all nodes who {@code are preloading} this partition.
     */
    public List<GridNode> moving(int p);

    /**
     * @param onlyActive If {@code true}, then only {@code active} partitions will be returned.
     * @return Node IDs mapped to partitions.
     */
    public GridDhtPartitionFullMap partitionMap(boolean onlyActive);

    /**
     * @param topVer Topology version.
     * @param e Entry added to cache.
     * @return Local partition.
     */
    public GridDhtLocalPartition<K, V> onAdded(long topVer, GridDhtCacheEntry<K, V> e);

    /**
     * @param e Entry removed from cache.
     */
    public void onRemoved(GridDhtCacheEntry<K, V> e);

    /**
     * @param exchId Exchange ID.
     * @param partMap Update partition map.
     * @return Local partition map if there were evictions or {@code null} otherwise.
     */
    public GridDhtPartitionMap update(@Nullable GridDhtPartitionExchangeId exchId, GridDhtPartitionFullMap partMap);

    /**
     * @param exchId Exchange ID.
     * @param parts Partitions.
     * @return Local partition map if there were evictions or {@code null} otherwise.
     */
    @Nullable public GridDhtPartitionMap update(@Nullable GridDhtPartitionExchangeId exchId,
        GridDhtPartitionMap parts);

    /**
     * @param part Partition to own.
     * @return {@code True} if owned.
     */
    public boolean own(GridDhtLocalPartition<K, V> part);

    /**
     * @param nodeId Node to get partitions for.
     * @return Partitions for node.
     */
    @Nullable public GridDhtPartitionMap partitions(UUID nodeId);

    /**
     * Prints memory stats.
     *
     * @param threshold Threshold for number of entries.
     */
    public void printMemoryStats(int threshold);
}
