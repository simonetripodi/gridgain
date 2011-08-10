// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache;

import org.gridgain.grid.util.mbean.*;

/**
 * This interface defines JMX view on {@link GridCache}.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.5.0c.10082011
 */
@GridMBeanDescription("MBean that provides access to cache descriptor.")
public interface GridCacheMBean {
    /**
     * Gets name of this cache.
     *
     * @return Cache name.
     */
    @GridMBeanDescription("Cache name.")
    public String name();

    /**
     * Gets metrics (statistics) for this cache.
     *
     * @return Cache metrics.
     */
    @GridMBeanDescription("Formatted cache metrics.")
    public String metricsFormatted();

    /**
     * Gets number of entries that was swapped to disk.
     *
     * @return Number of entries that was swapped to disk.
     */
    @GridMBeanDescription("Number of entries that was swapped to disk.")
    public long getOverflowSize();

    /**
     * Returns number of non-{@code null} values in the cache.
     *
     * @return Number of non-{@code null} values in the cache.
     */
    @GridMBeanDescription("Number of non-null values in the cache.")
    public int getSize();

    /**
     * Gets number of keys in the cache, possibly with {@code null} values.
     *
     * @return Number of keys in the cache.
     */
    @GridMBeanDescription("Number of keys in the cache (possibly with null values).")
    public int getKeySize();

    /**
     * Returns {@code true} if this cache is empty.
     *
     * @return {@code true} if this cache is empty.
     */
    @GridMBeanDescription("True if cache is empty.")
    public boolean isEmpty();

    /**
     * Gets current size of evict queue used to batch up evictions.
     *
     * @return Current size of evict queue.
     */
    @GridMBeanDescription("Current size of evict queue.")
    public int getDhtEvictQueueCurrentSize();

    /**
     * Gets transaction per-thread map size.
     *
     * @return Thread map size.
     */
    @GridMBeanDescription("Transaction per-thread map size.")
    public int getTxThreadMapSize();

    /**
     * Gets transaction per-Xid map size.
     *
     * @return Transaction per-Xid map size.
     */
    @GridMBeanDescription("Transaction per-Xid map size.")
    public int getTxXidMapSize();

    /**
     * Gets committed transaction queue size.
     *
     * @return Committed transaction queue size.
     */
    @GridMBeanDescription("Transaction committed queue size.")
    public int getTxCommitQueueSize();

    /**
     * Gets prepared transaction queue size.
     *
     * @return Prepared transaction queue size.
     */
    @GridMBeanDescription("Transaction prepared queue size.")
    public int getTxPrepareQueueSize();

    /**
     * Gets start version counts map size.
     *
     * @return Start version counts map size.
     */
    @GridMBeanDescription("Transaction start version counts map size.")
    public int getTxStartVersionCountsSize();

    /**
     * Gets number of cached committed transaction IDs.
     *
     * @return Number of cached committed transaction IDs.
     */
    @GridMBeanDescription("Transaction committed ID map size.")
    public int getTxCommittedVersionsSize();

    /**
     * Gets number of cached rolled back transaction IDs.
     *
     * @return Number of cached rolled back transaction IDs.
     */
    @GridMBeanDescription("Transaction rolled back ID map size.")
    public int getTxRolledbackVersionsSize();

    /**
     * Gets transaction DHT per-thread map size.
     *
     * @return DHT thread map size.
     */
    @GridMBeanDescription("Transaction DHT per-thread map size.")
    public int getTxDhtThreadMapSize();

    /**
     * Gets transaction DHT per-Xid map size.
     *
     * @return Transaction DHT per-Xid map size.
     */
    @GridMBeanDescription("Transaction DHT per-Xid map size.")
    public int getTxDhtXidMapSize();

    /**
     * Gets committed DHT transaction queue size.
     *
     * @return Committed DHT transaction queue size.
     */
    @GridMBeanDescription("Transaction DHT committed queue size.")
    public int getTxDhtCommitQueueSize();

    /**
     * Gets prepared DHT transaction queue size.
     *
     * @return Prepared DHT transaction queue size.
     */
    @GridMBeanDescription("Transaction DHT prepared queue size.")
    public int getTxDhtPrepareQueueSize();

    /**
     * Gets DHT start version counts map size.
     *
     * @return DHT start version counts map size.
     */
    @GridMBeanDescription("Transaction DHT start version counts map size.")
    public int getTxDhtStartVersionCountsSize();

    /**
     * Gets number of cached committed DHT transaction IDs.
     *
     * @return Number of cached committed DHT transaction IDs.
     */
    @GridMBeanDescription("Transaction DHT committed ID map size.")
    public int getTxDhtCommittedVersionsSize();

    /**
     * Gets number of cached rolled back DHT transaction IDs.
     *
     * @return Number of cached rolled back DHT transaction IDs.
     */
    @GridMBeanDescription("Transaction DHT rolled back ID map size.")
    public int getTxDhtRolledbackVersionsSize();
}
