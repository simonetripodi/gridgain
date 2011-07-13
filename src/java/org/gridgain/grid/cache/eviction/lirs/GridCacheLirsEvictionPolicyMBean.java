// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.eviction.lirs;

import org.gridgain.grid.util.mbean.*;

/**
 * MBean for {@code LIRS} eviction policy.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.1.1c.14072011
 */
@GridMBeanDescription("MBean for LIRS cache eviction policy.")
public interface GridCacheLirsEvictionPolicyMBean {
    /**
     * Gets maximum allowed cache size.
     *
     * @return Maximum allowed cache size.
     */
    @GridMBeanDescription("Maximum allowed main stack size.")
    public int getMaxSize();

    /**
     * Sets maximum allowed cache size.
     *
     * @param max Maximum allowed cache size.
     */
    @GridMBeanDescription("Sets maximum allowed main stack size.")
    public void setMaxSize(int max);

    /**
     * Gets ratio for {@code HIRS} queue size relative to main stack size.
     *
     * @return Ratio for {@code HIRS} queue size relative to main stack size.
     */
    @GridMBeanDescription("Ratio for HIRS queue size relative to main stack size.")
    public double getQueueSizeRatio();

    /**
     * Gets maximum allowed size of {@code HIRS} queue before entries will start getting evicted.
     * This value is computed based on {@link #getQueueSizeRatio()} value.
     *
     * @return Maximum allowed size of {@code HIRS} queue before entries will start getting evicted.
     */
    @GridMBeanDescription("Maximum allowed HIRS queue size.")
    public int getMaxQueueSize();

    /**
     * Gets maximum allowed size of main stack This value is computed based on
     * {@link #getQueueSizeRatio()} value.
     *
     * @return Maximum allowed size of main stack.
     */
    @GridMBeanDescription("Maximum allowed HIRS queue size.")
    public int getMaxStackSize();

    /**
     * Gets current main stack size.
     *
     * @return Current main stack size.
     */
    @GridMBeanDescription("Current main stack size.")
    public int getCurrentStackSize();

    /**
     * Gets current {@code HIRS} queue size.
     *
     * @return Current {@code HIRS} queue size.
     */
    @GridMBeanDescription("Current HIRS queue size.")
    public int getCurrentQueueSize();

    /**
     * Gets number of voided nodes that remain on stack to be removed later for better concurrency.
     * This concept is similar to Garbage Collection Eden space, hence the name.
     *
     * @return Number of voided nodes that remain on stack.
     */
    @GridMBeanDescription("Current stack eden size.")
    public int getCurrentStackEdenSize();

    /**
     * Gets number of voided nodes that remain on queue to be removed later for better concurrency.
     * This concept is similar to Garbage Collection Eden space, hence the name.
     *
     * @return Number of voided nodes that remain on queue.
     */
    @GridMBeanDescription("Current queue eden size.")
    public int getCurrentQueueEdenSize();

    /**
     * Clears Eden space for all internal queues.
     */
    @GridMBeanDescription("Clear eden space from all internal queues.")
    public void gc();

    /**
     * Gets average time spent on GC'ing queue.
     *
     * @return Average time spent on GC'ing queue.
     */
    @GridMBeanDescription("Average time spent on GC'ing queue.")
    public long getAverageQueueGcTime();

    /**
     * Gets average time spent on GC'ing stack.
     *
     * @return Average time spent on GC'ing stack.
     */
    @GridMBeanDescription("Average time spent on GC'ing stack.")
    public long getAverageStackGcTime();

    /**
     * Gets number of queue nodes GC'ed by this policy.
     *
     * @return Number of queue nodes GC'ed by this policy.
     */
    @GridMBeanDescription("Number of queue nodes GC'ed.")
    public long getQueueNodesGced();

    /**
     * Gets number of stack nodes GC'ed by this policy.
     *
     * @return Number of stack nodes GC'ed by this policy.
     */
    @GridMBeanDescription("Number of stack nodes GC'ed.")
    public long getStackNodesGced();

    /**
     * Gets number of queue nodes created by this policy.
     *
     * @return Number of queue nodes created by this policy.
     */
    @GridMBeanDescription("Number of queue nodes created.")
    public long getQueueNodesCreated();

    /**
     * Gets number of stack nodes created by this policy.
     *
     * @return Number of stack nodes created by this policy.
     */
    @GridMBeanDescription("Number of stack nodes created.")
    public long getStackNodesCreated();

    /**
     * Gets number of queue GC calls executed by this policy.
     *
     * @return Number of queue GC calls executed by this policy.
     */
    @GridMBeanDescription("Number of queue GC calls.")
    public long getQueueGcCalls();

    /**
     * Gets number of stack GC calls executed by this policy.
     *
     * @return Number of stack GC calls executed by this policy.
     */
    @GridMBeanDescription("Number of stack GC calls.")
    public long getStackGcCalls();

    /**
     * Gets formatted representation of queue.
     *
     * @return Formatted representation of queue.
     */
    @GridMBeanDescription("Formatted representation of queue.")
    public String queueFormatted();

    /**
     * Gets formatted representation of stack.
     *
     * @return Formatted representation of stack.
     */
    @GridMBeanDescription("Formatted representation of stack.")
    public String stackFormatted();
}
