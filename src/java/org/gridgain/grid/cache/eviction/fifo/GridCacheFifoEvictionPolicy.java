// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.eviction.fifo;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.eviction.*;
import org.gridgain.grid.lang.utils.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;

import javax.management.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.lang.utils.GridConcurrentLinkedQueue.*;

/**
 * Eviction policy based on {@code First In First Out (FIFO)} algorithm. This
 * implementation is very efficient since it is lock-free and does not
 * create any additional table-like data structures. The {@code FIFO} ordering
 * information is maintained by attaching ordering metadata to cache entries.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.1.1c.14072011
 */
public class GridCacheFifoEvictionPolicy<K, V> implements GridCacheEvictionPolicy<K, V>,
    GridCacheFifoEvictionPolicyMBean {
    /** MBean server. */
    @GridMBeanServerResource
    @GridToStringExclude
    private MBeanServer jmx;

    /** Logger. */
    @GridLoggerResource
    private GridLogger log;

    /** Init flag. */
    private AtomicBoolean init = new AtomicBoolean(false);

    /** Tag. */
    private final String meta = UUID.randomUUID().toString();

    /** Maximum size. */
    private volatile int max = -1;

    /** LRU queue. */
    private final GridConcurrentLinkedQueue<GridCacheEntry<K, V>> queue =
        new GridConcurrentLinkedQueue<GridCacheEntry<K,V>>();

    /**
     * Constructs LRU eviction policy with all defaults.
     */
    public GridCacheFifoEvictionPolicy() {
        // No-op.
    }

    /**
     * Constructs LRU eviction policy with maximum size.
     *
     * @param max Maximum allowed size of cache before entry will start getting evicted.
     */
    public GridCacheFifoEvictionPolicy(int max) {
        A.ensure(max > 0, "max > 0");

        this.max = max;
    }

    /**
     * Gets maximum allowed size of cache before entry will start getting evicted.
     *
     * @return Maximum allowed size of cache before entry will start getting evicted.
     */
    @Override public int getMaxSize() {
        return max;
    }

    /**
     * Sets maximum allowed size of cache before entry will start getting evicted.
     *
     * @param max Maximum allowed size of cache before entry will start getting evicted.
     */
    @Override public void setMaxSize(int max) {
        A.ensure(max > 0, "max > 0");

        this.max = max;
    }

    /** {@inheritDoc} */
    @Override public int getCurrentSize() {
        return queue.size();
    }

    /** {@inheritDoc} */
    @Override public int getCurrentEdenSize() {
        return queue.eden();
    }

    /** {@inheritDoc} */
    @Override public void gc() {
        queue.gc(0);
    }

    /** {@inheritDoc} */
    @Override public long getAverageGcTime() {
        return queue.averageGcTime();
    }

    /** {@inheritDoc} */
    @Override public long getNodesGced() {
        return queue.nodesGced();
    }

    /** {@inheritDoc} */
    @Override public long getNodesCreated() {
        return queue.nodesCreated();
    }

    /** {@inheritDoc} */
    @Override public long getGcCalls() {
        return queue.gcCalls();
    }

    /** {@inheritDoc} */
    @Override public String queueFormatted() {
        return queue.toShortString();
    }

    /**
     * Gets read-only view on internal {@code FIFO} queue in proper order.
     *
     * @return Read-only view ono internal {@code 'FIFO'} queue.
     */
    public Collection<GridCacheEntry<K, V>> queue() {
        return Collections.unmodifiableCollection(queue);
    }

    /**
     * @param entry Entry to get info from.
     */
    private void registerMbean(GridCacheEntry<K, V> entry) {
        if (init.compareAndSet(false, true))
            CU.registerEvictionMBean(log, jmx, this, GridCacheFifoEvictionPolicyMBean.class, entry);
    }

    /** {@inheritDoc} */
    @Override public void onEntryAccessed(boolean rmv, GridCacheEntry<K, V> entry) {
        registerMbean(entry);

        if (!rmv)
            touch(entry);
        else {
            Node<GridCacheEntry<K, V>> n = entry.meta(meta);

            if (n != null)
                queue.clearNode(n);
        }

        shrink();

        queue.gc(max);
    }

    /**
     * @param entry Entry to touch.
     */
    private void touch(GridCacheEntry<K, V> entry) {
        Node<GridCacheEntry<K, V>> n = entry.meta(meta);

        if (n == null) {
            Node<GridCacheEntry<K, V>> old = entry.putMetaIfAbsent(meta, n = new Node<GridCacheEntry<K, V>>(entry));

            if (old == null) {
                queue.addNode(n);

                return;
            }
            else
                n = old;
        }

        if (!n.active()) {
            Node<GridCacheEntry<K, V>> replace = new Node<GridCacheEntry<K, V>>(entry);

            if (entry.replaceMeta(meta, n, replace))
                queue.addNode(replace);
        }
    }

    /**
     * Shrinks LRU queue to maximum allowed size.
     */
    private void shrink() {
        int i = 0;

        int max = this.max;

        while (queue.size() > max && i++ < max) {
            Node<GridCacheEntry<K, V>> n = queue.peekNode();

            if (n != null) {
                GridCacheEntry<K, V> e = n.value();

                if (e != null) {
                    if (queue.clearNode(n)) {
                        if (!e.evict()) {
                            // Move to the beginning again.
                            touch(e);
                        }
                    }
                }
            }
            else
                break;
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheFifoEvictionPolicy.class, this,
            "size", getCurrentSize(),
            "eden", getCurrentEdenSize(),
            "queue", queue.toShortString());
    }
}
