// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.eviction.always;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.eviction.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;

import javax.management.*;
import java.util.concurrent.atomic.*;

/**
 * Cache eviction policy that expires every entry essentially keeping the cache empty.
 * This eviction policy can be used whenever one cache is used to front another
 * and its size should be kept at {@code 0}.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.1.1c.14072011
 */
public class GridCacheAlwaysEvictionPolicy<K, V> implements GridCacheEvictionPolicy<K, V>,
    GridCacheAlwaysEvictionPolicyMBean {
    /** MBean server. */
    @GridMBeanServerResource
    @GridToStringExclude
    private MBeanServer jmx;

    /** Logger. */
    @GridLoggerResource
    private GridLogger log;

    /** Init flag. */
    private AtomicBoolean init = new AtomicBoolean(false);

    /**
     * @param entry Entry to get info from.
     */
    private void registerMbean(GridCacheEntry<K, V> entry) {
        if (init.compareAndSet(false, true))
            CU.registerEvictionMBean(log, jmx, this, GridCacheAlwaysEvictionPolicyMBean.class, entry);
    }

    /** {@inheritDoc} */
    @Override public void onEntryAccessed(boolean rmv, GridCacheEntry<K, V> entry) {
        registerMbean(entry);

        // Always evict.
        if (!rmv)
            entry.evict();
    }
}
