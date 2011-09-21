// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.controllers.affinity.impl;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.editions.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.typedef.*;
import org.jetbrains.annotations.*;

/**
 * Affinity utility methods.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.5.0c.20092011
 */
class GridAffinityUtils {
    /**
     * @param cacheName Cache name.
     * @return Affinity job.
     */
    static GridOutClosure<GridCacheAffinity> affinityJob(final String cacheName) {
        return new CO<GridCacheAffinity>() {
            @GridInstanceResource
            private Grid grid;

            @Nullable @Override public GridCacheAffinity apply() {
                assert grid != null;

                GridCache cache = grid.cache(cacheName);

                return cache == null ? null : cache.configuration().getAffinity();
            }
        };
    }

    /**
     * Ensure singleton.
     */
    private GridAffinityUtils() {
        // No-op.
    }
}
