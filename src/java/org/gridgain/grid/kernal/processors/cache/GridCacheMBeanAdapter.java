// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;

/**
 * Management bean that provides access to {@link GridCache}.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.5.0c.11092011
 */
class GridCacheMBeanAdapter implements GridCacheMBean {
    /** Cache context. */
    private GridCacheContext<?, ?> cctx;

    /** DHT context. */
    private GridCacheContext<?, ?> dhtCtx;

    /**
     * Creates MBean;
     *
     * @param cctx Cache context.
     */
    GridCacheMBeanAdapter(GridCacheContext<?, ?> cctx) {
        assert cctx != null;

        this.cctx = cctx;

        if (cctx.isNear())
            dhtCtx = cctx.near().dht().context();
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return cctx.name();
    }

    /** {@inheritDoc} */
    @Override public String metricsFormatted() {
        return String.valueOf(cctx.cache().metrics());
    }

    /** {@inheritDoc} */
    @Override public long getOverflowSize() {
        try {
            return cctx.cache().overflowSize();
        }
        catch (GridException ignored) {
            return -1;
        }
    }

    /** {@inheritDoc} */
    @Override public int getSize() {
        return cctx.cache().size();
    }

    /** {@inheritDoc} */
    @Override public int getKeySize() {
        return cctx.cache().keySize();
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return cctx.cache().isEmpty();
    }

    /** {@inheritDoc} */
    @Override public int getDhtEvictQueueCurrentSize() {
        return cctx.isNear() ? dhtCtx.evicts().evictQueueSize() : cctx.evicts().evictQueueSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxCommitQueueSize() {
        return cctx.tm().commitQueueSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxThreadMapSize() {
        return cctx.tm().threadMapSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxXidMapSize() {
        return cctx.tm().idMapSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxPrepareQueueSize() {
        return cctx.tm().prepareQueueSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxStartVersionCountsSize() {
        return cctx.tm().startVersionCountsSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxCommittedVersionsSize() {
        return cctx.tm().committedVersionsSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxRolledbackVersionsSize() {
        return cctx.tm().rolledbackVersionsSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtThreadMapSize() {
        return cctx.isNear() ? dhtCtx.tm().threadMapSize() : -1;
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtXidMapSize() {
        return cctx.isNear() ? dhtCtx.tm().idMapSize() : -1;
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtCommitQueueSize() {
        return cctx.isNear() ? dhtCtx.tm().commitQueueSize() : -1;
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtPrepareQueueSize() {
        return cctx.isNear() ? dhtCtx.tm().prepareQueueSize() : -1;
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtStartVersionCountsSize() {
        return cctx.isNear() ? dhtCtx.tm().startVersionCountsSize() : -1;
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtCommittedVersionsSize() {
        return cctx.isNear() ? dhtCtx.tm().committedVersionsSize() : -1;
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtRolledbackVersionsSize() {
        return cctx.isNear() ? dhtCtx.tm().rolledbackVersionsSize() : -1;
    }
}
