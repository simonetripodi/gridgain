// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.replicated;

import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.typedef.internal.*;

import java.io.*;

/**
 * Acknowledgement message for {@link GridReplicatedPreloadBatchRequest}.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.5.0c.31082011
 */
public class GridReplicatedPreloadBatchResponse<K, V> extends GridCacheMessage<K, V> implements GridCacheDeployable {
    /** Partition. */
    private int part;

    /** Batch index. */
    private int idx;

    /** Mode. */
    private int mod;

    /** Retry flag. */
    private boolean retry;

    /**
     * Required by {@link Externalizable}.
     */
    public GridReplicatedPreloadBatchResponse() {
        /* No-op. */
    }

    /** {@inheritDoc} */
    @Override public boolean allowForStartup() {
        return true;
    }

    /**
     * @param part Partition.
     * @param mod Mod.
     * @param idx Batch index.
     */
    public GridReplicatedPreloadBatchResponse(int part, int mod, int idx) {
        this(part, mod, idx, false);
    }

    /**
     * @param part Partition.
     * @param mod Mod.
     * @param idx Batch index.
     * @param retry Retry flag.
     */
    public GridReplicatedPreloadBatchResponse(int part, int mod, int idx, boolean retry) {
        this.part = part;
        this.mod = mod;
        this.idx = idx;
        this.retry = retry;
    }

    /**
     * @return Partition.
     */
    public int partition() {
        return part;
    }

    /**
     * @return Mod.
     */
    public int mod() {
        return mod;
    }

    /**
     * @return Batch index.
     */
    public int batchIndex() {
        return idx;
    }

    /**
     * @return {@code True} if sender should resend batch.
     */
    public boolean retry() {
        return retry;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeInt(part);
        out.writeInt(mod);
        out.writeInt(idx);
        out.writeBoolean(retry);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        part = in.readInt();
        mod = in.readInt();
        idx = in.readInt();
        retry = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridReplicatedPreloadBatchResponse.class, this);
    }
}
