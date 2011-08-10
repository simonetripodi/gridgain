// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
*  __  ____/___________(_)______  /__  ____/______ ____(_)_______
*  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
*  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
*  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
*/

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;

import java.io.*;
import java.util.*;

/**
 * Cache eviction response.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.5.0c.10082011
 */
public class GridCacheEvictionResponse<K, V> extends GridCacheMessage<K, V> {
    /** Future id. */
    private long futId;

    /** Rejected keys. */
    @GridToStringInclude
    private Collection<K> rejectedKeys = new HashSet<K>();

    /** Serialized rejected keys. */
    @GridToStringExclude
    private Collection<byte[]> rejectedKeyBytes;

    /**
     * Required by {@link Externalizable}.
     */
    public GridCacheEvictionResponse() {
        // No-op.
    }

    /**
     * @param futId Future id.
     */
    GridCacheEvictionResponse(long futId) {
        this.futId = futId;
    }

    /** {@inheritDoc} */
    @Override public void p2pMarshal(GridCacheContext<K, V> ctx) throws GridException {
        super.p2pMarshal(ctx);

        rejectedKeyBytes = marshalCollection(rejectedKeys, ctx);
    }

    /** {@inheritDoc} */
    @Override public void p2pUnmarshal(GridCacheContext<K, V> ctx, ClassLoader ldr) throws GridException {
        super.p2pUnmarshal(ctx, ldr);

        rejectedKeys = unmarshalCollection(rejectedKeyBytes, ctx, ldr);
    }

    /**
     * @return Future id.
     */
    long futureId() {
        return futId;
    }

    /**
     * @return Rejected keys.
     */
    Collection<K> rejectedKeys() {
        return rejectedKeys;
    }

    /**
     * Add rejected key to response.
     *
     * @param key Evicted key.
     */
    void addRejected(K key) {
        assert key != null;

        rejectedKeys.add(key);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeLong(futId);

        U.writeCollection(out, rejectedKeyBytes);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        futId = in.readLong();

        rejectedKeyBytes = U.readCollection(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheEvictionResponse.class, this);
    }
}
