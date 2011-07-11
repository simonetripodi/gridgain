// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
*  __  ____/___________(_)______  /__  ____/______ ____(_)_______
*  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
*  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
*  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
*/

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.lang.utils.*;
import org.gridgain.grid.typedef.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;

import java.io.*;
import java.util.*;

/**
 * Cache eviction request.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.1.1c.11072011
 */
class GridCacheEvictionRequest<K, V> extends GridCacheMessage<K, V> implements GridCacheDeployable {
    /** Future id. */
    private long futId;

    /** Keys to clear from near and backup nodes. */
    @GridToStringInclude
    private Map<K, GridTuple2<GridCacheVersion, Boolean>> keys = new HashMap<K, GridTuple2<GridCacheVersion, Boolean>>();

    /** Serialized keys. */
    @GridToStringExclude
    private byte[] keyBytes;

    /**
     * Required by {@link Externalizable}.
     */
    public GridCacheEvictionRequest() {
        // No-op.
    }

    /**
     * @param futId Future id.
     */
    GridCacheEvictionRequest(long futId) {
        this.futId = futId;
    }

    /** {@inheritDoc} */
    @Override public void p2pMarshal(GridCacheContext<K, V> ctx) throws GridException {
        super.p2pMarshal(ctx);

        if (keys != null) {
            prepareObjects(keys.keySet(), ctx);

            keyBytes = U.marshal(ctx.marshaller(), keys).getEntireArray();
        }
    }

    /** {@inheritDoc} */
    @Override public void p2pUnmarshal(GridCacheContext<K, V> ctx, ClassLoader ldr) throws GridException {
        super.p2pUnmarshal(ctx, ldr);

        if (keyBytes != null)
            keys = U.unmarshal(ctx.marshaller(), new GridByteArrayList(keyBytes), ldr);
    }

    /**
     * @return Future id.
     */
    long futureId() {
        return futId;
    }

    /**
     * @return Key map.
     */
    Map<K, GridTuple2<GridCacheVersion, Boolean>> keys() {
        return keys;
    }

    /**
     * Add key to request.
     *
     * @param key Key to evict.
     * @param ver Entry version.
     * @param near {@code true} if key should be evicted from near cache.
     */
    void addKey(K key, GridCacheVersion ver, boolean near) {
        assert key != null;
        assert ver != null;

        keys.put(key, F.t(ver, near));
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeLong(futId);

        U.writeByteArray(out, keyBytes);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        futId = in.readLong();

        keyBytes = U.readByteArray(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheEvictionRequest.class, this);
    }
}
