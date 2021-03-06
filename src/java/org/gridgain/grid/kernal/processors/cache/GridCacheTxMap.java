// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.lang.*;
import org.gridgain.grid.typedef.*;
import org.gridgain.grid.lang.utils.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Grid cache transaction read or write set.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.5.0c.22092011
 */
public class GridCacheTxMap<K, V> extends AbstractMap<K, GridCacheTxEntry<K, V>> implements Externalizable {
    /** Base transaction map. */
    private Map<K, GridCacheTxEntry<K, V>> txMap;

    /** Entry set. */
    private Set<Entry<K, GridCacheTxEntry<K, V>>> entrySet;

    /** Cached size. */
    private int size = -1;

    /** Empty flag. */
    private Boolean empty;

    /** Sealed flag. */
    private boolean sealed;

    /** Filter. */
    private GridPredicate<GridCacheTxEntry<K, V>> filter;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCacheTxMap() {
        // No-op.
    }

    /**
     * @param txMap Transaction map.
     * @param filter Filter.
     */
    public GridCacheTxMap(Map<K, GridCacheTxEntry<K, V>> txMap, GridPredicate<GridCacheTxEntry<K, V>> filter) {
        this.txMap = txMap;
        this.filter = filter;
    }

    /**
     * Seals this map.
     *
     * @return This map for chaining.
     */
    GridCacheTxMap<K, V> seal() {
        sealed = true;

        return this;
    }

    /**
     * @return Sealed flag.
     */
    boolean sealed() {
        return sealed;
    }

    /** {@inheritDoc} */
    @Override public Set<Entry<K, GridCacheTxEntry<K, V>>> entrySet() {
        if (entrySet == null) {
            entrySet = new GridSerializableSet<Entry<K, GridCacheTxEntry<K, V>>>() {
                private Set<Entry<K, GridCacheTxEntry<K, V>>> set = txMap.entrySet();

                @Override public Iterator<Entry<K, GridCacheTxEntry<K, V>>> iterator() {
                    return new GridSerializableIterator<Entry<K, GridCacheTxEntry<K, V>>>() {
                        private Iterator<Entry<K, GridCacheTxEntry<K, V>>> it = set.iterator();

                        private Entry<K, GridCacheTxEntry<K, V>> cur;

                        // Constructor.
                        {
                            advance();
                        }

                        @Override public boolean hasNext() {
                            return cur != null;
                        }

                        @Override public Entry<K, GridCacheTxEntry<K, V>> next() {
                            if (cur == null)
                                throw new NoSuchElementException();

                            Entry<K, GridCacheTxEntry<K, V>> e = cur;

                            advance();

                            return e;
                        }

                        @Override public void remove() {
                            throw new UnsupportedOperationException();
                        }

                        private void advance() {
                            cur = null;

                            while (cur == null && it.hasNext()) {
                                Entry<K, GridCacheTxEntry<K, V>> e = it.next();

                                if (filter.apply(e.getValue()))
                                    cur = e;
                            }
                        }
                    };
                }

                @Override public int size() {
                    return !sealed ? F.size(iterator()) : size == -1 ? size = F.size(iterator()) : size;
                }

                @Override public boolean isEmpty() {
                    return !sealed ? !iterator().hasNext() : empty == null ? empty = !iterator().hasNext() : empty;
                }
            };
        }

        return entrySet;
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return entrySet().isEmpty();
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return entrySet().size();
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(Object key) {
        return get(key) != null;
    }

    /** {@inheritDoc} */
    @Nullable
    @Override public GridCacheTxEntry<K, V> get(Object key) {
        GridCacheTxEntry<K, V> e = txMap.get(key);

        return e == null ? null : filter.apply(e) ? e : null;
    }

    /** {@inheritDoc} */
    @Override public GridCacheTxEntry<K, V> remove(Object key) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        throw new IllegalStateException("Transaction view map should never be serialized: " + this);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        throw new IllegalStateException("Transaction view map should never be serialized: " + this);
    }
}
