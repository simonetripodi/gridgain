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
import org.gridgain.grid.lang.*;
import org.gridgain.grid.typedef.*;
import org.gridgain.grid.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

import static org.gridgain.grid.cache.GridCacheFlag.*;

/**
 * Concurrent implementation of cache map.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.5.0c.22082011
 */
public class GridCacheConcurrentMap<K, V> /*implements /* ConcurrentMap<K, V>,*/ {
    /** Random. */
    private static final Random RAND = new Random();

    /** The default load factor for this map. */
    private static final float DEFAULT_LOAD_FACTOR = 0.75f;

    /** The default concurrency level for this map. */
    private static final int DEFAULT_CONCURRENCY_LEVEL = 16;

    /**
     * The maximum capacity, used if a higher value is implicitly specified by either
     * of the constructors with arguments. Must be a power of two <= 1<<30 to ensure
     * that entries are indexable using integers.
     */
    private static final int MAXIMUM_CAPACITY = 1 << 30;

    /** The maximum number of segments to allow. */
    private static final int MAX_SEGMENTS = 1 << 16; // slightly conservative

    /**
     * Number of unsynchronized retries in size and containsValue
     * methods before resorting to locking. This is used to avoid
     * unbounded retries if tables undergo continuous modification
     * which would make it impossible to obtain an accurate result.
     */
    private static final int RETRIES_BEFORE_LOCK = 2;

    /**
     * Mask value for indexing into segments. The upper bits of a
     * key's hash code are used to choose the segment.
     */
    private final int segmentMask;

    /** Shift value for indexing within segments. */
    private final int segmentShift;

    /** The segments, each of which is a specialized hash table. */
    private final Segment<K, V>[] segments;

    /** Segment bits. */
    private final BitSet segBits;

    /** */
    private GridCacheMapEntryFactory<K, V> factory;

    /** Cache context. */
    private final GridCacheContext<K, V> ctx;

    /** */
    private final AtomicInteger bucketCnt = new AtomicInteger();

    /** */
    private final AtomicInteger pubSize = new AtomicInteger();

    /** */
    private final AtomicInteger size = new AtomicInteger();

    /** Filters cache internal entry. */
    private static final P1<GridCacheEntry<?, ?>> NON_INTERNAL =
        new P1<GridCacheEntry<?, ?>>() {
            @Override public boolean apply(GridCacheEntry<?, ?> entry) {
                return !(entry.getKey() instanceof GridCacheInternal);
            }
        };

    /** Non-internal predicate array. */
    public static final GridPredicate[] NON_INTERNAL_ARR = new P1[]{NON_INTERNAL};

    /**
     * Applies a supplemental hash function to a given hashCode, which
     * defends against poor quality hash functions.  This is critical
     * because ConcurrentHashMap uses power-of-two length hash tables,
     * that otherwise encounter collisions for hashCodes that do not
     * differ in lower or upper bits.
     *
     * @param h Value to hash.
     * @return Hash value.
     */
    private static int hash(int h) {
        // Spread bits to regularize both segment and index locations,
        // using variant of single-word Wang/Jenkins hash.
        h += (h <<  15) ^ 0xffffcd7d;
        h ^= (h >>> 10);
        h += (h <<   3);
        h ^= (h >>>  6);
        h += (h <<   2) + (h << 14);

        return h ^ (h >>> 16);
    }

    /**
     * Returns the segment that should be used for key with given hash
     *
     * @param hash The hash code for the key.
     * @return The segment.
     */
    private Segment<K, V> segmentFor(int hash) {
        return segments[(hash >>> segmentShift) & segmentMask];
    }

    /**
     * Creates a new, empty map with the specified initial
     * capacity, load factor and concurrency level.
     *
     * @param ctx Cache context.
     * @param initialCapacity the initial capacity. The implementation
     *      performs internal sizing to accommodate this many elements.
     * @param loadFactor  the load factor threshold, used to control resizing.
     *      Resizing may be performed when the average number of elements per
     *      bin exceeds this threshold.
     * @param concurrencyLevel the estimated number of concurrently
     *      updating threads. The implementation performs internal sizing
     *      to try to accommodate this many threads.
     * @throws IllegalArgumentException if the initial capacity is
     *      negative or the load factor or concurrencyLevel are
     *      non-positive.
     */
    @SuppressWarnings({"unchecked"})
    private GridCacheConcurrentMap(GridCacheContext<K, V> ctx, int initialCapacity, float loadFactor,
        int concurrencyLevel) {
        this.ctx = ctx;

        if (!(loadFactor > 0) || initialCapacity < 0 || concurrencyLevel <= 0)
            throw new IllegalArgumentException();

        if (concurrencyLevel > MAX_SEGMENTS)
            concurrencyLevel = MAX_SEGMENTS;

        // Find power-of-two sizes best matching arguments
        int sshift = 0;
        int ssize = 1;

        while (ssize < concurrencyLevel) {
            ++sshift;
            ssize <<= 1;
        }

        segmentShift = 32 - sshift;
        segmentMask = ssize - 1;
        segments = new Segment[ssize];

        if (initialCapacity > MAXIMUM_CAPACITY)
            initialCapacity = MAXIMUM_CAPACITY;

        int c = initialCapacity / ssize;

        if (c * ssize < initialCapacity)
            ++c;

        int cap = 1;

        while (cap < c)
            cap <<= 1;

        for (int i = 0; i < segments.length; ++i)
            segments[i] = new Segment<K, V>(i, cap, loadFactor);

        segBits = new BitSet(segments.length);
    }

    /**
     * Creates a new, empty map with the specified initial capacity
     * and load factor and with the default concurrencyLevel (16).
     *
     * @param ctx Cache context.
     * @param initialCapacity The implementation performs internal
     *      sizing to accommodate this many elements.
     * @param loadFactor  the load factor threshold, used to control resizing.
     *      Resizing may be performed when the average number of elements per
     *      bin exceeds this threshold.
     * @throws IllegalArgumentException if the initial capacity of
     *      elements is negative or the load factor is non-positive.
     */
    public GridCacheConcurrentMap(GridCacheContext<K, V> ctx, int initialCapacity, float loadFactor) {
        this(ctx, initialCapacity, loadFactor, DEFAULT_CONCURRENCY_LEVEL);
    }

    /**
     * Creates a new, empty map with the specified initial capacity,
     * and with default load factor (0.75) and concurrencyLevel (16).
     *
     * @param ctx Cache context.
     * @param initialCapacity the initial capacity. The implementation
     *      performs internal sizing to accommodate this many elements.
     * @throws IllegalArgumentException if the initial capacity of
     *      elements is negative.
     */
    public GridCacheConcurrentMap(GridCacheContext<K, V> ctx, int initialCapacity) {
        this(ctx, initialCapacity, DEFAULT_LOAD_FACTOR, DEFAULT_CONCURRENCY_LEVEL);
    }

    /**
     * Sets factory for entries.
     *
     * @param factory Entry factory.
     */
    public void setEntryFactory(GridCacheMapEntryFactory<K, V> factory) {
        assert factory != null;

        this.factory = factory;
    }

    /**
     * @return {@code True} if this map is empty.
     */
    public boolean isEmpty() {
        Segment<K, V>[] segments = this.segments;

        /*
         * We keep track of per-segment modCounts to avoid ABA
         * problems in which an element in one segment was added and
         * in another removed during traversal, in which case the
         * table was never actually empty at any point. Note the
         * similar use of modCounts in the size() and containsValue()
         * methods, which are the only other methods also susceptible
         * to ABA problems.
         */

        int[] mc = new int[segments.length];

        int mcsum = 0;

        for (int i = 0; i < segments.length; ++i) {
            if (segments[i].cnt != 0)
                return false;
            else
                mcsum += mc[i] = segments[i].modCnt;
        }

        // If mcsum happens to be zero, then we know we got a snapshot
        // before any modifications at all were made.  This is
        // probably common enough to bother tracking.
        if (mcsum != 0) {
            for (int i = 0; i < segments.length; ++i) {
                if (segments[i].cnt != 0 || mc[i] != segments[i].modCnt)
                    return false;
            }
        }

        return true;
    }

    /**
     * Returns the number of key-value mappings in this map.
     *
     * @return the number of key-value mappings in this map.
     */
    public int size() {
        return size.get();
    }

    /**
     * For test purposes.
     *
     * @return Map size.
     */
    @SuppressWarnings( {"LockAcquiredButNotSafelyReleased"})
    public int size0() {
        Segment<K, V>[] segments = this.segments;

        long sum = 0;

        long check = 0;

        int[] mc = new int[segments.length];

        // Try a few times to get accurate count. On failure due to
        // continuous async changes in table, resort to locking.
        for (int k = 0; k < RETRIES_BEFORE_LOCK; ++k) {
            check = 0;

            sum = 0;

            int mcsum = 0;

            for (int i = 0; i < segments.length; ++i) {
                sum += segments[i].cnt;

                mcsum += mc[i] = segments[i].modCnt;
            }

            if (mcsum != 0) {
                for (int i = 0; i < segments.length; ++i) {
                    check += segments[i].cnt;

                    if (mc[i] != segments[i].modCnt) {
                        check = -1; // force retry

                        break;
                    }
                }
            }

            if (check == sum)
                break;
        }

        if (check != sum) { // Resort to locking all segments
            sum = 0;

            for (Segment<K, V> segment : segments)
                segment.readLock().lock();

            for (Segment<K, V> segment : segments)
                sum += segment.count();

            for (Segment<K, V> segment : segments)
                segment.readLock().unlock();
        }

        return sum > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int)sum;
    }

    /**
     * @param key Key.
     * @return {@code True} if map contains mapping for provided key.
     */
    public boolean containsKey(Object key) {
        int hash = hash(key.hashCode());

        return segmentFor(hash).containsKey(key, hash);
    }

    /**
     * Collection of all (possibly {@code null}) values.
     *
     * @param filter Filter.
     * @return a collection view of the values contained in this map.
     */
    public Collection<V> allValues(GridPredicate<? super GridCacheEntry<K, V>>[] filter) {
        return new Values<K, V>(this, filter);
    }

    /**
     * TODO: optimize, what if we have 500 internals and only 1 public?
     *
     * @return Random entry out of hash map.
     */
    @Nullable public GridCacheMapEntry<K, V> randomEntry() {
        while (true) {
            if (publicSize() == 0)
                return null;

            int segIdx = segBits.nextSetBit(RAND.nextInt(segments.length));

            if (segIdx == -1)
                segIdx = segBits.nextSetBit(0);

            if (segIdx == -1)
                return null;

            Segment<K, V> seg = segments[segIdx];

            if (seg.count() == 0)
                continue;

            GridCacheMapEntry<K, V> entry = seg.randomEntry();

            if (entry instanceof GridCacheInternal)
                continue; // Retry.

            return entry;
        }
    }

    /**
     * @return Public size.
     */
    public int publicSize() {
        return pubSize.get();
    }

    /**
     * @return Bucket count.
     */
    public int buckets() {
        return bucketCnt.get();
    }

    /**
     * @return Average bucket size.
     */
    public double averageBucketSize() {
        int bucketsCnt = bucketCnt.get();

        if (bucketsCnt == 0)
            return 0;

        return (double)size() / bucketsCnt;
    }

    /**
     * Returns the entry associated with the specified key in the
     * HashMap.  Returns null if the HashMap contains no mapping
     * for this key.
     *
     * @param key Key.
     * @return Entry.
     */
    @Nullable public GridCacheMapEntry<K, V> getEntry(Object key) {
        assert key != null;

        int hash = hash(key.hashCode());

        return segmentFor(hash).get(key, hash);
    }

    /**
     * @param topVer Topology version.
     * @param key Key.
     * @param val Value.
     * @param ttl Time to live.
     * @return Cache entry for corresponding key-value pair.
     */
    public GridCacheMapEntry<K, V> putEntry(long topVer, K key, @Nullable V val, long ttl) {
        assert key != null;
        assert val != null;

        int hash = hash(key.hashCode());

        return segmentFor(hash).put(key, hash, val, topVer, ttl);
    }

    /**
     * Copies all of the mappings from the specified map to this map
     * These mappings will replace any mappings that
     * this map had for any of the keys currently in the specified map.
     *
     * @param m mappings to be stored in this map.
     * @param ttl Time to live.
     * @throws NullPointerException If the specified map is null.
     */
    public void putAll(Map<? extends K, ? extends V> m, long ttl) {
        for (Map.Entry<? extends K, ? extends V> e : m.entrySet())
            putEntry(-1, e.getKey(), e.getValue(), ttl);
    }

    /**
     * Removes and returns the entry associated with the specified key
     * in the HashMap. Returns null if the HashMap contains no mapping
     * for this key.
     *
     * @param key Key.
     * @return Removed entry, possibly {@code null}.
     */
    @Nullable public GridCacheMapEntry<K, V> removeEntry(Object key) {
        assert key != null;

        int hash = hash(key.hashCode());

        return segmentFor(hash).remove(key, hash);
    }

    /**
     * Removes the mapping for this key from this map if present.
     *
     * @param  key key whose mapping is to be removed from the map.
     * @return previous value associated with specified key, or {@code null}
     *           if there was no mapping for key.  A {@code null} return can
     *           also indicate that the map previously associated {@code null}
     *           with the specified key.
     */
    @Nullable public V remove(Object key) {
        GridCacheMapEntry<K, V> e = removeEntry(key);

        return (e == null ? null : e.rawGet());
    }

    /**
     * Entry wrapper set.
     *
     * @param filter Filter.
     * @return Entry wrapper set.
     */
    @SuppressWarnings({"unchecked", "RedundantCast"})
    public Set<GridCacheEntryImpl<K, V>> wrappers(GridPredicate<? super GridCacheEntry<K, V>>[] filter) {
        return (Set<GridCacheEntryImpl<K, V>>)(Set<? extends GridCacheEntry<K, V>>)entries(filter);
    }

    /**
     * Entry wrapper set casted to projections.
     *
     * @param filter Filter to check.
     * @return Entry projections set.
     */
    @SuppressWarnings({"unchecked", "RedundantCast"})
    public Set<GridCacheEntry<K, V>> projections(GridPredicate<? super GridCacheEntry<K, V>>[] filter) {
        return (Set<GridCacheEntry<K, V>>)(Set<? extends GridCacheEntry<K, V>>)wrappers(filter);
    }

    /**
     * Same as {@link #wrappers(GridPredicate[])}
     *
     * @param filter Filter.
     * @return a collection view of the mappings contained in this map.
     */
    @SuppressWarnings({"unchecked"})
    public Set<GridCacheEntry<K, V>> entries(GridPredicate<? super GridCacheEntry<K, V>>[] filter) {
        return new EntrySet<K, V>(this, filter);
    }

    /**
     * Internal entry set.
     *
     * @param filter Filter.
     * @return a collection view of the mappings contained in this map.
     */
    public Set<GridCacheEntryEx<K, V>> entries0(GridPredicate<? super GridCacheEntry<K, V>>[] filter) {
        return new Set0<K, V>(this, filter);
    }

    /**
     * Key set.
     *
     * @param filter Filter.
     * @return a set view of the keys contained in this map.
     */
    public Set<K> keySet(GridPredicate<? super GridCacheEntry<K, V>>[] filter) {
        return new KeySet<K, V>(this, filter);
    }

    /**
     * Collection of non-{@code null} values.
     *
     * @param filter Filter.
     * @return a collection view of the values contained in this map.
     */
    public Collection<V> values(GridPredicate<? super GridCacheEntry<K, V>>[] filter) {
        return F.view(allValues(filter), F.<V>notNull());
    }

    /**
     * TODO: remove this method.
     *
     * @param topVer Topology version.
     * @param key Cache key.
     * @param hash Key hash value.
     * @param val Entry value.
     * @param next Next entry in the linked list.
     * @param ttl Time to live.
     * @return New cache entry.
     */
    @SuppressWarnings( {"unchecked"})
    GridCacheMapEntry<K, V> create(long topVer, Object key, int hash, Object val,
        GridCacheMapEntry<K, V> next, long ttl) {
        return factory.create(ctx, topVer, (K)key, hash, (V)val, next, ttl);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheConcurrentMap.class, this, "size", size(),
            "pubSize", publicSize(), "buckets", bucketCnt.get());
    }

    /**
     * Hash bucket.
     */
    private static class Bucket<K, V> {
        /** */
        private volatile int cnt;

        /** */
        private volatile HashEntry<K, V> entry;

        /**
         * @param entry Entry.
         */
        private Bucket(HashEntry<K, V> entry) {
            assert entry != null;

            this.entry = entry;

            for (HashEntry<K, V> e = entry; e != null; e = e.next())
                cnt++;
        }

        /**
         *
         */
        private Bucket() {
            // No-op.
        }

        /**
         * @return Bucket count.
         */
        int count() {
            return cnt;
        }

        /**
         * @return Entry.
         */
        HashEntry<K, V> entry() {
            return entry;
        }

        /**
         * @param entry New root.
         * @return New count.
         */
        int onAdd(HashEntry<K, V> entry) {
            this.entry = entry;

            return ++cnt;
        }

        /**
         * @param entry New root.
         * @return New count.
         */
        int onRemove(HashEntry<K, V> entry) {
            this.entry = entry;

            return --cnt;
        }

        /**
         * @return New count.
         */
        int onRemove() {
            return --cnt;
        }


        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Bucket.class, this);
        }

        /**
         * @param i Size.
         * @return Bucket array.
         */
        @SuppressWarnings("unchecked")
        static <K, V> Bucket<K, V>[] newArray(int i) {
            return new Bucket[i];
        }
    }

    /**
     * ConcurrentHashMap list entry. Note that this is never exported
     * out as a user-visible Map.Entry.
     *
     * Because the value field is volatile, not final, it is legal wrt
     * the Java Memory Model for an unsynchronized reader to see null
     * instead of initial value when read via a data race.  Although a
     * reordering leading to this is not likely to ever actually
     * occur, the Segment.readValueUnderLock method is used as a
     * backup in case a null (pre-initialized) value is ever seen in
     * an unsynchronized access method.
     */
    private static final class HashEntry<K, V> {
        /** */
        private final K key;

        /** */
        private final int hash;

        /** */
        private final GridCacheMapEntry<K, V> value;

        /** */
        private final HashEntry<K, V> next;

        /**
         * @param key Key.
         * @param hash Hash.
         * @param next Next.
         * @param value Value.
         */
        HashEntry(K key, int hash, HashEntry<K, V> next, GridCacheMapEntry<K, V> value) {
            this.key = key;
            this.hash = hash;
            this.next = next;
            this.value = value;
        }

        /**
         * @return Value.
         */
        public GridCacheMapEntry<K, V> value() {
            return value;
        }

        /**
         * @return Key.
         */
        public K key() {
            return key;
        }

        /**
         * @return Next.
         */
        @Nullable public HashEntry<K, V> next() {
            return next;
        }

        /**
         * @return Hash.
         */
        public int hash() {
            return hash;
        }
    }

    /**
     * Segments are specialized versions of hash tables.  This
     * subclasses from ReentrantLock opportunistically, just to
     * simplify some locking and avoid separate construction.
     */
    private class Segment<K, V> extends ReentrantReadWriteLock {
        /*
         * Segments maintain a table of entry lists that are ALWAYS
         * kept in a consistent state, so can be read without locking.
         * Next fields of nodes are immutable (final).  All list
         * additions are performed at the front of each bin. This
         * makes it easy to check changes, and also fast to traverse.
         * When nodes would otherwise be changed, new nodes are
         * created to replace them. This works well for hash tables
         * since the bin lists tend to be short. (The average length
         * is less than two for the default load factor threshold.)
         *
         * Read operations can thus proceed without locking, but rely
         * on selected uses of volatiles to ensure that completed
         * write operations performed by other threads are
         * noticed. For most purposes, the "count" field, tracking the
         * number of elements, serves as that volatile variable
         * ensuring visibility.  This is convenient because this field
         * needs to be read in many read operations anyway:
         *
         *   - All (unsynchronized) read operations must first read the
         *     "count" field, and should not look at table entries if
         *     it is 0.
         *
         *   - All (synchronized) write operations should write to
         *     the "count" field after structurally changing any bin.
         *     The operations must not take any action that could even
         *     momentarily cause a concurrent read operation to see
         *     inconsistent data. This is made easier by the nature of
         *     the read operations in Map. For example, no operation
         *     can reveal that the table has grown but the threshold
         *     has not yet been updated, so there are no atomicity
         *     requirements for this with respect to reads.
         *
         * As a guide, all critical volatile reads and writes to the
         * count field are marked in code comments.
         */

        /**
         * The number of elements in this segment's region.
         */
        private volatile int cnt;

        /**
         * Number of updates that alter the size of the table. This is
         * used during bulk-read methods to make sure they see a
         * consistent snapshot: If modCounts change during a traversal
         * of segments computing size or checking containsValue, then
         * we might have an inconsistent view of state so (usually)
         * must retry.
         */
        private int modCnt;

        /**
         * The table is rehashed when its size exceeds this threshold.
         * (The value of this field is always <tt>(int)(capacity * loadFactor)</tt>.)
         */
        private int threshold;

        /** The per-segment table. */
        private volatile Bucket<K, V>[] table;

        /** Existing buckets. */
        private volatile BitSet bucketBits;

        /** Segment index. */
        private final int segIdx;

        /**
         * The load factor for the hash table. Even though this value
         * is same for all segments, it is replicated to avoid needing
         * links to outer object.
         * @serial
         */
        private final float loadFactor;

        /**
         * @param segIdx Segment index.
         * @param initialCapacity Initial capacity.
         * @param lf Load factor.
         */
        Segment(int segIdx, int initialCapacity, float lf) {
            this.segIdx = segIdx;

            loadFactor = lf;

            setTable(Bucket.<K, V>newArray(initialCapacity));

            bucketBits = new BitSet(table.length);
        }

        /**
         * Sets table to new HashEntry array.
         * Call only while holding lock or in constructor.
         *
         * @param newTable New table.
         */
        void setTable(Bucket<K, V>[] newTable) {
            threshold = (int)(newTable.length * loadFactor);

            table = newTable;
        }

        /**
         * Returns properly casted first entry of bin for given hash.
         *
         * @param hash Hash.
         * @return Entry for hash.
         */
        HashEntry<K, V> getFirst(int hash) {
            Bucket<K, V>[] tab = table;

            return tab[hash & (tab.length - 1)].entry();
        }

        /**
         * Reads value field of an entry under lock. Called if value
         * field ever appears to be null. This is possible only if a
         * compiler happens to reorder a HashEntry initialization with
         * its table assignment, which is legal under memory model
         * but is not known to ever occur.
         *
         * @param e Entry whose value to read.
         * @return Read value.
         */
        GridCacheMapEntry<K, V> readValueUnderLock(HashEntry<K, V> e) {
            writeLock().lock();

            try {
                return e.value;
            }
            finally {
                writeLock().unlock();
            }
        }

        /**
         * @param key Key.
         * @param hash Hash.
         * @return Value.
         */
        @Nullable GridCacheMapEntry<K, V> get(Object key, int hash) {
            if (cnt != 0) { // read-volatile
                HashEntry<K, V> e = getFirst(hash);

                while (e != null) {
                    if (e.hash == hash && key.equals(e.key)) {
                        GridCacheMapEntry<K, V> v = e.value;

                        if (v != null)
                            return v;

                        return readValueUnderLock(e); // recheck
                    }

                    e = e.next;
                }
            }

            return null;
        }

        /**
         * @param key Key.
         * @param hash Hash.
         * @return {@code True} if segment contains value.
         */
        boolean containsKey(Object key, int hash) {
            if (cnt != 0) { // read-volatile
                HashEntry<K, V> e = getFirst(hash);

                while (e != null) {
                    if (e.hash == hash && key.equals(e.key))
                        return true;

                    e = e.next;
                }
            }

            return false;
        }

//        boolean containsValue(Object val) {
//            if (cnt != 0) { // read-volatile
//                HashEntry<K, V>[] tab = table;
//
//                int len = tab.length;
//
//                for (int i = 0 ; i < len; i++) {
//                    for (HashEntry<K, V> e = tab[i]; e != null; e = e.next) {
//                        V v = e.value;
//
//                        if (v == null) // recheck
//                            v = readValueUnderLock(e);
//
//                        if (val.equals(v))
//                            return true;
//                    }
//                }
//            }
//
//            return false;
//        }

        /**
         * @param key Key.
         * @param hash Hash.
         * @param oldValue Old value.
         * @param newValue New value.
         * @return {@code True} if replaced.
         */
//        boolean replace(K key, int hash, V oldValue, V newValue) {
//            lock();
//
//            try {
//                HashEntry<K, V> e = getFirst(hash);
//
//                while (e != null && (e.hash != hash || !key.equals(e.key)))
//                    e = e.next;
//
//                boolean replaced = false;
//
//                if (e != null && oldValue.equals(e.value)) {
//                    replaced = true;
//
//                    e.value = newValue;
//                }
//
//                return replaced;
//            }
//            finally {
//                unlock();
//            }
//        }

        /**
         * @param key Key.
         * @param hash Hash.
         * @param newValue New value.
         * @return Previous value.
         */
//        @Nullable V replace(K key, int hash, V newValue) {
//            lock();
//
//            try {
//                HashEntry<K, V> e = getFirst(hash);
//
//                while (e != null && (e.hash != hash || !key.equals(e.key)))
//                    e = e.next;
//
//                V oldVal = null;
//
//                if (e != null) {
//                    oldVal = e.value;
//
//                    e.value = newValue;
//                }
//
//                return oldVal;
//            }
//            finally {
//                unlock();
//            }
//        }

//        /**
//         * Add a new entry with the specified key, value and hash code to
//         * the specified bucket.  It is the responsibility of this
//         * method to resize the table if appropriate.
//         *
//         * @param topVer Topology version
//         * @param hash Hash.
//         * @param key Key.
//         * @param val Value.
//         * @param ttl Time to live.
//         * @param idx Bucket index.
//         * @return Added entry.
//         */
//        private GridCacheMapEntry<K, V> addEntry(long topVer, int hash, K key, V val, int idx, long ttl) {
//            Bucket<K, V> b = table[idx];
//
//            GridCacheMapEntry<K, V> prev = b == null ? null : b.entry().value;
//
//
//
//            // Create bucket after creating entry, so we don't end up with empty
//            // bucket in case if there is exception.
//            if (b == null) {
//                b = new Bucket<K, V>();
//
//                table[idx] = b;
//
//                bits.set(idx);
//
//                bucketCnt++;
//            }
//
//            b.onAdd(e);
//
//            // Calculate only non internal entry.
//            if (!(key instanceof GridCacheInternal))
//                pubSize.getAndIncrement();
//
//            assert b.entry() != null;
//
//            if (size.getAndIncrement() >= threshold)
//                resize(2 * table.length);
//
//            return e;
//        }

        /**
         * @param key Key.
         * @param hash Hash.
         * @param val Value.
         * @param topVer Topology version.
         * @param ttl TTL.
         * @return Associated value.
         */
        @SuppressWarnings({"unchecked"})
        GridCacheMapEntry<K, V> put(Object key, int hash, Object val, long topVer, long ttl) {
            writeLock().lock();

            try {
                int c = cnt;

                if (c++ > threshold) // ensure capacity
                    rehash();

                Bucket<K, V>[] tab = table;

                int idx = hash & (tab.length - 1);

                Bucket<K, V> bucket = table[idx];

                if (bucket == null) {
                    table[idx] = bucket = new Bucket<K, V>();

                    bucketCnt.incrementAndGet();

                    bucketBits.set(idx);
                }

                HashEntry<K, V> first = bucket.entry();

                HashEntry<K, V> e = first;

                while (e != null && (e.hash != hash || !key.equals(e.key)))
                    e = e.next;

                GridCacheMapEntry<K, V> retVal;

                if (e != null) {
                    retVal = e.value;

                    e.value.rawPut((V)val, ttl);
                }
                else {
                    ++modCnt;

                    if (!(key instanceof GridCacheInternal))
                        pubSize.incrementAndGet();

                    size.incrementAndGet();

                    GridCacheMapEntry next = bucket.entry() != null ? bucket.entry().value() : null;

                    GridCacheMapEntry newEntry = create(topVer, key, hash, val, next, ttl);

                    retVal = newEntry;

                    HashEntry<K, V> newRoot = new HashEntry<K, V>((K)key, hash, first, newEntry);

                    bucket.onAdd(newRoot);

                    cnt = c; // write-volatile
                }

                segBits.set(segIdx);

                return retVal;
            }
            finally {
                writeLock().unlock();
            }
        }

        /**
         *
         */
        void rehash() {
            Bucket<K, V>[] oldTable = table;

            int oldCapacity = oldTable.length;

            if (oldCapacity >= MAXIMUM_CAPACITY)
                return;

            /*
             * Reclassify nodes in each list to new Map.  Because we are
             * using power-of-two expansion, the elements from each bin
             * must either stay at same index, or move with a power of two
             * offset. We eliminate unnecessary node creation by catching
             * cases where old nodes can be reused because their next
             * fields won't change. Statistically, at the default
             * threshold, only about one-sixth of them need cloning when
             * a table doubles. The nodes they replace will be eligible for GC
             * as soon as they are no longer referenced by any
             * reader thread that may be in the midst of traversing table
             * right now.
             */
            Bucket<K, V>[] newTable = Bucket.newArray(oldCapacity << 1);

            BitSet newBucketBits = new BitSet(newTable.length);

            threshold = (int)(newTable.length * loadFactor);

            int sizeMask = newTable.length - 1;

            for (int i = 0; i < oldCapacity ; i++) {
                // We need to guarantee that any existing reads of old Map can proceed.
                // So, we cannot yet null out each bin.
                Bucket<K, V> bucket = oldTable[i];

                if (bucket == null)
                    continue;

                bucketCnt.decrementAndGet();

                HashEntry<K, V> e = bucket.entry();

                assert e != null;

                HashEntry<K, V> next = e.next;

                int idx = e.hash & sizeMask;

                //  Single node on list
                if (next == null) {
                    newTable[idx] = bucket;

                    bucketCnt.incrementAndGet();

                    newBucketBits.set(idx);
                }
                else {
                    // Reuse trailing consecutive sequence at same slot.
                    HashEntry<K, V> lastRun = e;

                    int lastIdx = idx;

                    for (HashEntry<K, V> last = next; last != null; last = last.next) {
                        int k = last.hash & sizeMask;

                        if (k != lastIdx) {
                            lastIdx = k;
                            lastRun = last;
                        }
                    }

                    Bucket<K, V> b = new Bucket<K, V>(lastRun);

                    bucketCnt.incrementAndGet();

                    newTable[lastIdx] = b;

                    newBucketBits.set(lastIdx);

                    // Clone all remaining nodes.
                    for (HashEntry<K, V> p = e; p != lastRun; p = p.next) {
                        int k = p.hash & sizeMask;

                        Bucket<K, V> b1 = newTable[k];

                        if (b1 == null) {
                            newTable[k] = b1 = new Bucket<K, V>();

                            bucketCnt.incrementAndGet();

                            newBucketBits.set(k);
                        }

                        HashEntry<K, V> n = b1.entry();

                        HashEntry<K, V> newRoot = new HashEntry<K, V>(p.key, p.hash, n, p.value);

                        b1.onAdd(newRoot);
                    }
                }
            }

            table = newTable;

            bucketBits = newBucketBits;
        }

        /**
         * Remove; match on key only if value null, else match both.
         *
         * @param key Key.
         * @param hash Hash.
         * @return Removed value.
         */
        @Nullable GridCacheMapEntry<K, V> remove(Object key, int hash) {
            writeLock().lock();

            try {
                int c = cnt - 1;

                Bucket<K, V>[] tab = table;

                int idx = hash & (tab.length - 1);

                Bucket<K, V> bucket = tab[idx];

                if (bucket == null)
                    return null;

                HashEntry<K, V> first = bucket.entry();

                HashEntry<K, V> e = first;

                while (e != null && (e.hash() != hash || !key.equals(e.key())))
                    e = e.next;

                GridCacheMapEntry<K, V> oldValue = null;

                if (e != null) {
                    oldValue = e.value();

                    ++modCnt;

                    if (!(e.key instanceof GridCacheInternal))
                        pubSize.decrementAndGet();

                    size.decrementAndGet();

                    // All entries following removed node can stay in list,
                    // but all preceding ones need to be cloned.
                    HashEntry<K, V> newFirst = e.next;

                    for (HashEntry<K, V> p = first; p != e; p = p.next)
                        newFirst = new HashEntry<K, V>(p.key, p.hash, newFirst, p.value);

                    if (newFirst == null) {
                        table[idx] = null;

                        bucketCnt.decrementAndGet();

                        bucketBits.clear(idx);
                    }
                    else
                        tab[idx].onRemove(newFirst);

                    cnt = c; // write-volatile

                    if (cnt == 0)
                        segBits.clear(segIdx);
                }

                return oldValue;
            }
            finally {
                writeLock().unlock();
            }
        }

        /**
         * @return Entries count within segment.
         */
        int count() {
            return cnt;
        }

        /**
         * @return Random cache map entry from this segment.
         */
        @Nullable public GridCacheMapEntry<K, V> randomEntry() {
            readLock().lock();

            try {
                if (cnt == 0)
                    return null;

                int bucketIdx = bucketBits.nextSetBit(RAND.nextInt(table.length));

                if (bucketIdx == -1)
                    bucketIdx = bucketBits.nextSetBit(0);

                assert bucketIdx >= 0;

                Bucket<K, V> b = table[bucketIdx];

                assert b.count() > 0 : "Invalid entries count: " + b;

                int entryIdx = RAND.nextInt(b.count());

                HashEntry<K, V> e = b.entry();

                for (int i = 1; i < entryIdx; i++) {
                    assert e != null;

                    e = e.next();
                }

                assert e != null;

                GridCacheMapEntry<K, V> val = e.value();

                assert val != null;

                return val;
            }
            finally {
                readLock().unlock();
            }
        }
    }

    /**
     * Iterator over {@link GridCacheEntryEx} elements.
     *
     * @param <K> Key type.
     * @param <V> Value type.
     */
    private static class Iterator0<K, V> implements Iterator<GridCacheEntryEx<K, V>>, Externalizable {
        /** */
        private int nextSegmentIndex;

        /** */
        private int nextTableIndex;

        /** */
        private Bucket<K,V>[] currentTable;

        /** */
        private HashEntry<K, V> nextEntry;

        /** Next entry to return. */
        private GridCacheMapEntry<K, V> next;

        /** Next value. */
        private V nextVal;

        /** Current value. */
        private V curVal;

        /** */
        private boolean isVal;

        /** Current entry. */
        private GridCacheMapEntry<K, V> cur;

        /** Iterator filter. */
        private GridPredicate<? super GridCacheEntry<K, V>>[] filter;

        /** Outer cache map. */
        private GridCacheConcurrentMap<K, V> map;

        /** Cache context. */
        private GridCacheContext<K, V> ctx;

        /**
         * Empty constructor required for {@link Externalizable}.
         */
        public Iterator0() {
            // No-op.
        }

        /**
         * @param map Cache map.
         * @param isVal {@code True} if value iterator.
         * @param filter Entry filter.
         */
        @SuppressWarnings({"unchecked"})
        Iterator0(GridCacheConcurrentMap<K, V> map, boolean isVal,
            GridPredicate<? super GridCacheEntry<K, V>>[] filter) {
            this.filter = filter;
            this.isVal = isVal;

            this.map = map;

            ctx = map.ctx;

            nextSegmentIndex = map.segments.length - 1;
            nextTableIndex = -1;

            advance();
        }

        /**
         *
         */
        @SuppressWarnings({"unchecked"})
        private void advance() {
            if (nextEntry != null && advanceInBucket(nextEntry, true))
                return;

            while (nextTableIndex >= 0) {
                Bucket<K, V> bucket = currentTable[nextTableIndex--];

                if (bucket != null && advanceInBucket(bucket.entry(), false))
                    return;
            }

            while (nextSegmentIndex >= 0) {
                GridCacheConcurrentMap.Segment seg = map.segments[nextSegmentIndex--];

                if (seg.count() != 0) {
                    currentTable = seg.table;

                    for (int j = currentTable.length - 1; j >= 0; --j) {
                        Bucket<K, V> bucket = currentTable[j];

                        if (bucket != null && advanceInBucket(bucket.entry(), false)) {
                            nextTableIndex = j - 1;

                            return;
                        }
                    }
                }
            }
        }

        /**
         * @param e Current next.
         * @param skipFirst {@code True} to skip check on first iteration.
         * @return {@code True} if advance succeeded.
         */
        @SuppressWarnings( {"unchecked"})
        private boolean advanceInBucket(HashEntry<K, V> e, boolean skipFirst) {
            assert e != null;

            nextEntry = e;

            do {
                if (!skipFirst) {
                    next = nextEntry.value();

                    if (isVal) {
                        nextVal = next.wrap(true).peek(CU.<K, V>empty());

                        if (nextVal == null)
                            continue;
                    }

                    if (next.visitable(NON_INTERNAL_ARR) && next.visitable(filter))
                        return true;
                }

                // Perform checks in any case.
                skipFirst = false;
            }
            while ((nextEntry = nextEntry.next()) != null);

            assert nextEntry == null;

            next = null;
            nextVal = null;

            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return next != null && (!isVal || nextVal != null);
        }

        /**
         * @return Next value.
         */
        public V currentValue() {
            return curVal;
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"unchecked"})
        @Override public GridCacheEntryEx<K, V> next() {
            GridCacheMapEntry<K, V> e = next;
            V v = nextVal;

            if (e == null)
                throw new NoSuchElementException();

            advance();

            cur = e;
            curVal = v;

            return cur;
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            if (cur == null)
                throw new IllegalStateException();

            GridCacheMapEntry<K, V> e = cur;

            cur = null;
            curVal = null;

            try {
                ctx.cache().remove(e.key(), CU.<K, V>empty());
            }
            catch (GridException ex) {
                throw new GridRuntimeException(ex);
            }
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(ctx);
            out.writeObject(filter);
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"unchecked"})
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            ctx = (GridCacheContext<K, V>)in.readObject();
            filter = (GridPredicate<? super GridCacheEntry<K, V>>[])in.readObject();
        }

        /**
         * Reconstructs object on demarshalling.
         *
         * @return Reconstructed object.
         * @throws ObjectStreamException Thrown in case of demarshalling error.
         */
        protected Object readResolve() throws ObjectStreamException {
            return ctx.cache().map().entries0(filter).iterator();
        }
    }

    /**
     * Entry set.
     */
    @SuppressWarnings("unchecked")
    private static class Set0<K, V> extends AbstractSet<GridCacheEntryEx<K, V>> implements Externalizable {
        /** Filter. */
        private GridPredicate<? super GridCacheEntry<K, V>>[] filter;

        /** Base map. */
        private GridCacheConcurrentMap<K, V> map;

        /** Context. */
        private GridCacheContext<K, V> ctx;

        /** */
        private GridCacheProjectionImpl prjPerCall;

        /** */
        private GridCacheFlag[] forcedFlags;

        /** */
        private boolean clone;

        /**
         * Empty constructor required for {@link Externalizable}.
         */
        public Set0() {
            // No-op.
        }

        /**
         * @param map Base map.
         * @param filter Filter.
         */
        private Set0(GridCacheConcurrentMap<K, V> map, GridPredicate<? super GridCacheEntry<K, V>>[] filter) {
            assert map != null;

            this.map = map;
            this.filter = filter;

            ctx = map.ctx;

            prjPerCall = ctx.projectionPerCall();
            forcedFlags = ctx.forcedFlags();
            clone = ctx.hasFlag(CLONE);
        }

        /** {@inheritDoc} */
        @Override public boolean isEmpty() {
            return !iterator().hasNext();
        }

        /** {@inheritDoc} */
        @Override public Iterator<GridCacheEntryEx<K, V>> iterator() {
            return new Iterator0<K, V>(map, false, filter);
        }

        /**
         * @return Entry iterator.
         */
        Iterator<GridCacheEntry<K, V>> entryIterator() {
            return new EntryIterator<K, V>(map, filter, ctx, prjPerCall, forcedFlags);
        }

        /**
         * @return Key iterator.
         */
        Iterator<K> keyIterator() {
            return new KeyIterator<K, V>(map, filter);
        }

        /**
         * @return Value iterator.
         */
        Iterator<V> valueIterator() {
            return new ValueIterator<K, V>(map, filter, ctx, clone);
        }

        /**
         * Checks for key containment.
         *
         * @param k Key to check.
         * @return {@code True} if key is in the map.
         */
        boolean containsKey(K k) {
            GridCacheEntryEx<K, V> e = ctx.cache().peekEx(k);

            return e != null && !e.obsolete() && F.isAll(e.wrap(false), filter);
        }

        /**
         * @param v Checks if value is contained in
         * @return {@code True} if value is in the set.
         */
        boolean containsValue(V v) {
            A.notNull(v, "value");

            if (v == null)
                return false;

            for (Iterator<V> it = valueIterator(); it.hasNext(); ) {
                V v0 = it.next();

                if (F.eq(v0, v))
                    return true;
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean contains(Object o) {
            if (!(o instanceof GridCacheEntryEx))
                return false;

            GridCacheEntryEx<K, V> e = (GridCacheEntryEx<K, V>)o;

            GridCacheEntryEx<K, V> cur = ctx.cache().peekEx(e.key());

            return cur != null && cur.equals(e);
        }

        /** {@inheritDoc} */
        @Override public boolean remove(Object o) {
            return o instanceof GridCacheEntry && removeKey(((Map.Entry<K, V>)o).getKey());
        }

        /**
         * @param k Key to remove.
         * @return If key has been removed.
         */
        boolean removeKey(K k) {
            try {
                return ctx.cache().remove(k, CU.<K, V>empty()) != null;
            }
            catch (GridException e) {
                throw new GridRuntimeException("Failed to remove cache entry for key: " + k, e);
            }
        }

        /** {@inheritDoc} */
        @Override public int size() {
            return F.isEmpty(filter) ? map.publicSize() : F.size(iterator());
        }

        /** {@inheritDoc} */
        @Override public void clear() {
            ctx.cache().clearAll(new KeySet<K, V>(map, filter));
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(ctx);
            out.writeObject(filter);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            ctx = (GridCacheContext<K, V>)in.readObject();
            filter = (GridPredicate<? super GridCacheEntry<K, V>>[])in.readObject();

            // TODO: change return type.
            throw new UnsupportedOperationException("Change return type.");
            // map = ctx.cache().map();
        }
    }

    /**
     * Iterator over hash table.
     * <p>
     * Note, class is static for {@link Externalizable}.
     */
    private static class EntryIterator<K, V> implements Iterator<GridCacheEntry<K, V>>, Externalizable {
        /** Base iterator. */
        private Iterator0<K, V> it;

        /** */
        private GridCacheContext<K, V> ctx;

        /** */
        private GridCacheProjectionImpl<K, V> prjPerCall;

        /** */
        private GridCacheFlag[] forcedFlags;

        /**
         * Empty constructor required for {@link Externalizable}.
         */
        public EntryIterator() {
            // No-op.
        }

        /**
         * @param map Cache map.
         * @param filter Entry filter.
         * @param ctx Cache context.
         * @param prjPerCall Projection per call.
         * @param forcedFlags Forced flags.
         */
        EntryIterator(
            GridCacheConcurrentMap<K, V> map,
            GridPredicate<? super GridCacheEntry<K, V>>[] filter,
            GridCacheContext<K, V> ctx,
            GridCacheProjectionImpl<K, V> prjPerCall,
            GridCacheFlag[] forcedFlags) {
            it = new Iterator0<K, V>(map, false, filter);

            this.ctx = ctx;
            this.prjPerCall = prjPerCall;
            this.forcedFlags = forcedFlags;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return it.hasNext();
        }

        /** {@inheritDoc} */
        @Override public GridCacheEntry<K, V> next() {
            GridCacheProjectionImpl<K, V> oldPrj = ctx.projectionPerCall();

            ctx.projectionPerCall(prjPerCall);

            GridCacheFlag[] oldFlags = ctx.forceFlags(forcedFlags);

            try {
                return it.next().wrap(true);
            }
            finally {
                ctx.projectionPerCall(oldPrj);
                ctx.forceFlags(oldFlags);
            }
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            it.remove();
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(it);
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"unchecked"})
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            it = (Iterator0<K, V>)in.readObject();
        }
    }

    /**
     * Value iterator.
     * <p>
     * Note that class is static for {@link Externalizable}.
     */
    private static class ValueIterator<K, V> implements Iterator<V>, Externalizable {
        /** Hash table iterator. */
        private Iterator0<K, V> it;

        /** Context. */
        private GridCacheContext<K, V> ctx;

        /** */
        private boolean clone;

        /**
         * Empty constructor required for {@link Externalizable}.
         */
        public ValueIterator() {
            // No-op.
        }

        /**
         * @param map Base map.
         * @param filter Value filter.
         * @param ctx Cache context.
         * @param clone Clone flag.
         */
        private ValueIterator(
            GridCacheConcurrentMap<K, V> map,
            GridPredicate<? super GridCacheEntry<K, V>>[] filter,
            GridCacheContext<K, V> ctx,
            boolean clone) {
            it = new Iterator0<K, V>(map, true, filter);

            this.ctx = ctx;
            this.clone = clone;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return it.hasNext();
        }

        /** {@inheritDoc} */
        @Nullable @Override public V next() {
            it.next();

            // Cached value.
            V val = it.currentValue();

            try {
                return clone ? ctx.cloneValue(val) : val;
            }
            catch (GridException e) {
                throw new GridRuntimeException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            it.remove();
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(it);
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"unchecked"})
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            it = (Iterator0)in.readObject();
        }
    }

    /**
     * Key iterator.
     */
    private static class KeyIterator<K, V> implements Iterator<K>, Externalizable {
        /** Hash table iterator. */
        private Iterator0<K, V> it;

        /**
         * Empty constructor required for {@link Externalizable}.
         */
        public KeyIterator() {
            // No-op.
        }

        /**
         * @param map Cache map.
         * @param filter Filter.
         */
        private KeyIterator(GridCacheConcurrentMap<K, V> map, GridPredicate<? super GridCacheEntry<K, V>>[] filter) {
            it = new Iterator0<K, V>(map, false, filter);
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return it.hasNext();
        }

        /** {@inheritDoc} */
        @Override public K next() {
            return it.next().key();
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            it.remove();
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(it);
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"unchecked"})
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            it = (Iterator0)in.readObject();
        }
    }

    /**
     * Key set.
     */
    private static class KeySet<K, V> extends AbstractSet<K> implements Externalizable {
        /** Base entry set. */
        private Set0<K, V> set;

        /**
         * Empty constructor required for {@link Externalizable}.
         */
        public KeySet() {
            // No-op.
        }

        /**
         * @param map Base map.
         * @param filter Key filter.
         */
        private KeySet(GridCacheConcurrentMap<K, V> map, GridPredicate<? super GridCacheEntry<K, V>>[] filter) {
            assert map != null;

            set = new Set0<K, V>(map, filter);
        }

        /** {@inheritDoc} */
        @Override public Iterator<K> iterator() {
            return set.keyIterator();
        }

        /** {@inheritDoc} */
        @Override public int size() {
            return set.size();
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"unchecked"})
        @Override public boolean contains(Object o) {
            return set.containsKey((K)o);
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"unchecked"})
        @Override public boolean remove(Object o) {
            return set.removeKey((K)o);
        }

        /** {@inheritDoc} */
        @Override public void clear() {
            set.clear();
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(set);
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"unchecked"})
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            set = (Set0<K, V>)in.readObject();
        }
    }

    /**
     * Value set.
     * <p>
     * Note that the set is static for {@link Externalizable} support.
     */
    private static class Values<K, V> extends AbstractCollection<V> implements Externalizable {
        /** Base entry set. */
        private Set0<K, V> set;

        /**
         * Empty constructor required for {@link Externalizable}.
         */
        public Values() {
            // No-op.
        }

        /**
         * @param map Base map.
         * @param filter Value filter.
         */
        private Values(GridCacheConcurrentMap<K, V> map, GridPredicate<? super GridCacheEntry<K, V>>[] filter) {
            assert map != null;

            set = new Set0<K, V>(map, filter);
        }

        /** {@inheritDoc} */
        @Override public Iterator<V> iterator() {
            return set.valueIterator();
        }

        /** {@inheritDoc} */
        @Override public int size() {
            return set.size();
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"unchecked"})
        @Override public boolean contains(Object o) {
            return set.containsValue((V)o);
        }

        /** {@inheritDoc} */
        @Override public void clear() {
            set.clear();
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(set);
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"unchecked"})
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            set = (Set0<K, V>)in.readObject();
        }
    }
    /**
     * Entry set.
     */
    private static class EntrySet<K, V> extends AbstractSet<GridCacheEntry<K, V>> implements Externalizable {
        /** Base entry set. */
        private Set0<K, V> set;

        /**
         * Empty constructor required for {@link Externalizable}.
         */
        public EntrySet() {
            // No-op.
        }

        /**
         * @param map Base map.
         * @param filter Key filter.
         */
        private EntrySet(GridCacheConcurrentMap<K, V> map, GridPredicate<? super GridCacheEntry<K, V>>[] filter) {
            assert map != null;

            set = new Set0<K, V>(map, filter);
        }

        /** {@inheritDoc} */
        @Override public Iterator<GridCacheEntry<K, V>> iterator() {
            return set.entryIterator();
        }

        /** {@inheritDoc} */
        @Override public int size() {
            return set.size();
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"unchecked"})
        @Override public boolean contains(Object o) {
            return o instanceof GridCacheEntryImpl && set.contains(((GridCacheEntryImpl<K, V>)o).unwrap());
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"unchecked"})
        @Override public boolean remove(Object o) {
            return set.removeKey((K)o);
        }

        /** {@inheritDoc} */
        @Override public void clear() {
            set.clear();
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(set);
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"unchecked"})
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            set = (Set0<K, V>)in.readObject();
        }
    }
}
