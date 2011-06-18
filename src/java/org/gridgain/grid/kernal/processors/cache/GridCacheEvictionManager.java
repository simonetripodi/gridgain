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
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.cache.eviction.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.kernal.processors.timeout.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.lang.utils.*;
import org.gridgain.grid.typedef.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.GridEventType.EVT_CACHE_ENTRY_EVICTED;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.lang.utils.GridConcurrentLinkedQueue.*;

/**
 * Cache eviction manager.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.1.1c.17062011
 */
public class GridCacheEvictionManager<K, V> extends GridCacheManager<K, V> {
    /** */
    private static final int EVICT_QUEUE_MAX_EDEN_SIZE = 50;

    /** Eviction policy. */
    private GridCacheEvictionPolicy<K, V> policy;

    /** Transaction queue. */
    private final ConcurrentLinkedQueue<GridCacheTxEx<K, V>> txs = new ConcurrentLinkedQueue<GridCacheTxEx<K, V>>();

    /** Unlock queue. */
    private final ConcurrentLinkedQueue<GridCacheEntryEx<K, V>> entries =
        new ConcurrentLinkedQueue<GridCacheEntryEx<K, V>>();

    /** Unwinding flag to make sure that only one thread unwinds. */
    private final AtomicBoolean unwinding = new AtomicBoolean(false);

    /** Eviction queue. */
    private final GridConcurrentLinkedQueue<EvictionInfo> evictQ = new GridConcurrentLinkedQueue<EvictionInfo>();

    /** Attribute name used to queue node in entry metadata. */
    private final String meta = UUID.randomUUID().toString();

    /** Evicting flag to make sure that only one thread processes eviction queue. */
    private final AtomicBoolean evicting = new AtomicBoolean(false);

    /** Active eviction futures. */
    private final Map<Long, EvictionFuture> futs = new ConcurrentHashMap<Long, EvictionFuture>();

    /** Generator of future ids. */
    private final AtomicLong idGen = new AtomicLong();

    /** Busy lock. */
    private final GridBusyLock busyLock = new GridBusyLock();

    /** {@inheritDoc} */
    @Override public void start0() throws GridException {
        GridCacheConfigurationAdapter cfg = cctx.config();

        policy = cctx.isNear() ? cfg.<K, V>getNearEvictionPolicy() : cfg.<K, V>getEvictionPolicy();

        assert policy != null;

        if (cfg.getMaxEvictionOverflowRatio() < 0)
            throw new GridException("Configuration parameter 'maxEvictionOverflowRatio' cannot be negative.");

        if (cfg.getCacheMode() != LOCAL && !cctx.isNear() && cfg.isEvictSynchronized()) {
            cctx.io().addHandler(GridCacheEvictionRequest.class, new CI2<UUID, GridCacheEvictionRequest<K, V>>() {
                @Override public void apply(UUID nodeId, GridCacheEvictionRequest<K, V> msg) {
                    processEvictionRequest(nodeId, msg);
                }
            });

            cctx.io().addHandler(GridCacheEvictionResponse.class, new CI2<UUID, GridCacheEvictionResponse<K, V>>() {
                @Override public void apply(UUID nodeId, GridCacheEvictionResponse<K, V> msg) {
                    processEvictionResponse(nodeId, msg);
                }
            });
        }

        if (log.isDebugEnabled())
            log.debug("Eviction manager started on node: " + cctx.nodeId());
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel, boolean wait) {
        super.stop0(cancel, wait);

        busyLock.stop();

        if (log.isDebugEnabled())
            log.debug("Eviction manager stopped on node: " + cctx.nodeId());
    }

    /**
     * @param nodeId Sender node id.
     * @param res Response.
     */
    private void processEvictionResponse(UUID nodeId, GridCacheEvictionResponse<K, V> res) {
        assert nodeId != null;
        assert res != null;

        if (log.isDebugEnabled())
            log.debug("Processing eviction response [node=" + nodeId + ", localNode=" + cctx.nodeId() +
                ", res=" + res + ']');

        EvictionFuture fut = futs.get(res.futureId());

        if (fut != null)
            fut.onResponse(nodeId, res);
    }

    /**
     * @param nodeId Sender node id.
     * @param req Request.
     */
    private void processEvictionRequest(final UUID nodeId, final GridCacheEvictionRequest<K, V> req) {
        assert nodeId != null;
        assert req != null;

        if (log.isDebugEnabled())
            log.debug("Processing eviction request [node=" + nodeId + ", localNode=" + cctx.nodeId() +
                ", req=" + req + ']');

        // Run processing in a dedicated thread since it may take significant time.
        cctx.closures().runLocalSafe(new GPR() {
            @Override public void run() {
                if (!busyLock.enterBusy())
                    return;

                try {
                    final GridCacheEvictionResponse<K, V> res = new GridCacheEvictionResponse<K, V>(req.futureId());

                    final GridCompoundIdentityFuture<GridTuple2<K, Boolean>> compFut =
                        new GridCompoundIdentityFuture<GridTuple2<K, Boolean>>(cctx.kernalContext());

                    GridCacheVersion obsoleteVer = cctx.versions().next();

                    for (Map.Entry<K, GridTuple2<GridCacheVersion, Boolean>> e : req.keys().entrySet()) {
                        GridFuture<GridTuple2<K, Boolean>> fut =
                            evictLocally(e.getKey(),
                                e.getValue().get1() /* version */,
                                e.getValue().get2() /* near cache or not */,
                                obsoleteVer);

                        compFut.add(fut);
                    }

                    compFut.markInitialized();

                    compFut.listenAsync(new CI1<GridFuture<GridTuple2<K, Boolean>>>() {
                        @Override public void apply(GridFuture<GridTuple2<K, Boolean>> f) {
                            if (!busyLock.enterBusy())
                                return;

                            try {
                                try {
                                    // Check if the future completed successfully.
                                    f.get();

                                    for (GridFuture<GridTuple2<K, Boolean>> fut : compFut.futures()) {
                                        GridTuple2<K, Boolean> t = fut.get();

                                        if (!t.get2())
                                            res.addRejected(t.get1());
                                    }
                                }
                                catch (GridException e) {
                                    log.error("Failed to evict keys from eviction request (all will be rejected) [req=" + req +
                                        ", localNode=" + cctx.nodeId() + ']', e);

                                    for (K key : req.keys().keySet())
                                        res.addRejected(key);
                                }

                                if (log.isDebugEnabled())
                                    log.debug("Sending eviction response [res=" + res + ", node=" + nodeId +
                                        ", localNode=" + cctx.nodeId() + ']');

                                // Unwind event queue explicitly to make sure
                                // to fire eviction events.
                                cctx.events().unwind();

                                try {
                                    cctx.io().send(nodeId, res);
                                }
                                catch (GridTopologyException ignored) {
                                    if (log.isDebugEnabled())
                                        log.debug("Failed to send eviction response since initiating node left grid [node=" +
                                            nodeId + ", localNode=" + cctx.nodeId() + ']');
                                }
                                catch (GridException e) {
                                    log.error("Failed to send eviction response to node [node=" + nodeId +
                                        ", res=" + res + ", localNode=" + cctx.nodeId() + ']', e);
                                }
                            }
                            finally {
                                busyLock.leaveBusy();
                            }
                        }
                    });
                }
                finally {
                    busyLock.leaveBusy();
                }
            }
        }, true);
    }

    /**
     * @param key Key to evict.
     * @param ver Entry version on initial node.
     * @param near {@code true} if entry should be evicted from near cache.
     * @param obsoleteVer Obsolete version.
     * @return {@code true} if evicted successfully, {@code false} if could not be evicted.
     */
    @SuppressWarnings({"TypeMayBeWeakened", "unchecked"})
    private GridFuture<GridTuple2<K, Boolean>> evictLocally(final K key, final GridCacheVersion ver, boolean near,
        final GridCacheVersion obsoleteVer) {
        assert key != null;
        assert ver != null;
        assert obsoleteVer != null;

        if (log.isDebugEnabled())
            log.debug("Evicting key locally [key=" + key + ", ver=" + ver + ", obsoleteVer=" + obsoleteVer +
                ", localNode=" + cctx.localNode() + ']');

        GridKernalContext ctx = cctx.kernalContext();

        GridCacheAdapter<K, V> cache = near ? cctx.dht().near() : cctx.cache();

        final GridCacheEntryEx<K, V> entry = cache.peekEx(key);

        if (entry == null)
            return new GridFinishedFuture(ctx, F.t(key, true));

        try {
            // If entry should be evicted from near cache it can be done safely
            // without any consistency risks. We don't use filter in this case.
            if (near)
                return new GridFinishedFuture(ctx, F.t(key, evict0(entry, obsoleteVer, null)));

            // Create filter that will not evict entry if its version changes after we get it.
            final GridPredicate<? super GridCacheEntry<K, V>>[] filter =
                cctx.vararg(new P1<GridCacheEntry<K, V>>() {
                    @Override public boolean apply(GridCacheEntry<K, V> e) {
                        GridCacheVersion v = (GridCacheVersion)e.version();

                        return ver.compareTo(v) >= 0;
                    }
                });

            GridCacheVersion v = entry.version();

            if (ver.compareTo(v) == 0) {
                return new GridFinishedFuture(ctx, F.t(key, evict0(entry, obsoleteVer, filter)));
            }
            else if (ver.compareTo(v) < 0) {
                // Received version is less than entry local version.
                // Cannot evict in this case.
                return new GridFinishedFuture(ctx, F.t(key, false));
            }
            else {
                // If the received version is greater than local then try to wait
                // till all locks are released if there are no versions in entry
                // mvcc list greater than received. This block is needed to have
                // more chances to evict entry.
                Collection<GridCacheMvccCandidate<K>> cands =
                    F.concat(true, entry.localCandidates(ver), entry.remoteMvccSnapshot(ver));

                if (!F.isEmpty(cands)) {
                    boolean foundGreater = false;

                    for (GridCacheMvccCandidate<K> cand : cands)
                        if (ver.compareTo(cand.version()) < 0) {
                            foundGreater = true;

                            break;
                        }

                    if (foundGreater)
                        return new GridFinishedFuture(ctx, F.t(key, false));

                    GridFuture fut = cctx.mvcc().finishKeys(new GridPredicate[]{
                        new P1<K>() {
                            @Override public boolean apply(K k) {
                                return key.equals(k);
                            }
                        }});

                    return new GridEmbeddedFuture<GridTuple2<K, Boolean>, Object>(
                        ctx, fut,
                        new CX2<Object, Exception, GridTuple2<K, Boolean>>() {
                            @Override public GridTuple2<K, Boolean> applyx(Object o, Exception e) throws GridException {
                                if (e != null)
                                    return F.t(key, false);

                                return F.t(key, evict0(entry, obsoleteVer, filter));
                            }
                        }
                    );
                }

                return new GridFinishedFuture(ctx, F.t(key, false));
            }
        }
        catch (GridCacheEntryRemovedException ignored) {
            // Entry was concurrently removed.
            return new GridFinishedFuture(ctx, F.t(key, true));
        }
        catch (GridException e) {
            log.error("Failed to evict entry on remote node [key=" + key + ", localNode=" + cctx.nodeId() + ']', e);

            return new GridFinishedFuture(ctx, F.t(key, false));
        }
    }

    /**
     * @param entry Entry to evict.
     * @param obsoleteVer Obsolete version.
     * @param filter Filter.
     * @return {@code true} if entry has been evicted.
     * @throws GridException If failed to evict entry.
     */
    private boolean evict0(GridCacheEntryEx<K, V> entry, GridCacheVersion obsoleteVer,
        @Nullable GridPredicate<? super GridCacheEntry<K, V>>[] filter) throws GridException {
        boolean evicted = entry.evictInternal(cctx.isSwapEnabled(), obsoleteVer, filter);

        if (evicted) {
            cctx.cache().removeEntry(entry);

            cctx.events().addEvent(entry.partition(), entry.key(), cctx.nodeId(), (UUID)null, null,
                EVT_CACHE_ENTRY_EVICTED, null, null);

            if (log.isDebugEnabled())
                log.debug("Entry got evicted [entry=" + entry + ", localNode=" + cctx.nodeId() + ']');
        }
        else if (log.isDebugEnabled())
            log.debug("Entry didn't get evicted [entry=" + entry + ", localNode=" + cctx.nodeId() + ']');

        return evicted;
    }

    /**
     * @param tx Transaction to register for eviction policy notifications.
     */
    public void touch(GridCacheTxEx<K, V> tx) {
        if (log.isDebugEnabled())
            log.debug("Touching transaction [tx=" + CU.txString(tx) + ", localNode=" + cctx.nodeId() + ']');

        txs.add(tx);

        for (GridCacheTxEntry<K, V> e : F.concat(false, tx.readEntries(), tx.writeEntries())) {
            Node<EvictionInfo> node = e.cached().removeMeta(meta);

            if (node != null)
                evictQ.clearNode(node);

            for (EvictionFuture fut : futs.values())
                fut.rejectEntry(e.cached());
        }

        evictQ.gc(EVICT_QUEUE_MAX_EDEN_SIZE);
    }

    /**
     * @param entry Entry for eviction policy notification.
     */
    public void touch(GridCacheEntryEx<K, V> entry) {
        if (log.isDebugEnabled())
            log.debug("Touching entry [entry=" + entry + ", localNode=" + cctx.nodeId() + ']');

        entries.add(entry);

        Node<EvictionInfo> node = entry.removeMeta(meta);

        if (node != null)
            evictQ.clearNode(node);

        for (EvictionFuture fut : futs.values())
            fut.rejectEntry(entry);

        evictQ.gc(EVICT_QUEUE_MAX_EDEN_SIZE);
    }

    /**
     * @param entry Entry to attempt to evict.
     * @param obsoleteVer Obsolete version.
     * @param filter Optional entry filter.
     * @return {@code True} if entry was marked for eviction.
     * @throws GridException In case of error.
     */
    public boolean evict(@Nullable GridCacheEntryEx<K, V> entry, GridCacheVersion obsoleteVer,
        @Nullable GridPredicate<? super GridCacheEntry<K, V>>[] filter) throws GridException {
        if (entry == null)
            return true;

        // Entry cannot be evicted if entry contains GridCacheInternal key.
        if (entry.key() instanceof GridCacheInternal)
            return false;

        if (!cctx.isSwapEnabled())
            // Entry cannot be evicted on backup node if swap is disabled.
            if (!entry.wrap(false).primary())
                return false;

        if (cctx.config().getCacheMode() == LOCAL || cctx.isNear() ||
            !cctx.config().isEvictSynchronized() || cctx.isSwapEnabled()) {
            return evict0(entry, obsoleteVer, filter);
        }
        else {
            try {
                if (!cctx.isAll(entry, filter))
                    return false;

                if (log.isDebugEnabled())
                    log.debug("Since swap is disabled and eviction is synchronized, it will take longer [entry=" +
                        entry + ']');

                // Add entry to eviction queue.
                enqueue(entry, filter);
            }
            catch (GridCacheEntryRemovedException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Entry got removed while evicting [entry=" + entry +
                        ", localNode=" + cctx.nodeId() + ']');
            }
            finally {
                checkEvictionQueue();
            }
        }

        return true;
    }

    /**
     * @param entry Entry.
     * @param filter Filter.
     * @throws GridCacheEntryRemovedException If entry got removed.
     */
    private void enqueue(GridCacheEntryEx<K, V> entry, GridPredicate<? super GridCacheEntry<K, V>>[] filter)
        throws GridCacheEntryRemovedException {
        Node<EvictionInfo> node = entry.meta(meta);

        if (node == null) {
            node = evictQ.addNode(new EvictionInfo(entry, entry.version(), filter));

            if (entry.putMetaIfAbsent(meta, node) != null)
                // Was concurrently added, need to clear it from queue.
                evictQ.clearNode(node);
        }
    }

    /**
     * Checks eviction queue.
     */
    private void checkEvictionQueue() {
        if (isQueueFull()) {
            if (evicting.compareAndSet(false, true)) {
                try {
                    processEvictionQueue();
                }
                finally {
                    evicting.set(false);
                }
            }
        }
    }

    /**
     * @return {@code true} if queue has reached maximum allowed size
     *      and must be shrinked.
     */
    private boolean isQueueFull() {
        return evictQ.size() >=
            (int)(cctx.cache().size() * cctx.config().getMaxEvictionOverflowRatio()) / 100;
    }

    /**
     * Processes eviction queue (sends required requests, etc.).
     */
    private void processEvictionQueue() {
        if (log.isDebugEnabled())
            log.debug("Processing eviction queue on node: " + cctx.nodeId());

        final EvictionFuture fut = new EvictionFuture();

        fut.prepare();

        // Force future completion on elapsing network timeout.
        cctx.time().addTimeoutObject(new GridTimeoutObject() {
            private UUID id = UUID.randomUUID();
            private long endTime = System.currentTimeMillis() + cctx.gridConfig().getNetworkTimeout();

            @Override public UUID timeoutId() {
                return id;
            }

            @Override public long endTime() {
                return endTime;
            }

            @Override public void onTimeout() {
                fut.complete();
            }
        });

        // Listen to the future completion.
        fut.listenAsync(new CI1<GridFuture<?>>() {
            @Override public void apply(GridFuture<?> f) {
                EvictionFuture fut = (EvictionFuture)f;

                GridTuple2<Collection<EvictionInfo>, Collection<EvictionInfo>> t;

                try {
                    t = fut.get();
                }
                catch (GridException e) {
                    log.error("Eviction future finished with error (all entries will be touched): " + fut, e);

                    for (EvictionInfo info : fut.entries())
                        touch(info.entry());

                    return;
                }

                // Evict remotely evicted entries.
                GridCacheVersion obsoleteVer = cctx.versions().next();

                Collection<EvictionInfo> evictedEntries = t.get1();

                for (final EvictionInfo info : evictedEntries) {
                    GridCacheEntryEx<K, V> entry = info.entry();

                    try {
                        // Remove readers on which the entry was evicted.
                        for (GridTuple2<GridRichNode, Long> r : fut.evictedReaders(entry.key()))
                            ((GridDhtCacheEntry<K, V>)entry).removeReader(
                                r.get1().id() /* reader node id */,
                                r.get2() /* eviction response id */);

                        // If version has changed since we started the whole process
                        // then we should not evict entry.
                        GridPredicate<? super GridCacheEntry<K, V>>[] filter =
                            cctx.vararg(new P1<GridCacheEntry<K, V>>() {
                                @Override public boolean apply(GridCacheEntry<K, V> e) {
                                    GridCacheVersion ver = (GridCacheVersion)e.version();

                                    return info.version().equals(ver) && F.isAll(info.filter());
                                }
                            });

                        evict0(entry, obsoleteVer, filter);
                    }
                    catch (GridException e) {
                        log.error("Failed to evict entry [entry=" + entry +
                            ", localNode=" + cctx.nodeId() + ']', e);
                    }
                    catch (GridCacheEntryRemovedException ignored) {
                        if (log.isDebugEnabled())
                            log.debug("Entry was concurrently removed while evicting [entry=" + entry +
                                ", localNode=" + cctx.nodeId() + ']');
                    }
                }

                // Touch rejected entries.
                Collection<EvictionInfo> rejectedEntries = t.get2();

                for (EvictionInfo info : rejectedEntries)
                    touch(info.entry());
            }
        });
    }

    /**
     * Gets a collection of nodes to sent eviction requests to.
     *
     * @param entry Entry.
     * @return Tuple of two collections: dht (in case of partitioned cache) nodes
     *      and readers (empty for replicated cache).
     * @throws GridCacheEntryRemovedException If entry got removed during method
     *      execution.
     */
    private GridTuple2<Collection<GridRichNode>, Collection<GridRichNode>> remoteNodes(GridCacheEntryEx<K, V> entry)
        throws GridCacheEntryRemovedException {
        assert entry != null;

        GridCacheAffinity<Object> aff = cctx.config().getAffinity();

        if (cctx.config().getCacheMode() == REPLICATED) {
            Collection<GridRichNode> nodes =
                new HashSet<GridRichNode>(aff.nodes(cctx.partition(entry.key()), CU.allNodes(cctx)));

            nodes.remove(cctx.localNode());

            return new GridPair<Collection<GridRichNode>>(nodes, Collections.<GridRichNode>emptySet());
        }
        else {
            assert cctx.config().getCacheMode() == PARTITIONED;

            Collection<GridRichNode> nodes =
                F.transform(cctx.dht().topology().nodes(cctx.partition(entry.key())), cctx.rich().richNode());

            // Exclude local node.
            nodes.remove(cctx.localNode());

            return new GridPair<Collection<GridRichNode>>(nodes,
                F.transform(((GridDhtCacheEntry<K, V>)entry).readers(), new C1<UUID, GridRichNode>() {
                    @Override public GridRichNode apply(UUID nodeId) {
                        return cctx.node(nodeId);
                    }
                }));
        }
    }

    /**
     * Notifications.
     */
    public void unwind() {
        // Only one thread should unwind for efficiency.
        if (unwinding.compareAndSet(false, true)) {
            GridCacheFlag[] old = cctx.forceLocal();

            try {
                // Touch first.
                for (GridCacheEntryEx<K, V> e = entries.poll(); e != null; e = entries.poll())
                    // Internal entry can't be checked in policy.
                    if (!(e.key() instanceof GridCacheInternal))
                        policy.onEntryAccessed(e.obsolete(), e.wrap(false));

                for (Iterator<GridCacheTxEx<K, V>> it = txs.iterator(); it.hasNext(); ) {
                    GridCacheTxEx<K, V> tx = it.next();

                    if (!tx.done())
                        return;

                    it.remove();

                    if (!tx.internal()) {
                        notify(tx.readEntries());
                        notify(tx.writeEntries());
                    }
                }
            }
            finally {
                unwinding.set(false);

                // This call will clear memory for tx queue.
                txs.peek();

                cctx.forceFlags(old);
            }
        }
    }

    /**
     * @param entries Transaction entries for eviction notifications.
     */
    private void notify(Iterable<GridCacheTxEntry<K, V>> entries) {
        for (GridCacheTxEntry<K, V> txe : entries) {
            GridCacheEntryEx<K, V> e = txe.cached();

            // Internal entry can't be checked in policy.
            if (!(e.key() instanceof GridCacheInternal))
                policy.onEntryAccessed(e.obsolete(), e.wrap(false));
        }
    }

    /**
     * Wrapper around an entry to be put into queue.
     */
    private class EvictionInfo {
        /** Cache entry. */
        private GridCacheEntryEx<K, V> entry;

        /** Start version. */
        private GridCacheVersion ver;

        /** Filter to pass before entry will be evicted. */
        private GridPredicate<? super GridCacheEntry<K, V>>[] filter;

        /**
         * @param entry Entry.
         * @param ver Version.
         * @param filter Filter.
         */
        EvictionInfo(GridCacheEntryEx<K, V> entry, GridCacheVersion ver,
            GridPredicate<? super GridCacheEntry<K, V>>[] filter) {
            assert entry != null;
            assert ver != null;

            this.entry = entry;
            this.ver = ver;
            this.filter = filter;
        }

        /**
         * @return Entry.
         */
        GridCacheEntryEx<K, V> entry() {
            return entry;
        }

        /**
         * @return Version.
         */
        GridCacheVersion version() {
            return ver;
        }

        /**
         * @return Filter.
         */
        GridPredicate<? super GridCacheEntry<K, V>>[] filter() {
            return filter;
        }
    }

    /**
     * Future for synchronized eviction. Result is a tuple: {evicted entries, rejected entries}.
     */
    private class EvictionFuture
        extends GridFutureAdapter<GridTuple2<Collection<EvictionInfo>, Collection<EvictionInfo>>> {
        /** */
        private long id = idGen.incrementAndGet();

        /** */
        @GridToStringInclude
        private ConcurrentMap<K, EvictionInfo> entries;

        /** */
        @GridToStringInclude
        private final ConcurrentMap<K, Collection<GridRichNode>> readers =
            new ConcurrentHashMap<K, Collection<GridRichNode>>();

        /** */
        @GridToStringInclude
        private final Collection<EvictionInfo> evictedEntries = new GridConcurrentHashSet<EvictionInfo>();

        /** */
        @GridToStringInclude
        private final ConcurrentMap<K, EvictionInfo> rejectedEntries = new ConcurrentHashMap<K, EvictionInfo>();

        /** Request map. */
        @GridToStringInclude
        private ConcurrentMap<UUID, GridCacheEvictionRequest<K, V>> reqMap =
            new ConcurrentHashMap<UUID, GridCacheEvictionRequest<K, V>>();

        /** Response map. */
        @GridToStringInclude
        private ConcurrentMap<UUID, GridCacheEvictionResponse<K, V>> resMap =
            new ConcurrentHashMap<UUID, GridCacheEvictionResponse<K, V>>();

        /** To make sure that future is completing within a single thread. */
        private AtomicBoolean completing = new AtomicBoolean(false);

        /**
         * Default constructor.
         */
        public EvictionFuture() {
            super(cctx.kernalContext());
        }

        /**
         * Prepares future (sends all required requests).
         */
        @SuppressWarnings({"MismatchedQueryAndUpdateOfCollection"}) void prepare() {
            if (log.isDebugEnabled())
                log.debug("Preparing eviction future [futId=" + id + ", localNode=" + cctx.nodeId() + ']');

            futs.put(id, this);

            entries = new ConcurrentHashMap<K, EvictionInfo>();

            Collection<EvictionInfo> locals = new HashSet<EvictionInfo>();

            for (EvictionInfo info = evictQ.poll(); info != null; info = evictQ.poll()) {
                if (log.isDebugEnabled())
                    log.debug("Polled entry from eviction queue: " + info);

                // Queue node may have been stored in entry metadata concurrently
                // but we don't care about it since we are already processing this
                // entry.
                Node<EvictionInfo> queueNode = info.entry().removeMeta(meta);

                if (queueNode != null)
                    evictQ.clearNode(queueNode);

                GridTuple2<Collection<GridRichNode>, Collection<GridRichNode>> tup;

                try {
                    tup = remoteNodes(info.entry());
                }
                catch (GridCacheEntryRemovedException ignored) {
                    if (log.isDebugEnabled())
                        log().debug("Entry got removed while preparing eviction future (will be ignored) [entry=" +
                            info.entry() + ", nodeId=" + cctx.nodeId() + ']');

                    // Entry has already been remove, just skip it.
                    continue;
                }

                Collection<GridRichNode> entryReaders =
                    F.addIfAbsent(readers, info.entry().key(), new CO<Collection<GridRichNode>>() {
                        @Override public Collection<GridRichNode> apply() {
                            return new GridConcurrentHashSet<GridRichNode>();
                        }
                    });

                // Add entry readers so that we could remove them right before local eviction.
                entryReaders.addAll(tup.get2());

                Collection<GridRichNode> nodes = F.concat(true, tup.get1(), tup.get2());

                if (!nodes.isEmpty()) {
                    entries.put(info.entry().key(), info);

                    // There are remote participants.
                    for (GridRichNode node : nodes) {
                        GridCacheEvictionRequest<K, V> req = F.addIfAbsent(reqMap, node.id(),
                            new CO<GridCacheEvictionRequest<K, V>>() {
                                @Override public GridCacheEvictionRequest<K, V> apply() {
                                    return new GridCacheEvictionRequest<K, V>(id);
                                }
                            });

                        assert req != null;

                        req.addKey(info.entry().key(), info.version(), entryReaders.contains(node));
                    }
                }
                else
                    // There are no remote participants, need to keep the entry as local.
                    locals.add(info);
            }

            // Evict entries without remote participant nodes immediately.
            GridCacheVersion obsoleteVer = cctx.versions().next();

            for (EvictionInfo info : locals) {
                if (log.isDebugEnabled())
                    log.debug("Evicting key without remote participant nodes: " + info);

                try {
                    evict0(info.entry(), obsoleteVer, info.filter());
                }
                catch (GridException e) {
                    log.error("Failed to evict entry: " + info.entry(), e);
                }
            }

            // Send eviction requests.
            for (Map.Entry<UUID, GridCacheEvictionRequest<K, V>> e : reqMap.entrySet()) {
                UUID nodeId = e.getKey();

                GridCacheEvictionRequest<K, V> req = e.getValue();

                if (log.isDebugEnabled())
                    log.debug("Sending eviction request [node=" + nodeId + ", req=" + req + ']');

                try {
                    cctx.io().send(nodeId, req);
                }
                catch (GridTopologyException ignored) {
                    // Node left the topology.
                    onNodeLeft(nodeId);
                }
                catch (GridException ex) {
                    log.error("Failed to send eviction request to node [node=" + nodeId +
                        ", req=" + req + ']', ex);

                    rejectEntries(nodeId);
                }
            }
        }

        /**
         * @return Keys to readers mapping.
         */
        Map<K, Collection<GridRichNode>> readers() {
            return readers;
        }

        /**
         * @return All entries associated with future that should be evicted (or rejected).
         */
        Collection<EvictionInfo> entries() {
            return entries.values();
        }

        /**
         * Reject all entries on behalf of specified node.
         *
         * @param nodeId Node id.
         */
        void rejectEntries(UUID nodeId) {
            assert nodeId != null;

            if (log.isDebugEnabled())
                log.debug("Rejecting entries for node: " + nodeId);

            GridCacheEvictionRequest<K, V> req = reqMap.remove(nodeId);

            for (K k : req.keys().keySet()) {
                EvictionInfo info = entries.get(k);

                assert info != null;

                rejectedEntries.put(k, info);
            }

            checkDone();
        }

        /**
         * @param entry Entry to reject from being evicted.
         */
        public void rejectEntry(GridCacheEntryEx<K, V> entry) {
            assert entry != null;

            if (log.isDebugEnabled())
                log.debug("Rejecting entry: " + entry);

            EvictionInfo info = entries.get(entry.key());

            if (info != null)
                rejectedEntries.put(entry.key(), info);
        }

        /**
         * @param nodeId Node id that left the topology.
         */
        void onNodeLeft(UUID nodeId) {
            assert nodeId != null;

            // Stop waiting response from this node.
            reqMap.remove(nodeId);

            checkDone();
        }

        /**
         * @param nodeId Sender node id.
         * @param res Response.
         */
        void onResponse(UUID nodeId, GridCacheEvictionResponse<K, V> res) {
            assert nodeId != null;
            assert res != null;

            if (log.isDebugEnabled())
                log.debug("Entered to eviction future onResponse() [fut=" + this + ", node=" + nodeId +
                    ", res=" + res + ']');

            GridRichNode node = cctx.node(nodeId);

            if (node != null) {
                resMap.put(nodeId, res);
            }
            else
                // Sender node left grid.
                reqMap.remove(nodeId);

            checkDone();
        }

        /**
         *
         */
        private void checkDone() {
            if (reqMap.isEmpty() || resMap.keySet().containsAll(reqMap.keySet()))
                complete();
        }

        /**
         * Completes future.
         */
        void complete() {
            if (completing.compareAndSet(false, true)) {
                futs.remove(id);

                buildResult();

                onDone(F.t(evictedEntries, rejectedEntries.values()));
            }
        }

        /**
         * Builds collections of remotely evicted entries and rejected entries.
         */
        private void buildResult() {
            if (log.isDebugEnabled())
                log.debug("Building eviction future result...");

            for (EvictionInfo info : entries.values()) {
                K key = info.entry().key();

                if (rejectedEntries.containsKey(key))
                    // Was already rejected.
                    continue;

                boolean rejected = false;

                for (GridCacheEvictionResponse<K, V> res : resMap.values())
                    if (res.rejectedKeys().contains(key)) {
                        rejectedEntries.put(key, info);

                        rejected = true;

                        break;
                    }

                if (!rejected)
                    evictedEntries.add(info);
            }
        }

        /**
         * @param key Key.
         * @return Reader nodes on which given key was evicted.
         */
        Collection<GridTuple2<GridRichNode, Long>> evictedReaders(K key) {
            Collection<GridRichNode> mappedReaders = readers.get(key);

            if (mappedReaders == null)
                return Collections.emptyList();

            Collection<GridTuple2<GridRichNode, Long>> col = new LinkedList<GridTuple2<GridRichNode, Long>>();

            for (Map.Entry<UUID, GridCacheEvictionResponse<K, V>> e : resMap.entrySet()) {
                GridRichNode node = cctx.node(e.getKey());

                // If node has left or response did not arrive from near node
                // then just skip it.
                if (node == null || !mappedReaders.contains(node))
                    continue;

                GridCacheEvictionResponse<K, V> res = e.getValue();

                if (!res.rejectedKeys().contains(key))
                    col.add(F.t(node, res.messageId()));
            }

            return col;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(EvictionFuture.class, this);
        }
    }
}
