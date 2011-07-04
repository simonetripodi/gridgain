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
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.thread.*;
import org.gridgain.grid.typedef.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.worker.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.GridClosureCallMode.*;
import static org.gridgain.grid.cache.GridCacheConfiguration.*;

/**
 * Distributed Garbage Collector for cache.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.1.1c.03072011
 */
public class GridCacheDgcManager<K, V> extends GridCacheManager<K, V> {
    /** DGC thread. */
    private GridThread gcThread;

    /** Request worker thread. */
    private GridThread reqThread;

    /** Request worker. */
    private RequestWorker reqWorker;

    /** Response worker thread. */
    private GridThread resThread;

    /** Response worker. */
    private ResponseWorker resWorker;

    /** DGC frequency. */
    private int dgcFreq;

    /** DGC suspect lock timeout. */
    private int dgcSuspectLockTimeout;

    /** Trace log. */
    private GridLogger traceLog;

    /** */
    private CI2<UUID, GridCacheDgcRequest<K, V>> reqHandler = new CI2<UUID, GridCacheDgcRequest<K, V>>() {
        @Override public void apply(UUID nodeId, GridCacheDgcRequest<K, V> req) {
            if (log.isDebugEnabled())
                log.debug("Received DGC request [rmtNodeId=" + nodeId + ", req=" + req + ']');

            reqWorker.addDgcRequest(F.t(nodeId, req));
        }
    };

    /** */
    private CI2<UUID, GridCacheDgcResponse<K, V>> resHandler = new CI2<UUID, GridCacheDgcResponse<K, V>>() {
        @Override public void apply(UUID nodeId, GridCacheDgcResponse<K, V> res) {
            if (log.isDebugEnabled())
                log.debug("Received DGC response [rmtNodeId=" + nodeId + ", res=" + res + ']');

            resWorker.addDgcResponse(F.t(nodeId, res));
        }
    };

    /** {@inheritDoc} */
    @Override public void start0() throws GridException {
        if (cctx.config().getCacheMode() == GridCacheMode.LOCAL)
            // No-op for local cache.
            return;

        traceLog = log.getLogger(DGC_TRACE_LOGGER_NAME);

        dgcFreq = cctx.config().getDgcFrequency();

        A.ensure(dgcFreq >= 0, "dgcFreq cannot be negative");

        dgcSuspectLockTimeout = cctx.config().getDgcSuspectLockTimeout();

        A.ensure(dgcSuspectLockTimeout >= 0, "dgcSuspiciousLockTimeout cannot be negative");

        if (dgcFreq > 0 && log.isDebugEnabled()) {
            log.debug(
                "Locks older than " + dgcSuspectLockTimeout + " ms. " +
                "will be implicitly removed in case they are not present on lock owner nodes. " +
                "To change this behavior please configure 'dgcFrequency' and 'dgcSuspectLockTimeout' " +
                "cache configuration properties."
            );
        }

        reqThread = new GridThread(reqWorker = new RequestWorker());

        reqThread.start();

        resThread = new GridThread(resWorker = new ResponseWorker());

        resThread.start();

        cctx.io().addHandler(GridCacheDgcRequest.class, reqHandler);
        cctx.io().addHandler(GridCacheDgcResponse.class, resHandler);

        if (log.isDebugEnabled())
            log.debug("Started DGC manager " +
                "[dgcFreq=" + dgcFreq + ", suspectLockTimeout=" + dgcSuspectLockTimeout + ']');
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStart0() throws GridException {
        if (dgcFreq > 0) {
            // Start thread here since discovery may not start within DGC frequency and
            // thread cannot be started in start0() method.
            gcThread = new GridThread(new DgcWorker());

            gcThread.start();
        }
    }

    /** {@inheritDoc} */
    @Override public void stop0(boolean cancel, boolean wait) {
        if (cctx.config().getCacheMode() == GridCacheMode.LOCAL)
            // No-op for local cache.
            return;

        cctx.io().removeHandler(GridCacheDgcRequest.class, reqHandler);
        cctx.io().removeHandler(GridCacheDgcResponse.class, resHandler);

        if (reqThread != null) {
            U.interrupt(reqThread);

            U.join(reqThread, log);
        }

        if (resThread != null) {
            U.interrupt(resThread);

            U.join(resThread, log);
        }

        if (gcThread != null) {
            U.interrupt(gcThread);

            U.join(gcThread, log);
        }
    }

    /**
     * Runs DGC procedure on demand using
     * {@link GridCacheConfiguration#getDgcSuspectLockTimeout()} to identify suspect locks.
     * <p>
     * Method blocks current thread until locks are examined and all DGC requests are sent
     * to remote nodes.
     * <p>
     * DGC does not remove locks if {@link GridCacheConfiguration#isDgcRemoveLocks()}
     * is set to {@code false}.
     */
    public void dgc() {
        dgc(dgcSuspectLockTimeout, true, cctx.config().isDgcRemoveLocks());
    }

    /**
     * Runs DGC procedure on demand using provided parameter to identify suspect locks.
     * <p>
     * Method blocks current thread until locks are examined and all DGC requests are sent
     * to remote nodes and (if {@code global} is {@code true}) all nodes having this cache
     * get signal to start DGC procedure.
     *
     * @param suspectLockTimeout Custom suspect lock timeout (should be greater than or equal to 0).
     * @param global If {@code true} then DGC procedure will start on all nodes having this cache.
     * @param rmvLocks If {@code false} then DGC does not remove locks, just report them to log.
     */
    public void dgc(int suspectLockTimeout, boolean global, boolean rmvLocks) {
        A.ensure(suspectLockTimeout >= 0, "suspectLockTimeout cannot be negative");

        if (log.isDebugEnabled())
            log.debug("Starting DGC iteration.");

        Map<UUID, GridCacheDgcRequest<K, V>> map = new HashMap<UUID, GridCacheDgcRequest<K, V>>();

        final long threshold = System.currentTimeMillis() - suspectLockTimeout;

        Collection<GridCacheMvccCandidate<K>> suspectLocks = F.view(
            cctx.mvcc().remoteCandidates(),
            new P1<GridCacheMvccCandidate<K>>() {
                @Override public boolean apply(GridCacheMvccCandidate<K> lock) {
                    return lock.timestamp() < threshold;
                }
            }
        );

        if (traceLog.isDebugEnabled() && !suspectLocks.isEmpty()) {
            traceLog.debug("Beginning to check on DHT suspect locks [" + U.newLine() +
                "\t DHT suspect locks: " + suspectLocks + "," + U.newLine() +
                "\t DHT active transactions: " + cctx.tm().txs() + U.newLine() +
                "\t DHT active local locks: " + cctx.mvcc().localCandidates() + U.newLine() +
                "\t DHT active remote locks: " + cctx.mvcc().remoteCandidates() + U.newLine() +
                "]");
        }

        for (GridCacheMvccCandidate<K> lock : suspectLocks) {
            GridCacheDgcRequest<K, V> req = F.addIfAbsent(map, lock.nodeId(), new GridCacheDgcRequest<K, V>());

            assert req != null;

            req.removeLocks(rmvLocks);

            req.addCandidate(lock.key(), lock.version());
        }

        if (cctx.isDht()) {
            // Add near entries to message.
            GridNearCache<K,V> nearCache = cctx.dht().near();

            Collection<GridCacheMvccCandidate<K>> nearSuspectLocks = F.view(
                nearCache.context().mvcc().remoteCandidates(),
                new P1<GridCacheMvccCandidate<K>>() {
                    @Override public boolean apply(GridCacheMvccCandidate<K> lock) {
                        return !cctx.localNode().id().equals(lock.nodeId()) && lock.timestamp() < threshold;
                    }
                }
            );

            if (traceLog.isDebugEnabled() && !nearSuspectLocks.isEmpty()) {
                GridCacheContext<K, V> nearCtx = nearCache.context();

                traceLog.debug("Beginning to check on near suspect locks [" + U.newLine() +
                    "\t near suspect locks: " + nearSuspectLocks + "," + U.newLine() +
                    "\t near active transactions: " + nearCtx.tm().txs() + U.newLine() +
                    "\t near active local locks: " + nearCtx.mvcc().localCandidates() + U.newLine() +
                    "\t near active remote locks: " + nearCtx.mvcc().remoteCandidates() + U.newLine() +
                    "]");
            }

            for (GridCacheMvccCandidate<K> lock : nearSuspectLocks) {
                // All candidates are remote, filter out local.
                GridCacheDgcRequest<K, V> req = F.addIfAbsent(map, lock.nodeId(), new GridCacheDgcRequest<K, V>());

                assert req != null;

                req.removeLocks(rmvLocks);

                req.addCandidate(lock.key(), lock.version());
            }
        }

        if (log.isDebugEnabled())
            log.debug("Finished examining locks.");

        for (Map.Entry<UUID, GridCacheDgcRequest<K, V>> entry : map.entrySet()) {
            UUID nodeId = entry.getKey();
            GridCacheDgcRequest<K, V> req = entry.getValue();

            if (cctx.discovery().node(nodeId) == null)
                // Node has left the topology, safely remove all locks.
                resWorker.addDgcResponse(F.t(nodeId, createFakeResponse(req)));
            else
                sendMessage(nodeId, req);
        }

        if (log.isDebugEnabled())
            log.debug("Finished sending DGC requests.");

        Collection<GridRichNode> nodes = CU.remoteNodes(cctx);

        if (global && !nodes.isEmpty())
            cctx.closures().callAsync(
                BROADCAST,
                new DgcCallable(cctx.name(), suspectLockTimeout, cctx.config().isDgcRemoveLocks()),
                nodes
            );

        if (log.isDebugEnabled())
            log.debug("Finished DGC iteration.");
    }

    /**
     * @param req Request to create fake response for.
     * @return Fake response.
     */
    private GridCacheDgcResponse<K, V> createFakeResponse(GridCacheDgcRequest<K, V> req) {
        assert req != null;

        GridCacheDgcResponse<K, V> res = new GridCacheDgcResponse<K, V>();

        res.removeLocks(req.removeLocks());

        for (Map.Entry<K, Collection<GridCacheVersion>> entry : req.candidatesMap().entrySet()) {
            K key = entry.getKey();

            for (GridCacheVersion ver : entry.getValue())
                res.addCandidate(key, ver, false);
        }

        return res;
    }

    /**
     * Send message to node.
     *
     * @param nodeId Node id.
     * @param msg  Message to send.
     */
    private void sendMessage(UUID nodeId, GridCacheMessage<K, V> msg) {
        try {
            cctx.io().send(nodeId, msg);

            if (log.isDebugEnabled())
                log.debug("Sent DGC message [rmtNodeId=" + nodeId + ", msg=" + msg + ']');
        }
        catch (GridTopologyException ignored) {
            if (log.isDebugEnabled())
                log.debug("Failed to send message to node (node left grid): " + nodeId);
        }
        catch (GridException e) {
            U.error(log, "Failed to send message to node: " + nodeId, e);
        }
    }

    /**
     * Worker that scans current locks and initiates DGC requests if needed.
     */
    private class DgcWorker extends GridWorker {
        /**
         * Constructor.
         */
        private DgcWorker() {
            super(cctx.gridName(), "cache-dgc-wrk", log);
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"BusyWait"})
        @Override public void body() throws InterruptedException {
            assert dgcFreq > 0;

            while (!isCancelled()) {
                Thread.sleep(dgcFreq);

                dgc(dgcSuspectLockTimeout, false, cctx.config().isDgcRemoveLocks());
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheDgcManager.class, this);
    }

    /**
     * Worker that processes DGC requests and sends responses back.
     */
    private class RequestWorker extends GridWorker {
        /** */
        private BlockingQueue<GridTuple2<UUID, GridCacheDgcRequest<K, V>>> queue =
            new LinkedBlockingQueue<GridTuple2<UUID, GridCacheDgcRequest<K, V>>>();

        /**
         * Default constructor.
         */
        RequestWorker() {
            super(cctx.gridName(), "cache-dgc-req-wrk", log);
        }

        /**
         * @param t Request tuple.
         */
        void addDgcRequest(GridTuple2<UUID, GridCacheDgcRequest<K, V>> t) {
            assert t != null;

            queue.add(t);
        }

        /** {@inheritDoc} */
        @Override public void body() throws InterruptedException {
            while (!isCancelled()) {
                GridTuple2<UUID, GridCacheDgcRequest<K, V>> tup = queue.take();

                UUID senderId = tup.get1();
                GridCacheDgcRequest<K, V> req = tup.get2();

                GridCacheDgcResponse<K, V> res = new GridCacheDgcResponse<K, V>();

                res.removeLocks(req.removeLocks());

                for (Map.Entry<K, Collection<GridCacheVersion>> entry : req.candidatesMap().entrySet()) {
                    K key = entry.getKey();
                    Collection<GridCacheVersion> vers = entry.getValue();

                    while (true) {
                        GridCacheEntryEx<K, V> cached = cctx.cache().peekEx(key);

                        try {
                            if (cached != null) {
                                for (GridCacheVersion ver : vers) {
                                    if (!cached.hasLockCandidate(ver)) {
                                        res.addCandidate(key, ver, cctx.tm().rolledbackVersions(ver).contains(ver));

                                        if (traceLog.isDebugEnabled()) {
                                            String nearInfo = "";

                                            if (cctx.isDht()) {
                                                GridCacheContext<K, V> nearCtx = cctx.dht().near().context();

                                                nearInfo =
                                                    "\t near active transactions: " + nearCtx.tm().txs() +
                                                    U.newLine() +
                                                    "\t near active local locks: " + nearCtx.mvcc().localCandidates() +
                                                    U.newLine() +
                                                    "\t near active remote locks: " +
                                                        nearCtx.mvcc().remoteCandidates() +
                                                    U.newLine();
                                            }

                                            traceLog.debug("DHT Entry is not locked on local node, but locked on " +
                                                "remote [entry=" + cached + ", ver=" + ver + ", rmtNodeId=" + senderId +
                                                U.newLine() +
                                                "\t DHT active transactions: " + cctx.tm().txs() +
                                                U.newLine() +
                                                "\t DHT active local locks: " + cctx.mvcc().localCandidates() +
                                                U.newLine() +
                                                "\t DHT active remote locks: " + cctx.mvcc().remoteCandidates() +
                                                U.newLine() +
                                                nearInfo +
                                                ']');
                                        }
                                    }
                                }
                            }
                            else {
                                // Entry is removed, add all versions to response.
                                for (GridCacheVersion ver : vers)
                                    res.addCandidate(key, ver, cctx.tm().rolledbackVersions(ver).contains(ver));

                                if (traceLog.isDebugEnabled()) {
                                    String nearInfo = "";

                                    if (cctx.isDht()) {
                                        GridCacheContext<K, V> nearCtx = cctx.dht().near().context();

                                        nearInfo =
                                            "\t near active transactions: " + nearCtx.tm().txs() +
                                            U.newLine() +
                                            "\t near active local locks: " + nearCtx.mvcc().localCandidates() +
                                            U.newLine() +
                                            "\t near active remote locks: " +
                                                nearCtx.mvcc().remoteCandidates() +
                                            U.newLine();
                                    }

                                    traceLog.debug("Entry has been removed on local node, but still locked on remote " +
                                        "[key=" + key + ", vers=" + vers + ", rmtNodeId=" + senderId +
                                        U.newLine() +
                                        "\t DHT active transactions: " + cctx.tm().txs() +
                                        U.newLine() +
                                        "\t DHT active local locks: " + cctx.mvcc().localCandidates() +
                                        U.newLine() +
                                        "\t DHT active remote locks: " + cctx.mvcc().remoteCandidates() +
                                        U.newLine() +
                                        nearInfo +
                                        ']');
                                }
                            }

                            break;
                        }
                        catch (GridCacheEntryRemovedException ignored) {
                            if (log.isDebugEnabled())
                                log.debug("Got removed entry during DGC (will retry): " + cached);
                        }
                    }
                }

                if (!res.candidatesMap().isEmpty())
                    sendMessage(senderId, res);
            }
        }
    }

    /**
     * Worker that processes DGC responses.
     */
    private class ResponseWorker extends GridWorker {
        /** */
        private BlockingQueue<GridTuple2<UUID, GridCacheDgcResponse<K, V>>> queue =
            new LinkedBlockingQueue<GridTuple2<UUID, GridCacheDgcResponse<K, V>>>();

        /**
         * Default constructor.
         */
        ResponseWorker() {
            super(cctx.gridName(), "cache-dgc-res-wrk", log);
        }

        /**
         * @param t Response tuple.
         */
        void addDgcResponse(GridTuple2<UUID, GridCacheDgcResponse<K, V>> t) {
            assert t != null;

            queue.add(t);
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"MismatchedQueryAndUpdateOfCollection", "TooBroadScope"})
        @Override public void body() throws InterruptedException {
            while (!isCancelled()) {
                GridTuple2<UUID, GridCacheDgcResponse<K, V>> tup = queue.take();

                GridCacheDgcResponse<K, V> res = tup.get2();

                int salvagedTxCnt = 0;
                int rolledbackTxCnt = 0;
                int rmvLockCnt = 0;

                Map<K, Collection<GridCacheVersion>> nonTx = new HashMap<K, Collection<GridCacheVersion>>();

                for (Map.Entry<K, Collection<GridTuple2<GridCacheVersion, Boolean>>> e :
                    res.candidatesMap().entrySet()) {
                    for (GridTuple2<GridCacheVersion, Boolean> t : e.getValue()) {
                        GridCacheTxEx<K, V> tx = cctx.tm().<GridCacheTxEx<K, V>>tx(t.get1());

                        if (tx != null && t.get2() != null && t.get2()) {
                            if (res.removeLocks()) {
                                cctx.tm().rollbackTx(tx);

                                if (traceLog.isDebugEnabled())
                                    traceLog.debug("DHT transaction has been rolled back: " + tx);

                                rolledbackTxCnt++;
                            }
                            else if (traceLog.isDebugEnabled())
                                traceLog.debug("DGC has not rolled back DHT transaction due to user configuration: " +
                                    tx);
                        }
                        else if (tx != null) {
                            if (res.removeLocks()) {
                                cctx.tm().salvageTx(tx);

                                if (traceLog.isDebugEnabled())
                                    traceLog.debug("DHT transaction has been salvaged: " + tx);

                                salvagedTxCnt++;
                            }
                            else if (traceLog.isDebugEnabled())
                                traceLog.debug("DGC has not salvaged DHT transaction due to user configuration: " + tx);
                        }
                        else {
                            Collection<GridCacheVersion> col =
                                F.addIfAbsent(nonTx, e.getKey(), new LinkedHashSet<GridCacheVersion>());

                            assert col != null;

                            col.add(t.get1());
                        }
                    }
                }

                if (cctx.isDht()) {
                    // Process near cache.
                    for (Map.Entry<K, Collection<GridTuple2<GridCacheVersion, Boolean>>> e :
                        res.candidatesMap().entrySet()) {
                        GridCacheTxManager<K, V> nearTm = cctx.dht().near().context().tm();

                        for (GridTuple2<GridCacheVersion, Boolean> t : e.getValue()) {
                            GridCacheTxEx<K, V> tx = nearTm.<GridCacheTxEx<K, V>>tx(t.get1());

                            if (tx != null && t.get2() != null && t.get2()) {
                                if (res.removeLocks()) {
                                    nearTm.rollbackTx(tx);

                                    if (traceLog.isDebugEnabled())
                                        traceLog.debug("Near transaction has been rolled back: " + tx);

                                    rolledbackTxCnt++;
                                }
                                else if (traceLog.isDebugEnabled()) {
                                    traceLog.debug("DGC has not rolled back near transaction due to user " +
                                        "configuration: " + tx);
                                }
                            }
                            else if (tx != null) {
                                if (res.removeLocks()) {
                                    nearTm.salvageTx(tx);

                                    if (traceLog.isDebugEnabled())
                                        traceLog.debug("Near transaction has been salvaged: " + tx);

                                    salvagedTxCnt++;
                                }
                                else if (traceLog.isDebugEnabled())
                                    traceLog.debug("DGC has not salvaged near transaction due to user configuration: " +
                                        tx);
                            }
                            else {
                                Collection<GridCacheVersion> col =
                                    F.addIfAbsent(nonTx, e.getKey(), new LinkedHashSet<GridCacheVersion>());

                                assert col != null;

                                col.add(t.get1());
                            }
                        }
                    }
                }

                if (!nonTx.isEmpty()) {
                    for (Map.Entry<K, Collection<GridCacheVersion>> e : nonTx.entrySet()) {
                        GridCacheVersion newVer = cctx.versions().next();

                        while (true) {
                            GridCacheEntryEx<K, V> cached = cctx.cache().peekEx(e.getKey());

                            if (cached != null) {
                                if (!res.removeLocks()) {
                                    if (traceLog.isDebugEnabled()) {
                                        traceLog.debug("DGC has not removed locks on DHT entry due to user " +
                                            "configuration [entry=" + cached + ", vers=" + e.getValue() + ']');
                                    }

                                    break; // While loop.
                                }

                                try {
                                    // Invalidate before removing lock.
                                    try {
                                        cached.invalidate(null, newVer);
                                    }
                                    catch (GridException ex) {
                                        U.error(log, "Failed to invalidate entry: " + cached, ex);
                                    }

                                    for (GridCacheVersion ver : e.getValue()) {
                                        if (cached.removeLock(ver))
                                            rmvLockCnt++;
                                    }

                                    break; // While loop.
                                }
                                catch (GridCacheEntryRemovedException ignored) {
                                    if (log.isDebugEnabled())
                                        log.debug("Attempted to remove lock on obsolete entry (will retry).");
                                }
                            }
                            else
                                break;
                        }
                    }

                    if (cctx.isDht()) {
                        for (Map.Entry<K, Collection<GridCacheVersion>> e : nonTx.entrySet()) {
                            GridCacheVersion newVer = cctx.dht().near().context().versions().next();

                            while (true) {
                                GridCacheEntryEx<K, V> cached = cctx.dht().near().peekEx(e.getKey());

                                if (cached != null) {
                                    if (!res.removeLocks()) {
                                        if (traceLog.isDebugEnabled()) {
                                            traceLog.debug("DGC has not removed locks on near entry due to user " +
                                                "configuration [entry=" + cached + ", vers=" + e.getValue() + ']');
                                        }

                                        break; // While loop.
                                    }

                                    try {
                                        // Invalidate before removing lock.
                                        try {
                                            cached.invalidate(null, newVer);
                                        }
                                        catch (GridException ex) {
                                            U.error(log, "Failed to invalidate entry: " + cached, ex);
                                        }

                                        for (GridCacheVersion ver : e.getValue()) {
                                            if (cached.removeLock(ver))
                                                rmvLockCnt++;
                                        }

                                        break; // While loop.
                                    }
                                    catch (GridCacheEntryRemovedException ignored) {
                                        if (log.isDebugEnabled())
                                            log.debug("Attempted to remove lock on obsolete entry (will retry).");
                                    }
                                }
                                else
                                    break;
                            }
                        }
                    }
                }

                if (salvagedTxCnt != 0 || rolledbackTxCnt != 0 || rmvLockCnt != 0) {
                    U.warn(log, "DGCed suspicious transactions and locks " +
                        "[rmtNodeId=" + tup.get1() + ", salvagedTxCnt=" + salvagedTxCnt +
                        ", rolledbackTxCnt=" + rolledbackTxCnt +
                        ", rmvLockCnt=" + rmvLockCnt + ']');
                }
            }
        }
    }

    /**
     *
     */
    private static class DgcCallable extends GridCallable<Object> {
        /** */
        private final String cacheName;

        /** */
        private final int suspectLockTimeout;

        /** */
        private final boolean removeLocks;

        /** */
        @GridInstanceResource
        private Grid grid;

        /**
         * @param cacheName Cache name.
         * @param suspectLockTimeout Suspect lock timeout.
         * @param removeLocks Remove locks flag.
         */
        private DgcCallable(String cacheName, int suspectLockTimeout, boolean removeLocks) {
            this.cacheName = cacheName;
            this.suspectLockTimeout = suspectLockTimeout;
            this.removeLocks = removeLocks;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object call() throws Exception {
            grid.cache(cacheName).dgc(suspectLockTimeout, false, removeLocks);

            return null;
        }
    }
}
