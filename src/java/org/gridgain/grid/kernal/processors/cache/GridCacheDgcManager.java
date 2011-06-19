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
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.thread.*;
import org.gridgain.grid.typedef.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.worker.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.GridClosureCallMode.*;

/**
 * Distributed Garbage Collector for cache.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.1.1c.19062011
 */
public class GridCacheDgcManager<K, V> extends GridCacheManager<K, V> {
    /** GC thread. */
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

    /** */
    private CI2<UUID, GridCacheDgcRequest<K, V>> reqHandler = new CI2<UUID, GridCacheDgcRequest<K, V>>() {
        @Override public void apply(UUID nodeId, GridCacheDgcRequest<K, V> req) {
            if (log.isDebugEnabled())
                log.debug("Received DGC request [rmtNodeId=" + nodeId + ", req=" + req + ']');

            reqWorker.addDgcRequest(nodeId, req);
        }
    };

    /** */
    private CI2<UUID, GridCacheDgcResponse<K, V>> resHandler = new CI2<UUID, GridCacheDgcResponse<K, V>>() {
        @Override public void apply(UUID nodeId, GridCacheDgcResponse<K, V> res) {
            if (log.isDebugEnabled())
                log.debug("Received DGC response [rmtNodeId=" + nodeId + ", res=" + res + ']');

            resWorker.addDgcResponse(res);
        }
    };

    /** {@inheritDoc} */
    @Override public void start0() throws GridException {
        if (cctx.config().getCacheMode() == GridCacheMode.LOCAL)
            // No-op for local cache.
            return;

        dgcFreq = cctx.config().getDgcFrequency();

        A.ensure(dgcFreq >= 0, "dgcFreq cannot be negative");

        dgcSuspectLockTimeout = cctx.config().getDgcSuspectLockTimeout();

        A.ensure(dgcSuspectLockTimeout >= 0, "dgcSuspiciousLockTimeout cannot be negative");

        if (dgcFreq > 0) {
            U.warn(log,
                "Locks older than " + dgcSuspectLockTimeout + "ms. " +
                "will be implicitly removed in case they are not present on lock owner nodes. " +
                "To change this behavior please configure 'dgcFrequency' and 'dgcSuspectLockTimeout' " +
                "cache configuration properties.",
                "Locks older than " + dgcSuspectLockTimeout + "ms. " +
                "will be removed in case they are not present on lock owner nodes. "
            );

            gcThread = new GridThread(new GcWorker());

            gcThread.start();
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
     * Runs DGC procedure locally on demand using
     * {@link GridCacheConfiguration#getDgcSuspectLockTimeout()} to identify suspect locks.
     * <p>
     * Method blocks current thread until locks are examined and all DGC requests are sent
     * to remote nodes.
     */
    public void dgc() {
        dgc(dgcSuspectLockTimeout, false);
    }

    /**
     * Runs DGC procedure on demand using
     * {@link GridCacheConfiguration#getDgcSuspectLockTimeout()} to identify suspect locks.
     * <p>
     * Method blocks current thread until locks are examined and all DGC requests are sent
     * to remote nodes and (if {@code global} is {@code true}) all nodes having this cache
     * get signal to start GC procedure.
     *
     * @param global If {@code true} then GC procedure will start on all nodes having this cache.
     */
    public void dgc(boolean global) {
        dgc(dgcSuspectLockTimeout, global);
    }

    /**
     * Runs DGC procedure locally on demand using provided parameter to identify suspect locks.
     * <p>
     * Method blocks current thread until locks are examined and all DGC requests are sent
     * to remote nodes.
     *
     * @param suspectLockTimeout Custom suspect lock timeout (should be greater than or equal to 0).
     */
    public void dgc(int suspectLockTimeout) {
        dgc(suspectLockTimeout, false);
    }

    /**
     * Runs DGC procedure on demand using provided parameter to identify suspect locks.
     * <p>
     * Method blocks current thread until locks are examined and all DGC requests are sent
     * to remote nodes and (if {@code global} is {@code true}) all nodes having this cache
     * get signal to start GC procedure.
     *
     * @param suspectLockTimeout Custom suspect lock timeout (should be greater than or equal to 0).
     * @param global If {@code true} then GC procedure will start on all nodes having this cache.
     */
    public void dgc(int suspectLockTimeout, boolean global) {
        A.ensure(suspectLockTimeout >= 0, "suspectLockTimeout cannot be negative");

        if (log.isDebugEnabled())
            log.debug("Starting DGC iteration.");

        Map<UUID, GridCacheDgcRequest<K, V>> map = new HashMap<UUID, GridCacheDgcRequest<K, V>>();

        long threshold = System.currentTimeMillis() - suspectLockTimeout;

        for (GridCacheMvccCandidate<K> lock : cctx.mvcc().remoteCandidates()) {
            if (lock.timestamp() < threshold) {
                GridCacheDgcRequest<K, V> req = F.addIfAbsent(map, lock.nodeId(),
                    new GridCacheDgcRequest<K, V>());

                assert req != null;

                req.addCandidate(lock.key(), lock.version());
            }
        }

        if (cctx.isDht()) {
            // Adding near entries to message.
            for (GridCacheMvccCandidate<K> lock :
                ((GridDhtCache<K, V>)cctx.cache()).near().context().mvcc().remoteCandidates()) {
                // All candidates are remote, filter out local.
                if (!cctx.localNode().id().equals(lock.nodeId()) && lock.timestamp() < threshold) {
                    GridCacheDgcRequest<K, V> req = F.addIfAbsent(map, lock.nodeId(),
                        new GridCacheDgcRequest<K, V>());

                    assert req != null;

                    req.addCandidate(lock.key(), lock.version());
                }
            }
        }

        if (log.isDebugEnabled())
            log.debug("Finished examining locks.");

        for (Map.Entry<UUID, GridCacheDgcRequest<K, V>> entry : map.entrySet()) {
            UUID nodeId = entry.getKey();
            GridCacheDgcRequest<K, V> req = entry.getValue();

            if (cctx.discovery().node(nodeId) == null)
                // Node has left the topology, safely remove all locks.
                resWorker.addDgcResponse(createFakeResponse(req));
            else
                sendMessage(nodeId, req);
        }

        if (log.isDebugEnabled())
            log.debug("Finished sending DGC requests.");

        Collection<GridRichNode> nodes = CU.remoteNodes(cctx);

        if (global && !nodes.isEmpty()) {
            try {
                cctx.closures().callAsync(
                    BROADCAST,
                    new DgcCallable(cctx.name(), suspectLockTimeout),
                    nodes
                );
            }
            catch (GridException e) {
                log.error("Failed to send DGC closure to nodes: " + nodes, e);
            }
        }

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
     * Worker that scans current locks and initiates GC requests if needed.
     */
    private class GcWorker extends GridWorker {
        /**
         * Constructor.
         */
        private GcWorker() {
            super(cctx.gridName(), "cache-dgc-wrk", log);
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"BusyWait"})
        @Override public void body() throws InterruptedException {
            assert dgcFreq > 0;

            while (!isCancelled()) {
                Thread.sleep(dgcFreq);

                dgc(dgcSuspectLockTimeout);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheDgcManager.class, this);
    }

    /**
     * Worker that processes GC requests and sends responses back.
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
         * @param nodeId Node id.
         * @param req Request.
         */
        void addDgcRequest(UUID nodeId, GridCacheDgcRequest<K, V> req) {
            assert nodeId != null;
            assert req != null;

            queue.add(F.t(nodeId, req));
        }

        /** {@inheritDoc} */
        @Override public void body() throws InterruptedException {
            while (!isCancelled()) {
                GridTuple2<UUID, GridCacheDgcRequest<K, V>> tup = queue.take();

                UUID senderId = tup.get1();
                GridCacheDgcRequest<K, V> req = tup.get2();

                GridCacheDgcResponse<K, V> res = new GridCacheDgcResponse<K, V>();

                for (Map.Entry<K, Collection<GridCacheVersion>> entry : req.candidatesMap().entrySet()) {
                    K key = entry.getKey();
                    Collection<GridCacheVersion> vers = entry.getValue();

                    while (true) {
                        GridCacheEntryEx<K, V> cached = cctx.cache().peekEx(key);

                        try {
                            if (cached != null) {
                                for (GridCacheVersion ver : vers)
                                    if (!cached.hasLockCandidate(ver))
                                        res.addCandidate(key, ver, cctx.tm().rolledbackVersions(ver).contains(ver));
                            }
                            else
                                // Entry is removed, add all versions to response.
                                for (GridCacheVersion ver : vers)
                                    res.addCandidate(key, ver, cctx.tm().rolledbackVersions(ver).contains(ver));

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
     * Worker that processes GC responses.
     */
    private class ResponseWorker extends GridWorker {
        /** */
        private BlockingQueue<GridCacheDgcResponse<K, V>> queue =
            new LinkedBlockingQueue<GridCacheDgcResponse<K, V>>();

        /**
         * Default constructor.
         */
        ResponseWorker() {
            super(cctx.gridName(), "cache-dgc-res-wrk", log);
        }

        /**
         * @param res Response.
         */
        void addDgcResponse(GridCacheDgcResponse<K, V> res) {
            assert res != null;

            queue.add(res);
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"MismatchedQueryAndUpdateOfCollection", "TooBroadScope"})
        @Override public void body() throws InterruptedException {
            while (!isCancelled()) {
                GridCacheDgcResponse<K, V> res = queue.take();

                int salvagedTxCnt = 0;
                int rolledbackTxCnt = 0;
                int rmvLockCnt = 0;

                Map<K, Collection<GridCacheVersion>> nonTx = new HashMap<K, Collection<GridCacheVersion>>();

                for (Map.Entry<K, Collection<GridTuple2<GridCacheVersion, Boolean>>> e : res.candidatesMap().entrySet()) {
                    for (GridTuple2<GridCacheVersion, Boolean> t : e.getValue()) {
                        GridCacheTxEx<K, V> tx = cctx.tm().<GridCacheTxEx<K, V>>tx(t.get1());

                        if (tx != null && t.get2() != null && t.get2()) {
                            cctx.tm().rollbackTx(tx);

                            rolledbackTxCnt++;
                        }
                        else if (tx != null) {
                            cctx.tm().salvageTx(tx);

                            salvagedTxCnt++;
                        }
                        else {
                            Collection<GridCacheVersion> col =
                                F.addIfAbsent(nonTx, e.getKey(), new ArrayList<GridCacheVersion>());

                            assert col != null;

                            col.add(t.get1());
                        }
                    }
                }

                if (!nonTx.isEmpty()) {
                    for (Map.Entry<K, Collection<GridCacheVersion>> e : nonTx.entrySet()) {
                        GridCacheVersion newVer = cctx.versions().next();

                        while (true) {
                            GridCacheEntryEx<K, V> cached = cctx.cache().peekEx(e.getKey());

                            if (cached != null)
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
                            else
                                break;
                        }
                    }
                }

                if (salvagedTxCnt != 0 || rolledbackTxCnt != 0 || rmvLockCnt != 0)
                    U.warn(log, "GCed suspicious transactions and locks " +
                        "[salvagedTxCnt=" + salvagedTxCnt + ", rolledbackTxCnt=" + rolledbackTxCnt +
                        ", rmvLockCnt=" + rmvLockCnt + ']');
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
        @GridInstanceResource
        private Grid grid;

        /**
         * @param cacheName Cache name.
         * @param suspectLockTimeout Suspect lock timeout.
         */
        private DgcCallable(String cacheName, int suspectLockTimeout) {
            this.cacheName = cacheName;
            this.suspectLockTimeout = suspectLockTimeout;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object call() throws Exception {
            grid.cache(cacheName).dgc(suspectLockTimeout, false);

            return null;
        }
    }
}
