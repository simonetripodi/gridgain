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
import org.gridgain.grid.thread.*;
import org.gridgain.grid.typedef.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.worker.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Distributed Garbage Collector for cache.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.1.0c.31052011
 */
public class GridCacheDgcManager<K, V> extends GridCacheManager<K, V> {
    /** GC thread. */
    private GridThread gcThread;

    /** Response thread. */
    private GridThread resThread;

    /** Response worker. */
    private ResponseWorker resWorker;

    /** DGC frequency. */
    private int dgcFreq;

    /** DGC suspicious lock timeout. */
    private int dgcSuspiciousLockTimeout;

    /** {@inheritDoc} */
    @Override public void start0() throws GridException {
        if (cctx.config().getCacheMode() == GridCacheMode.LOCAL)
            // No-op for local cache.
            return;

        dgcFreq = cctx.config().getDistributedGarbageCollectionFrequency();

        A.ensure(dgcFreq >=0, "dgcFreq cannot be negative");

        dgcSuspiciousLockTimeout = cctx.config().getDistributedGarbageCollectionSuspectLockTimeout();

        A.ensure(dgcFreq >0, "dgcSuspiciousLockTimeout should be positive");

        if (dgcFreq > 0) {
            gcThread = new GridThread(new GcWorker());

            gcThread.start();
        }

        resThread = new GridThread(resWorker = new ResponseWorker());

        resThread.start();

        cctx.io().addHandler(GridCacheDgcMessage.class, new CI2<UUID, GridCacheDgcMessage<K, V>>() {
            @Override public void apply(UUID nodeId, GridCacheDgcMessage<K, V> req) {
                processMessage(nodeId, req);
            }
        });
    }

    /**
     * Stops GC manager.
     */
    @Override public void stop0(boolean cancel, boolean wait) {
        if (cctx.config().getCacheMode() == GridCacheMode.LOCAL)
            // No-op for local cache.
            return;

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
     * @param nodeId Node id.
     * @param msg Message
     */
    private void processMessage(UUID nodeId, GridCacheDgcMessage<K, V> msg) {
        if (log.isDebugEnabled())
            log.debug("Received DGC message [rmtNodeId=" + nodeId + ", req=" + msg + ']');

        if (msg.request())
            resWorker.addDgcRequest(nodeId, msg);
        else
            removeLocks(msg);
    }

    /**
     * @param msg Removes all locks that are provided in message.
     */
    private void removeLocks(GridCacheDgcMessage<K, V> msg) {
        assert msg != null;

        GridCacheVersion newVer = cctx.versions().next();

        for (Map.Entry<K, Collection<GridCacheVersion>> reqEntry : msg.candidatesMap().entrySet()) {
            K key = reqEntry.getKey();
            Collection<GridCacheVersion> col = reqEntry.getValue();

            while (true) {
                GridCacheEntryEx<K, V> cached = cctx.cache().peekEx(key);

                if (cached != null)
                    try {
                        // Invalidate before removing lock.
                        try {
                            cached.invalidate(null, newVer);
                        }
                        catch (GridException e) {
                            U.error(log, "Failed to invalidate entry: " + cached, e);
                        }

                        for (GridCacheVersion ver : col)
                            cached.removeLock(ver);

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
            super(cctx.gridName(), "cache-gc", log);
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"BusyWait"})
        @Override public void body() throws InterruptedException {
            assert dgcFreq > 0;

            while (!isCancelled()) {
                Thread.sleep(dgcFreq);

                if (log.isDebugEnabled())
                    log.debug("Starting DGC iteration.");

                Map<UUID, GridCacheDgcMessage<K, V>> map = new HashMap<UUID, GridCacheDgcMessage<K, V>>();

                long threshold = System.currentTimeMillis() - dgcSuspiciousLockTimeout;

                for (GridCacheMvccCandidate<K> lock : cctx.mvcc().remoteCandidates()) {
                    GridCacheDgcMessage<K, V> req = F.addIfAbsent(map, lock.nodeId(),
                        new GridCacheDgcMessage<K, V>(true /* request */));

                    assert req != null;

                    if (lock.timestamp() < threshold)
                        req.addCandidate(lock.key(), lock.version());
                }

                if (cctx.isDht()) {
                    // Adding near entries to message.
                    for (GridCacheMvccCandidate<K> lock :
                        ((GridDhtCache<K, V>)cctx.cache()).near().context().mvcc().remoteCandidates()) {
                        // All candidates are remote, filter out local.
                        if (!cctx.localNode().id().equals(lock.nodeId())) {
                            GridCacheDgcMessage<K, V> req = F.addIfAbsent(map, lock.nodeId(),
                                new GridCacheDgcMessage<K, V>(true /* request */));

                            assert req != null;

                            if (lock.timestamp() < threshold)
                                req.addCandidate(lock.key(), lock.version());
                        }
                    }
                }

                if (log.isDebugEnabled())
                    log.debug("Finished examining locks.");

                for (Map.Entry<UUID, GridCacheDgcMessage<K, V>> entry : map.entrySet()) {
                    UUID nodeId = entry.getKey();
                    GridCacheDgcMessage<K, V> req = entry.getValue();

                    if (cctx.discovery().node(nodeId) == null)
                        // Node has left the topology, safely remove all locks.
                        removeLocks(req);
                    else
                        sendMessage(nodeId, req);
                }

                if (log.isDebugEnabled())
                    log.debug("Finished DGC iteration.");
            }
        }
    }

    /**
     * Worker that processes GC requests and sends required responses.
     */
    private class ResponseWorker extends GridWorker {
        /** */
        private BlockingQueue<GridTuple2<UUID, GridCacheDgcMessage<K, V>>> queue =
            new LinkedBlockingQueue<GridTuple2<UUID, GridCacheDgcMessage<K, V>>>();

        /**
         * Default constructor.
         */
        ResponseWorker() {
            super(cctx.gridName(), "cache-gc-response", log);
        }

        /**
         * @param nodeId Node id.
         * @param req request.
         */
        void addDgcRequest(UUID nodeId, GridCacheDgcMessage<K, V> req) {
            assert nodeId != null;
            assert req != null;

            queue.add(F.t(nodeId, req));
        }

        /** {@inheritDoc} */
        @Override public void body() throws InterruptedException {
            while (!isCancelled()) {
                GridTuple2<UUID, GridCacheDgcMessage<K, V>> pair = queue.take();

                UUID senderId = pair.get1();
                GridCacheDgcMessage<K, V> req = pair.get2();

                GridCacheDgcMessage<K, V> res = new GridCacheDgcMessage<K, V>(false /* response */);

                for (Map.Entry<K, Collection<GridCacheVersion>> entry : req.candidatesMap().entrySet()) {
                    K key = entry.getKey();
                    Collection<GridCacheVersion> vers = entry.getValue();

                    while (true) {
                        GridCacheEntryEx<K, V> cached = cctx.cache().peekEx(key);

                        try {
                            if (cached != null) {
                                for (GridCacheVersion ver : vers)
                                    if (!cached.hasLockCandidate(ver))
                                        res.addCandidate(key, ver);
                            }
                            else
                                // Entry is removed, add all versions to response.
                                for (GridCacheVersion ver : vers)
                                    res.addCandidate(key, ver);

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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheDgcManager.class, this);
    }
}
