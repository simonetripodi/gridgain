// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht.preloader;

import org.gridgain.grid.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.kernal.processors.timeout.*;
import org.gridgain.grid.lang.utils.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.typedef.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

/**
 * Future for exchanging partition maps.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.1.1c.19062011
 */
public class GridDhtPartitionsExchangeFuture<K, V> extends GridFutureAdapter<Object>
    implements Comparable<GridDhtPartitionsExchangeFuture<K, V>> {
    /** Dummy flag. */
    private final boolean dummy;

    /** */
    private GridDhtPartitionTopology<K, V> top;

    /** Discovery event. */
    private volatile GridDiscoveryEvent discoEvt;

    /** */
    @GridToStringInclude
    private final Collection<UUID> rcvdIds = new GridConcurrentHashSet<UUID>();

    /** Remote nodes. */
    private Collection<GridRichNode> rmtNodes;

    /** Remote nodes. */
    @GridToStringInclude
    private Collection<UUID> rmtIds;

    /** Oldest node. */
    @GridToStringExclude
    private volatile GridNode oldestNode;

    /** ExchangeFuture id. */
    private GridDhtPartitionExchangeId exchId;

    /** Init flag. */
    @GridToStringInclude
    private final AtomicBoolean init = new AtomicBoolean(false);

    /** Ready for reply flag. */
    @GridToStringInclude
    private final AtomicBoolean ready = new AtomicBoolean(false);

    /** Replied flag. */
    @GridToStringInclude
    private final AtomicBoolean replied = new AtomicBoolean(false);

    /** Timeout object. */
    @GridToStringExclude
    private volatile GridTimeoutObject timeoutObj;

    /** Cache context. */
    private GridCacheContext<K, V> cctx;

    /** Busy lock to prevent activities from accessing exchanger while it's stopping. */
    private ReadWriteLock busyLock;

    /** */
    private AtomicBoolean added = new AtomicBoolean(false);

    /** Event latch. */
    @GridToStringExclude
    private CountDownLatch evtLatch = new CountDownLatch(1);

    /** */
    private GridFutureAdapter<Boolean> initFut;

    /** Logger. */
    private GridLogger log;

    /**
     * Dummy future created to trigger reassignments if partition
     * topology changed while preloading.
     *
     * @param cctx Cache context.
     * @param discoEvt Discovery event.
     */
    public GridDhtPartitionsExchangeFuture(GridCacheContext<K, V> cctx, GridDiscoveryEvent discoEvt) {
        super(cctx.kernalContext());
        dummy = true;

        this.discoEvt = discoEvt;

        syncNotify(true);

        onDone();
    }

    /**
     * @param cctx Cache context.
     * @param busyLock Busy lock.
     * @param exchId Exchange ID.
     */
    GridDhtPartitionsExchangeFuture(GridCacheContext<K, V> cctx, ReadWriteLock busyLock,
        GridDhtPartitionExchangeId exchId) {
        super(cctx.kernalContext());

        syncNotify(true);

        assert busyLock != null;
        assert exchId != null;

        dummy = false;

        this.cctx = cctx;
        this.busyLock = busyLock;
        this.exchId = exchId;

        log = cctx.logger(getClass());

        top = cctx.dht().topology();

        GridRichNode loc = cctx.localNode();

        // In case of join, only wait for nodes with smaller or equal version
        // to the joining node.
        Collection<GridRichNode> allNodes = new LinkedList<GridRichNode>(
            exchId.isJoined() ? CU.allNodes(cctx, exchId.order()) : CU.allNodes(cctx));

        oldestNode = F.isEmpty(allNodes) ? loc : CU.oldest(allNodes);

        assert oldestNode != null;

        rmtNodes = F.view(allNodes, F.remoteNodes(loc.id()));

        rmtIds = new HashSet<UUID>(F.viewReadOnly(rmtNodes, F.node2id()));

        initFut = new GridFutureAdapter<Boolean>(ctx, true);

        addWatch(cctx.stopwatch("EXCHANGE_PARTITIONS"));

        if (log.isDebugEnabled())
            log.debug("Creating exchange future [cacheName=" + cctx.namex() + ", localNode=" + cctx.nodeId() +
                ", fut=" + this + ']');
    }

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridDhtPartitionsExchangeFuture() {
        assert false;

        dummy = true;
    }

    /**
     * @return Dummy flag.
     */
    boolean dummy() {
        return dummy;
    }

    /**
     * @return {@code True}
     */
    boolean onAdded() {
        return added.compareAndSet(false, true);
    }

    /**
     * Event callback.
     *
     * @param exchId Exchange ID.
     * @param discoEvt Discovery event.
     */
    void onEvent(GridDhtPartitionExchangeId exchId, GridDiscoveryEvent discoEvt) {
        assert exchId.equals(this.exchId);

        this.discoEvt = discoEvt;

        evtLatch.countDown();
    }

    /**
     * @return Discovery event.
     */
    GridDiscoveryEvent discoveryEvent() {
        return discoEvt;
    }

    /**
     * @return Exchange id.
     */
    GridDhtPartitionExchangeId key() {
        return exchId;
    }

    /**
     * @return Oldest node.
     */
    GridNode oldestNode() {
        return oldestNode;
    }

    /**
     * @return Cause node id.
     */
    UUID causeNodeId() {
        return exchId.nodeId();
    }

    /**
     * @return {@code True} if exchange is due to local node event.
     */
    boolean local() {
        return causeNodeId().equals(cctx.nodeId());
    }

    /**
     * @return Cause node order.
     */
    long causeNodeOrder() {
        return exchId.order();
    }

    /**
     * @return Discovery event type.
     */
    int event() {
        return exchId.event();
    }

    /**
     * @return Exchange ID.
     */
    GridDhtPartitionExchangeId exchangeId() {
        return exchId;
    }

    /**
     * @return Init future.
     */
    GridFuture<?> initFuture() {
        return initFut;
    }

    /**
     * @return {@code true} if entered to busy state.
     */
    private boolean enterBusy() {
        if (busyLock.readLock().tryLock())
            return true;

        if (log.isDebugEnabled())
            log.debug("Failed to enter busy state (exchanger is stopping): " + this);

        return false;
    }

    /**
     *
     */
    private void leaveBusy() {
        busyLock.readLock().unlock();
    }

    /**
     * @return Init flag.
     */
    boolean isInit() {
        return init.get();
    }

    /**
     * Starts activity.
     *
     * @throws GridInterruptedException If interrupted.
     */
    void init() throws GridInterruptedException {
        assert oldestNode != null;

        if (init.compareAndSet(false, true)) {
            if (isDone())
                return;

            try {
                // Wait for event to occur to make sure that discovery
                // will return corresponding nodes.
                U.await(evtLatch);

                assert discoEvt != null;

                // Only wait in case of join event. If a node leaves,
                // then transactions handled by it will be either remapped
                // or invalidated.
                if (exchId.isJoined()) {
                    GridNode node = cctx.node(exchId.nodeId());

                    if (node == null) {
                        if (log.isDebugEnabled())
                            log.debug("Joined node left before exchange completed (nothing to do): " + this);

                        onDone();

                        return;
                    }

                    Set<Integer> parts = cctx.primaryPartitions(node, null);

                    cctx.partitionReleaseFuture(parts, exchId.nodeId()).get();
                }

                top.beforeExchange(exchId);
            }
            catch (GridInterruptedException e) {
                onDone(e);

                throw e;
            }
            catch (GridException e) {
                U.error(log, "Failed to reinitialize local partitions (preloading will be stopped): " + exchId, e);

                onDone(e);

                return;
            }

            if (F.isEmpty(rmtIds)) {
                onDone();

                return;
            }

            ready.set(true);

            initFut.onDone(true);

            if (log.isDebugEnabled())
                log.debug("Initialized future: " + this);

            // If this node is not oldest.
            if (!oldestNode.id().equals(cctx.nodeId()))
                sendPartitions();
            else if (allReceived() && replied.compareAndSet(false, true)) {
                if (spreadPartitions(top.partitionMap(true)))
                    onDone();
            }

            scheduleRecheck();

            watch.step("EXCHANGE_STARTED");
        }
    }

    /**
     * @param node Node.
     * @param id ID.
     * @throws GridException If failed.
     */
    private void sendLocalPartitions(GridNode node, @Nullable GridDhtPartitionExchangeId id) throws GridException {
        GridDhtPartitionsSingleMessage<K, V> m = new GridDhtPartitionsSingleMessage<K, V>(id, top.localPartitionMap());

        if (log.isDebugEnabled())
            log.debug("Sending local partitions [nodeId=" + node.id() + ", exchId=" + exchId + ", msg=" + m + ']');

        cctx.io().send(node, m);
    }

    /**
     * @param nodes Nodes.
     * @param id ID.
     * @param partMap Partition map.
     * @throws GridException If failed.
     */
    private void sendAllPartitions(Collection<? extends GridNode> nodes, GridDhtPartitionExchangeId id,
        GridDhtPartitionFullMap partMap) throws GridException {
        GridDhtPartitionsFullMessage<K, V> m = new GridDhtPartitionsFullMessage<K, V>(id, partMap);

        if (log.isDebugEnabled())
            log.debug("Sending full partition map [nodeIds=" + F.viewReadOnly(nodes, F.node2id()) +
                ", exchId=" + exchId + ", msg=" + m + ']');

        cctx.io().safeSend(nodes, m, null);
    }

    /**
     *
     */
    private void sendPartitions() {
        GridNode oldestNode = this.oldestNode;

        try {
            sendLocalPartitions(oldestNode, exchId);
        }
        catch (GridTopologyException ignore) {
            if (log.isDebugEnabled())
                log.debug("Oldest node left during partition exchange [nodeId=" + oldestNode.id() +
                    ", exchId=" + exchId + ']');
        }
        catch (GridException e) {
            scheduleRecheck();

            U.error(log, "Failed to send local partitions to oldest node (will retry after timeout) [oldestNodeId=" +
                oldestNode.id() + ", exchId=" + exchId + ']', e);
        }
    }

    /**
     * @param partMap Partition map.
     * @return {@code True} if succeeded.
     */
    private boolean spreadPartitions(GridDhtPartitionFullMap partMap) {
        try {
            sendAllPartitions(rmtNodes, exchId, partMap);

            return true;
        }
        catch (GridException e) {
            scheduleRecheck();

            U.error(log, "Failed to send full partition map to nodes (will retry after timeout) [nodes=" + rmtIds +
                ", exchangeId=" + exchId + ']', e);

            return false;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(Object res, Throwable err) {
        if (super.onDone(res, err) && !dummy) {
            if (log.isDebugEnabled())
                log.debug("Completed partition exchange [localNode=" + cctx.nodeId() + ", exchange= " + this + ']');

            initFut.onDone(err == null);

            GridTimeoutObject timeoutObj = this.timeoutObj;

            // Deschedule timeout object.
            if (timeoutObj != null)
                cctx.time().removeTimeoutObject(timeoutObj);

            return true;
        }

        return dummy;
    }

    /**
     * @return {@code True} if all replies are received.
     */
    private boolean allReceived() {
        return rcvdIds.containsAll(rmtIds);
    }

    /**
     * @param nodeId Sender node id.
     * @param msg Single partition info.
     */
    void onReceive(UUID nodeId, GridDhtPartitionsSingleMessage<K, V> msg) {
        assert msg != null;

        assert msg.exchangeId().equals(exchId);

        if (isDone()) {
            if (log.isDebugEnabled())
                log.debug("Received message for finished future (will reply only to sender) [msg=" + msg +
                    ", fut=" + this + ']');

            try {
                GridNode n = cctx.node(nodeId);

                if (n != null)
                    sendAllPartitions(F.asList(n), exchId, top.partitionMap(true));
            }
            catch (GridException e) {
                scheduleRecheck();

                U.error(log, "Failed to send full partition map to nodes (will retry after timeout) [nodes=" + rmtIds +
                    ", exchangeId=" + exchId + ']', e);
            }
        }
        else {
            rcvdIds.add(nodeId);

            top.update(exchId, msg.partitions());

            // If got all replies, and initialization finished, and reply has not been sent yet.
            if (allReceived() && ready.get() && replied.compareAndSet(false, true)) {
                spreadPartitions(top.partitionMap(true));

                onDone();
            }
            else if (log.isDebugEnabled())
                log.debug("Exchange future full map is not sent [allReceived=" + allReceived() + ", ready=" + ready +
                    ", replied=" + replied.get() + ", init=" + init.get() + ", fut=" + this + ']');
        }
    }

    /**
     * @param nodeId Sender node ID.
     * @param msg Full partition info.
     */
    void onReceive(UUID nodeId, GridDhtPartitionsFullMessage<K, V> msg) {
        assert msg != null;

        if (isDone()) {
            if (log.isDebugEnabled())
                log.debug("Received message for finished future [msg=" + msg + ", fut=" + this + ']');

            return;
        }

        if (!nodeId.equals(oldestNode.id())) {
            if (log.isDebugEnabled())
                log.debug("Received full partition map from unexpected node [oldest=" + oldestNode.id() +
                    ", unexpectedNodeId=" + nodeId + ']');

            return;
        }

        assert msg.exchangeId().equals(exchId);

        if (log.isDebugEnabled())
            log.debug("Received full partition map from node [nodeId=" + nodeId + ", msg=" + msg + ']');

        top.update(exchId, msg.partitions());

        onDone();
    }

    /**
     * @param nodeId Left node id.
     */
    public void onNodeLeft(final UUID nodeId) {
        if (isDone())
            return;

        if (!enterBusy())
            return;

        try {
            // Wait for initialization part of this future to complete.
            initFut.listenAsync(new CI1<GridFuture<?>>() {
                @Override public void apply(GridFuture<?> f) {
                    if (f.isDone())
                        return;

                    if (!enterBusy())
                        return;

                    try {
                        if (oldestNode.id().equals(nodeId)) {
                            if (log.isDebugEnabled())
                                log.debug("Oldest node left or failed on partition exchange " +
                                    "(will restart exchange process)) [cacheName=" + cctx.namex() +
                                    ", oldestNodeId=" + oldestNode.id() + ", exchangeId=" + exchId + ']');

                            oldestNode = CU.oldest(CU.allNodes(cctx));

                            // Pretend to have received message from this node.
                            rcvdIds.add(nodeId);

                            // Reassign oldest node and resend.
                            recheck();
                        }
                        else if (rmtIds.contains(nodeId)) {
                            if (log.isDebugEnabled())
                                log.debug("Remote node left of failed during partition exchange (will ignore) " +
                                    "[rmtNode=" + nodeId + ", exchangeId=" + exchId + ']');

                            // Pretend to have received message from this node.
                            rcvdIds.add(nodeId);

                            if (allReceived() && ready.get() && replied.compareAndSet(false, true))
                                if (spreadPartitions(top.partitionMap(true)))
                                    onDone();
                        }
                    }
                    finally {
                        leaveBusy();
                    }
                }
            });
        }
        finally {
            leaveBusy();
        }
    }

    /**
     *
     */
    private void recheck() {
        // If this is the oldest node.
        if (oldestNode.id().equals(cctx.nodeId())) {
            Collection<UUID> remaining = remaining();

            if (!remaining.isEmpty()) {
                try {
                    cctx.io().safeSend(cctx.discovery().nodes(remaining),
                        new GridDhtPartitionsSingleRequest<K, V>(exchId), null);
                }
                catch (GridException e) {
                    U.error(log, "Failed to request partitions from nodes [exchangeId=" + exchId +
                        ", nodes=" + remaining + ']', e);
                }
            }
            // Resend full partition map because last attempt failed.
            else {
                if (spreadPartitions(top.partitionMap(true)))
                    onDone();
            }
        }
        else
            sendPartitions();

        // Schedule another send.
        scheduleRecheck();
    }

    /**
     *
     */
    private void scheduleRecheck() {
        if (!isDone()) {
            GridTimeoutObject old = timeoutObj;

            if (old != null)
                cctx.time().removeTimeoutObject(old);

            GridTimeoutObject timeoutObj = new GridTimeoutObject() {
                /** */
                private final UUID timeoutId = UUID.randomUUID();

                /** */
                private final long startTime = System.currentTimeMillis();

                @Override public UUID timeoutId() {
                    return timeoutId;
                }

                @Override public long endTime() {
                    return cctx.gridConfig().getNetworkTimeout() * cctx.gridConfig().getCacheConfiguration().length +
                        startTime;
                }

                @Override public void onTimeout() {
                    if (isDone())
                        return;

                    if (!enterBusy())
                        return;

                    try {
                        U.warn(log,
                            "Retrying preload partition exchange due to timeout: " +
                                GridDhtPartitionsExchangeFuture.this,
                            "Retrying preload partition exchange due to timeout ...");

                        recheck();
                    }
                    finally {
                        leaveBusy();
                    }
                }
            };

            this.timeoutObj = timeoutObj;

            cctx.time().addTimeoutObject(timeoutObj);
        }
    }

    /**
     * @return Remaining node IDs.
     */
    Collection<UUID> remaining() {
        return F.lose(rmtIds, true, rcvdIds);
    }

    /** {@inheritDoc} */
    @Override public int compareTo(GridDhtPartitionsExchangeFuture<K, V> fut) {
        return exchId.compareTo(fut.exchId);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        GridDhtPartitionsExchangeFuture fut = (GridDhtPartitionsExchangeFuture)o;

        return exchId.equals(fut.exchId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return exchId.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionsExchangeFuture.class, this,
            "oldest", oldestNode == null ? "null" : oldestNode.id(),
            "oldestOrder", oldestNode == null ? "null" : oldestNode.order(),
            "evtLatch", evtLatch == null ? "null" : evtLatch.getCount(),
            "super", super.toString());
    }
}
