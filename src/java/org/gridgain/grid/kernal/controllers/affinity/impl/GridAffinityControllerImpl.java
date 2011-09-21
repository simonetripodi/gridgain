// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.controllers.affinity.impl;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.controllers.*;
import org.gridgain.grid.kernal.controllers.affinity.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.lang.utils.*;
import org.gridgain.grid.typedef.*;
import org.gridgain.grid.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.GridClosureCallMode.*;
import static org.gridgain.grid.GridEventType.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.kernal.controllers.affinity.impl.GridAffinityUtils.*;

/**
 * Processor responsible for getting key affinity nodes.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.5.0c.20092011
 */
public class GridAffinityControllerImpl extends GridControllerAdapter implements GridAffinityController {
    /** Retries to get affinity in case of error. */
    private static final int ERROR_RETRIES = 3;

    /** Time to wait between errors (in milliseconds). */
    private static final long ERROR_WAIT = 500;

    /** Null cache name. */
    private static final String NULL_NAME = UUID.randomUUID().toString();

    /** Affinity map. */
    private final ConcurrentMap<String, GridTuple3<GridCacheAffinity, GridException, CountDownLatch>> affMap =
        new ConcurrentHashMap<String, GridTuple3<GridCacheAffinity, GridException, CountDownLatch>>();

    /** Listener. */
    private final GridLocalEventListener lsnr = new GridLocalEventListener() {
        @Override public void onEvent(GridEvent evt) {
            assert evt.type() == EVT_NODE_FAILED || evt.type() == EVT_NODE_LEFT;

            for (Iterator<String> it = affMap.keySet().iterator(); it.hasNext();) {
                String cacheName = unmaskNull(it.next());

                boolean found = false;

                for (GridNode n : ctx.discovery().allNodes()) {
                    if (U.hasCache(n, cacheName)) {
                        found = true;

                        break;
                    }
                }

                if (!found)
                    it.remove();
            }
        }
    };

    /**
     * @param ctx Context.
     */
    public GridAffinityControllerImpl(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws GridException {
        ctx.event().addLocalEventListener(lsnr, EVT_NODE_FAILED, EVT_NODE_LEFT);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel, boolean wait) {
        if (ctx != null && ctx.event() != null)
            ctx.event().removeLocalEventListener(lsnr);
    }

    /** {@inheritDoc} */
    @Override public <K> Map<GridRichNode, Collection<K>> mapKeysToNodes(String cacheName, Collection<GridRichNode> nodes,
        @Nullable Collection<? extends K> keys, boolean sys) throws GridException {
        return keysToNodes(cacheName, keys, nodes, sys);
    }

    /** {@inheritDoc} */
    @Override public <K> Map<GridRichNode, Collection<K>> mapKeysToNodes(Collection<GridRichNode> nodes,
        @Nullable Collection<? extends K> keys, boolean sys) throws GridException {
        return keysToNodes(null, keys, nodes, sys);
    }

    /** {@inheritDoc} */
    @Override @Nullable public <K> GridRichNode mapKeyToNode(String cacheName, Collection<GridRichNode> nodes, K key,
        boolean sys) throws GridException {
        Map<GridRichNode, Collection<K>> map = keysToNodes(cacheName, F.asList(key), nodes, sys);

        return map != null ? F.first(map.keySet()) : null;
    }

    /** {@inheritDoc} */
    @Override @Nullable public <K> GridRichNode mapKeyToNode(Collection<GridRichNode> nodes, K key, boolean sys)
        throws GridException {
        Map<GridRichNode, Collection<K>> map = keysToNodes(null, F.asList(key), nodes, sys);

        return map != null ? F.first(map.keySet()) : null;
    }

    /**
     * @param cacheName Cache name.
     * @return Non-null cache name.
     */
    private String maskNull(@Nullable String cacheName) {
        return cacheName == null ? NULL_NAME : cacheName;
    }

    /**
     * @param cacheName Cache name.
     * @return Unmasked cache name.
     */
    @Nullable private String unmaskNull(String cacheName) {
        return NULL_NAME.equals(cacheName) ? null : cacheName;
    }

    /**
     * @param cacheName Cache name.
     * @param keys Keys.
     * @param nodes Nodes.
     * @param sys If {@code true}, request will be performed on system pool.
     * @return Affinity map.
     * @throws GridException If failed.
     */
    @SuppressWarnings({"unchecked"})
    private <K> Map<GridRichNode, Collection<K>> keysToNodes(@Nullable final String cacheName, Collection<? extends K> keys,
        Collection<GridRichNode> nodes, boolean sys) throws GridException {
        if (F.isEmpty(keys) || F.isEmpty(nodes))
            return Collections.emptyMap();

        GridTuple3<GridCacheAffinity, GridException, CountDownLatch> tup = affMap.get(maskNull(cacheName));

        if (tup != null && tup.get1() != null && tup.get3().getCount() == 0)
            return affinityMap(cacheName, tup.get1(), keys, nodes);

        GridRichNode loc = ctx.rich().rich(ctx.discovery().localNode());

        // Check local node.
        if (U.hasCache(loc, cacheName)) {
            // Map all keys to local node for local caches.
            if (ctx.cache().cache(cacheName).configuration().getCacheMode() == LOCAL)
                return F.asMap(loc, (Collection<K>)keys);

            GridCacheAffinity<K> a = ctx.cache().cache(cacheName).configuration().getAffinity();

            affMap.put(maskNull(cacheName), F.t((GridCacheAffinity)a, (GridException)null, new CountDownLatch(0)));

            return affinityMap(cacheName, a, keys, nodes);
        }

        // In Community Edition we always return null if
        // cache is not available locally.
        if (!U.isEnterprise())
            return null;

        Collection<GridNode> cacheNodes = F.view(ctx.discovery().remoteNodes(), new P1<GridNode>() {
            @Override public boolean apply(GridNode n) {
                return U.hasCache(n, cacheName);
            }
        });

        if (F.isEmpty(cacheNodes))
            return Collections.emptyMap();

        GridTuple3<GridCacheAffinity, GridException, CountDownLatch> old =
            affMap.putIfAbsent(maskNull(cacheName), tup = F.t((GridCacheAffinity)null, null, new CountDownLatch(1)));

        if (old != null) {
            U.await(old.get3());

            if (old.get2() != null)
                throw old.get2();

            if (old.get1() != null)
                return affinityMap(cacheName, old.get1(), keys, nodes);
        }

        int max = ERROR_RETRIES;
        int cnt = 0;

        Iterator<GridNode> it = cacheNodes.iterator();

        // We are here because affinity has not been fetched yet, or cache is local.
        while (true) {
            cnt++;

            if (!it.hasNext())
                it = cacheNodes.iterator();

            GridNode n = it.next();

            GridCacheMode mode = U.cacheMode(n, cacheName);

            assert mode != null;

            // Map all keys to a single node, if the cache mode is LOCAL.
            if (mode == LOCAL) {
                tup.get3().countDown();

                return F.asMap(ctx.rich().rich(n), (Collection<K>)keys);
            }

            try {
                GridCacheAffinity a = ctx.closure().callAsync(UNICAST, affinityJob(cacheName), F.asList(n),
                    /*system pool*/sys).get();

                assert a != null;

                ctx.resource().injectGeneric(a);

                // Bring to initial state.
                a.reset();

                // Set affinity before counting down on latch.
                tup.set1(a);

                tup.get3().countDown();

                break;
            }
            catch (GridException e) {
                if (e instanceof GridExecutionRejectedException || cnt == max && ctx.discovery().node(n.id()) != null) {
                    affMap.remove(maskNull(cacheName));

                    tup.set2(new GridException("Failed to get affinity mapping from node: " + n, e));

                    // Failed... no point to wait any longer.
                    tup.get3().countDown();

                    throw tup.get2();
                }

                if (log.isDebugEnabled())
                    log.debug("Failed to get affinity from node (will retry) [cache=" + cacheName +
                        ", node=" + U.toShortString(n) + ']');

                U.sleep(ERROR_WAIT);
            }
        }

        return affinityMap(cacheName, tup.get1(), keys, nodes);
    }

    /**
     * @param cacheName Cache name.
     * @param aff Affinity.
     * @param keys Keys.
     * @param nodes Nodes.
     * @return Affinity map.
     * @throws GridException If failed.
     */
    @SuppressWarnings({"unchecked"})
    private <K> Map<GridRichNode, Collection<K>> affinityMap(final String cacheName, GridCacheAffinity<K> aff,
        Collection<? extends K> keys, Collection<GridRichNode> nodes) throws GridException {
        assert !F.isEmpty(keys);

        try {
            nodes = F.view(nodes, new P1<GridRichNode>() {
                @Override public boolean apply(GridRichNode n) {
                    return U.hasCache(n, cacheName);
                }
            });

            if (keys.size() == 1)
                return Collections.singletonMap(affinityNode(aff, F.first(keys), nodes), (Collection<K>)keys);

            Map<GridRichNode, Collection<K>> map = new GridLeanMap<GridRichNode, Collection<K>>(nodes.size());

            for (K k : keys) {
                int part = aff.partition(k);

                GridRichNode n = F.first(aff.nodes(part, nodes));

                if (n == null)
                    throw new GridException("Failed to map keys to any node: " + keys);

                Collection<K> mapped = map.get(n);

                if (mapped == null)
                    map.put(n, mapped = new LinkedList<K>());

                mapped.add(k);
            }

            return map;
        }
        catch (Throwable e) {
            throw new GridException("Failed to get affinity map for keys: " + keys, e);
        }
    }

    /**
     * @param aff Affinity.
     * @param key Key.
     * @param nodes Nodes.
     * @return Affinity node ID.
     * @throws GridTopologyException If topology is empty.
     */
    private <K> GridRichNode affinityNode(GridCacheAffinity<K> aff, K key, Collection<GridRichNode> nodes)
        throws GridTopologyException {
        assert key != null;

        GridRichNode n = F.first(aff.nodes(aff.partition(key), nodes));

        if (n == null)
            throw new GridTopologyException("Key affinity cannot be determined (topology is empty): " + key);

        return n;
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>>");
        X.println(">>> Affinity controller memory stats [grid=" + ctx.gridName() + ']');
        X.println(">>>   affMapSize: " + affMap.size());
    }
}
