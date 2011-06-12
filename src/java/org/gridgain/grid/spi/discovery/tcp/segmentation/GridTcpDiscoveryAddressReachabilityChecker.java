// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.tcp.segmentation;

import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.discovery.tcp.*;

import java.net.*;

/**
 * Address reachability checker interface for {@link GridTcpDiscoverySpi}.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.1.1c.12062011
 */
public interface GridTcpDiscoveryAddressReachabilityChecker {
    /**
     * Checks whether provided address is reachable from the specified interface
     * within the specified timeout.
     *
     * @param addr Address to check reachability of.
     * @param itf Network interface to use.
     * @param ttl TTL to use.
     * @param timeout Timeout to use.
     * @return {@code True} if address is reachable, {@code false} otherwise.
     * @throws GridSpiException If an error occurs.
     */
    public boolean isReachable(InetAddress addr, NetworkInterface itf, int ttl, int timeout) throws GridSpiException;
}
