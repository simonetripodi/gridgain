// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.cloud;

import org.gridgain.grid.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.cloud.jvm.*;
import org.gridgain.grid.typedef.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.GridClosureCallMode.*;
import static org.gridgain.grid.GridEventType.*;

/**
 * Demonstrates some features of the cloud API.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.1.1c.17062011
 */
public class GridCloudExample {
    /** Cloud ID. */
    private static final String CLOUD_ID = "jvm-cloud";

    /** Cloud node count. */
    private static final int NODE_CNT = 1;

    /**
     * Ensures singleton.
     */
    private GridCloudExample() {
        // No-op.
    }

    /**
     * Starts grid with configured cloud, start nodes on cloud,
     * print hello message on each cloud node, stops each cloud node
     * and then stop grid.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        GridConfigurationAdapter cfg = new GridConfigurationAdapter();

        GridJvmCloudSpi cloudSpi = new GridJvmCloudSpi();

        cloudSpi.setCloudId(CLOUD_ID);

        // Manually configure cloud SPI.
        cfg.setCloudSpi(cloudSpi);
        cfg.setDisableCloudCoordinator(false);

        try {
            Grid g = G.start(cfg);

            // Cloud resource event latch.
            final CountDownLatch added = new CountDownLatch(NODE_CNT);
            final CountDownLatch removed = new CountDownLatch(NODE_CNT);

            // Cloud event listener.
            GridLocalEventListener lsnr = new GridLocalEventListener() {
                @Override public void onEvent(GridEvent evt) {
                    assert evt instanceof GridCloudEvent;

                    X.println("Received grid cloud event: " + evt);

                    if (evt.type() == EVT_CLOUD_RESOURCE_ADDED)
                        added.countDown();
                    else if(evt.type() == EVT_CLOUD_RESOURCE_REMOVED)
                        removed.countDown();
                    else
                        assert false : "Unexpected event type: " + evt.type();
                }
            };

            g.addLocalEventListener(lsnr, EVT_CLOUD_RESOURCE_ADDED, EVT_CLOUD_RESOURCE_REMOVED);

            GridRichCloud cloud = g.cloud(CLOUD_ID);

            // Checks empty cloud.
            assert cloud.resources().isEmpty() :
                "Unexpected resource count [count=" + cloud.resources().size() + ", expected=0]";

            // Invokes command to start nodes.
            cloud.invoke(new GridCloudCommandAdapter("GridCloudExample.main", GridJvmCloudSpi.START_NODES_ACT,
                NODE_CNT));

            // Waits for started nodes.
            added.await();

            // Print hello message on each cloud node.
            cloud.run(BROADCAST, new Runnable() {
                @GridLocalNodeIdResource
                private UUID id;

                @Override public void run() {
                    X.println(">>>>>");
                    X.println("Hello Cloud Node [id=" + id + ']');
                    X.println(">>>>>");
                }
            });

            // Invokes command to stop nodes.
            cloud.invoke(new GridCloudCommandAdapter("GridCloudExample.main", GridJvmCloudSpi.STOP_CLOUD_ACT));

            // Waits fot stopped nodes.
            removed.await();

            g.removeLocalEventListener(lsnr, EVT_CLOUD_RESOURCE_ADDED, EVT_CLOUD_RESOURCE_REMOVED);
        }
        finally {
            G.stopAll(false);
        }
    }
}
