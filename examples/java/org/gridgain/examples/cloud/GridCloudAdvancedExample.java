// @java.file.header

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
import org.gridgain.grid.strategies.*;
import org.gridgain.grid.typedef.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.GridClosureCallMode.*;
import static org.gridgain.grid.GridEventType.*;

/**
 * Demonstrates some features of the cloud API.
 * <p>
 * Note that this example requires working method of attaining accurate CPU load. JDK
 * implementation is proven to be buggy (as of Java 6 on various OSs). We highly recommend
 * to use Sigar GPL-based library that is supported by GridGain (license permitting). This
 * example will not properly work if CPU load cannot be accurately obtained.
 * <p>
 * To use Sigar library simply put its JARs and native libraries into <code>$GRIDGAIN_HOME/libs</code>
 * folder and restart the example.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridCloudAdvancedExample {
    /** Cloud ID. */
    private static final String CLOUD_ID = "jvm-cloud";

    /** Cloud nodes count determines how many nodes to start in cloud. */
    private static final int NODES_TO_START = 1;

    /** Threads that will load CPUs, two per each */
    private static Thread threads[] = new Thread[Runtime.getRuntime().availableProcessors() * 2];

    /**
     * Ensures singleton.
     */
    private GridCloudAdvancedExample() {
        // No-op.
    }

    /**
     * Starts grid with configured cloud, starts CPU loader thread.
     * When strategy determines that CPU load is too high (configured), it adds nodes to cloud.
     *
     * Then we print hello message on each cloud node, stop entire cloud
     * and then stop grid.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        // Detect if Sigar is available in classpath.
        try {
            Class.forName("org.hyperic.sigar.Sigar");
        }
        catch (ClassNotFoundException ignored) {
            X.println(
                ">>> Sigar library is not found in the classpath.",
                ">>> This example requires working method of attaining accurate CPU load.",
                ">>> Sigar is not shipped with enterprise edition of GridGain due to licence limitations.",
                ">>> Please download Sigar GPL-based library from http://www.hyperic.com/products/sigar",
                ">>> and put its JARs and native libraries into $GRIDGAIN_HOME/libs.");

            return;
        }

        GridConfiguration cfg = createConfiguration();

        // Cloud resource event latch.
        final CountDownLatch added = new CountDownLatch(NODES_TO_START);
        final CountDownLatch removed = new CountDownLatch(NODES_TO_START);

        try {
            Grid g = G.start(cfg);

            // Cloud event listener.
            GridLocalEventListener lsnr = new GridLocalEventListener() {
                @Override public void onEvent(GridEvent evt) {
                    assert evt instanceof GridCloudEvent;

                    if (evt.type() == EVT_CLOUD_RESOURCE_ADDED) {
                        added.countDown();

                        X.println("", ">>>>>", "Cloud resource added.", ">>>>>");
                    }
                    else if(evt.type() == EVT_CLOUD_RESOURCE_REMOVED) {
                        removed.countDown();

                        X.println("", ">>>>>", "Cloud resource removed.", ">>>>>");
                    }
                    else
                        assert false : "Unexpected event type [type=" + evt.type() +']';
                }
            };

            g.addLocalEventListener(lsnr, EVT_CLOUD_RESOURCE_ADDED, EVT_CLOUD_RESOURCE_REMOVED);

            // Load CPU
            loadCPUs();

            // Waits for started nodes.
            added.await();

            GridRichCloud cloud = g.cloud(CLOUD_ID);

            // Print hello message on each cloud node.
            cloud.run(BROADCAST, new Runnable() {
                @GridLocalNodeIdResource
                private UUID id;

                @Override public void run() {
                    X.println("", ">>>>>", "Hello Cloud Node [id=" + id + ']', ">>>>>");
                }
            });

            // Free CPU, no output suppression.
            freeCPUs(false);

            // Waits fot stopped nodes.
            removed.await();

            g.removeLocalEventListener(lsnr, EVT_CLOUD_RESOURCE_ADDED, EVT_CLOUD_RESOURCE_REMOVED);
        }
        finally {
            freeCPUs(true);

            G.stopAll(false);
        }
    }

    /**
     * Creates and starts loader threads for this machine (two threads per each CPU).
     */
    private static void loadCPUs() {
        X.println("", ">>>>>", "Loading CPUs.", ">>>>>");

        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread() {
                @Override public void run() {
                    while (!isInterrupted()) { /* No-op. */ }
                }
            };

            threads[i].start();
        }
    }

    /**
     * Frees CPUs by interrupting each the loader threads.
     *
     * @param quiet If {@code true} output will be suppressed.
     */
    private static void freeCPUs(boolean quiet) {
        if (!quiet)
            X.println("", ">>>>>", "Freeing CPU.", ">>>>>");

        for (Thread thread : threads)
            thread.interrupt();
    }

    /**
     * Creates configuration adapter, configures cloud and strategy.
     *
     * @return Grid configuration.
     */
    private static GridConfiguration createConfiguration() {
        GridConfigurationAdapter cfg = new GridConfigurationAdapter();

        UUID locNodeId = UUID.randomUUID();

        cfg.setNodeId(locNodeId);

        // Manually configure cloud SPI.
        GridJvmCloudSpi cloudSpi = new GridJvmCloudSpi();

        cloudSpi.setCloudId(CLOUD_ID);

        cfg.setCloudSpi(cloudSpi);
        cfg.setDisableCloudCoordinator(false);

        // Create CPU strategy to add cloud nodes if CPU is overloaded
        GridCloudCpuStrategyAdapter cpuSgy = new GridCloudCpuStrategyAdapter() {
            @Override public void onCpuAboveMax(double cpuAvg) {
                X.println("", ">>>>>", "CPU load above max, adding nodes to cloud...", ">>>>>");

                GridRichCloud cloud = grid.cloud(CLOUD_ID);

                try {
                    // Invokes command to start nodes.
                    cloud.invoke(new GridCloudCommandAdapter("1", GridJvmCloudSpi.START_NODES_ACT, NODES_TO_START));
                }
                catch (GridException ex) {
                    log.error("Error while invoking cloud command.", ex);
                }
            }

            @Override public void onCpuBelowMin(double cpuAvg) {
                X.println("", ">>>>>", "CPU load below min, stopping cloud nodes...", ">>>>>");

                GridRichCloud cloud = grid.cloud(CLOUD_ID);

                try {
                    // Invokes command to stop nodes.
                    cloud.invoke(new GridCloudCommandAdapter("2", GridJvmCloudSpi.STOP_CLOUD_ACT));
                }
                catch (GridException ex) {
                    log.error("Error while invoking cloud command.", ex);
                }
            }
        };

        // Monitor only local node.
        cpuSgy.setPredicates(F.<GridRichNode>localNode(locNodeId));

        // Configure CPU strategy.
        cpuSgy.setFrequency(3000);
        cpuSgy.setMinCpu(0.49f);
        cpuSgy.setMaxCpu(0.50f);
        cpuSgy.setSampleNumber(3);

        // This is a critical setting for this algorithm as it ensures
        // that nodes in the cloud will be started only once (per this
        // algorithm).
        cpuSgy.setOncePerThreshold(true);

        cfg.setCloudStrategies(new GridCloudStrategy[] {cpuSgy});

        return cfg;
    }
}
