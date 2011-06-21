// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.groovy.examples.helloworld.api

import org.gridgain.grid.*

/**
 * <img id="callout_img" src="{@docRoot}/img/callout_blue.gif"><span id="callout_blue">Start Here</span>&nbsp;This
 * example demonstrates a simple use of GridGain grid with Groovy.
 * <p>
 * String "Hello, World!" is passed as an argument for execution
 * of <tt>HelloWorld</tt> task. As an outcome, two participating
 * nodes will print out a single word from "Hello World" string.
 * One node will print out "Hello, " and the other will printout
 * "World!". If there is only one node participating, then it will
 * print out both words.
 * <p>
 * Grid task <tt>GridHelloWorldTask</tt> handles actual splitting
 * into sub-jobs, remote execution, and result aggregation
 * (see {@link GridTask}).
 * <p>
 * <h1 class="header">Starting Remote Nodes</h1>
 * To try this example you should (but don't have to) start remote grid instances.
 * You can start as many as you like by executing the following script:
 * <pre class="snippet">{GRIDGAIN_HOME}/bin/ggstart.{bat|sh}</pre>
 * Once remote instances are started, you can execute this example from
 * Eclipse, Idea, or NetBeans (or any other IDE) by simply hitting run
 * button. You will witness that all nodes discover each other and
 * some of the nodes will participate in task execution (check node
 * output).
 * <p>
 * <h1 class="header">XML Configuration</h1>
 * If no specific configuration is provided, GridGain will start with
 * all defaults. For information about GridGain default configuration
 * refer to {@link GridFactory} documentation. If you would like to
 * try out different configurations you should pass a path to Spring
 * configuration file as 1st command line argument into this example.
 * The path can be relative to <tt>GRIDGAIN_HOME</tt> environment variable.
 * You should also pass the same configuration file to all other
 * grid nodes by executing startup script as follows (you will need
 * to change the actual file name):
 * <pre class="snippet">{GRIDGAIN_HOME}/bin/ggstart.{bat|sh} examples/config/specific-config-file.xml</pre>
 * <p>
 * GridGain examples come with multiple configuration files you can try.
 * All configuration files are located under <tt>GRIDGAIN_HOME/examples/config</tt>
 * folder. You are free to try any of these configurations, but whenever
 * using 3rd party configurations, such as JBoss JMS, ActiveMQ JMS, Sun MQ JMS, or GigaSpaces,
 * make sure to download these respective products and include all the necessary
 * libraries into classpath at node startup. All these libraries are already
 * specified in commented format in <tt>GRIDGAIN_HOME/bin/setenv.{bat|sh}</tt> files
 * which get executed automatically by GridGain startup scripts. You can simply
 * uncomment the necessary classpath portions as you need.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.1.1c.21062011
 */
class GridHelloWorldGroovyExample {
    /**
     * Execute <tt>HelloWorld</tt> example grid-enabled with <tt>Gridify</tt> annotation.
     *
     * @param args Command line arguments, none required but if provided
     *      first one should point to the Spring XML configuration file. See
     *      <tt>"examples/config/"</tt> for configuration file examples.
     * @throws GridException If example execution failed.
     */
    static void main(args) {
        if (args.size() == 0) {
            GridFactory.start();
        }
        else {
            GridFactory.start(args[0]);
        }

        try {
            Grid grid = GridFactory.getGrid();

            // Execute Hello World task.
            GridTaskFuture<Integer> future = grid.execute(GridHelloWorldGroovyTask.class, "Hello World");

            // Wait for task completion.
            def phraseLen = future.get();

            println ">>>"
            println ">>> Finished executing Grid \"Hello World\" example with custom task."
            println ">>> Total number of characters in the phrase is '$phraseLen'."
            println ">>> You should see print out of 'Hello' on one node and 'World' on another node."
            println ">>> Check all nodes for output (this node is also part of the grid)."
            println ">>>"
        }
        finally {
            GridFactory.stop(true);
        }
    }
}
