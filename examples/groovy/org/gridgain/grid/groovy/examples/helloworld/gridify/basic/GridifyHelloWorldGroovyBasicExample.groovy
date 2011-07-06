// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.groovy.examples.helloworld.gridify.basic

import org.gridgain.grid.*
import org.gridgain.grid.gridify.*

/**
 * <img id="callout_img" src="{@docRoot}/img/callout_blue.gif"><span id="callout_blue">Start Here</span>&nbsp;This
 * example demonstrates a simple use of GridGain grid in Groovy with
 * <tt>Gridify</tt> annotation.
 * <p>
 * String "Hello, World!" is passed as an argument to
 * <tt>sayIt(String)</tt> method. Since this method is annotated with
 * <tt>Gridify</tt> annotation it is automatically grid-enabled and
 * will be executed on remote node. Note, that the only thing user had
 * to do is annotate method <tt>sayIt(String)</tt> with {@link Gridify}
 * annotation, everything else is taken care of by the system.
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
 * <p>
 * <h1 class="header">AOP Configuration</h1>
 * In order for this example to execute on the grid, any of the following
 * AOP configurations must be provided (only on the task initiating node).
 * <h2 class="header">Jboss AOP</h2>
 * The following configuration needs to be applied to enable JBoss byte code
 * weaving. Note that GridGain is not shipped with JBoss and necessary
 * libraries will have to be downloaded separately (they come standard
 * if you have JBoss installed already):
 * <ul>
 * <li>
 *      The following JVM configuration must be present:
 *      <ul>
 *      <li><tt>-javaagent:[path to jboss-aop-jdk50-4.x.x.jar]</tt></li>
 *      <li><tt>-Djboss.aop.class.path=[path to gridgain.jar]</tt></li>
 *      <li><tt>-Djboss.aop.exclude=org,com -Djboss.aop.include=org.gridgain.examples</tt></li>
 *      </ul>
 * </li>
 * <li>
 *      The following JARs should be in a classpath:
 *      <ul>
 *      <li><tt>javassist-3.x.x.jar</tt></li>
 *      <li><tt>jboss-aop-jdk50-4.x.x.jar</tt></li>
 *      <li><tt>jboss-aspect-library-jdk50-4.x.x.jar</tt></li>
 *      <li><tt>jboss-common-4.x.x.jar</tt></li>
 *      <li><tt>trove-1.0.2.jar</tt></li>
 *      </ul>
 * </li>
 * </ul>
 * <p>
 * <h2 class="header">AspectJ AOP</h2>
 * The following configuration needs to be applied to enable AspectJ byte code
 * weaving.
 * <ul>
 * <li>
 *      JVM configuration should include:
 *      <tt>-javaagent:[GRIDGAIN_HOME]/libs/aspectjweaver-1.6.8.jar</tt>
 * </li>
 * <li>
 *      Classpath should contain the <tt>[GRIDGAIN_HOME]/config/aop/aspectj</tt> folder.
 * </li>
 * </ul>
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.1.1c.06072011
 */
class GridifyHelloWorldGroovyBasicExample {
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
            def phraseLen = sayIt("Hello World");

            println ">>>"
            println ">>> Finished executing Gridify \"Hello World\" example with all defaults."
            println ">>> Total number of characters in the phrase is '$phraseLen'."
            println ">>> You should see print out of 'Hello World' on one of the remote nodes."
            println ">>> Check all nodes for output (this node is also part of the grid)."
            println ">>>"
        }
        finally {
            GridFactory.stop(true);
        }
    }

    /**
     * Method grid-enabled with {@link Gridify} annotation. Simply prints
     * out the argument passed in.
     * <p>
     * Note that default <tt>Gridify</tt> configuration is used, so this method
     * will be executed on remote node with the same argument.
     *
     * @param phrase Phrase string to print.
     * @return Number of characters in the phrase.
     */
    @Gridify
    def static int sayIt(String phrase) {
        // Simply print out the argument.
        println ">>>"
        println ">>> Printing '$phrase' on this node from grid-enabled method."
        println ">>>"

        return phrase.size()
    }
}
