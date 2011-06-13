#!/bin/sh
#
# Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html
#  _________        _____ __________________        _____
#  __  ____/___________(_)______  /__  ____/______ ____(_)_______
#  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
#  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
#  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
#
# Version: 3.1.1c.13062011
#

#
# Grid command line loader.
#

#
# Check JAVA_HOME.
#
if [ "$JAVA_HOME" = "" ]; then
    echo $0", ERROR: JAVA_HOME environment variable is not found."
    echo $0", ERROR: Please create JAVA_HOME variable pointing to location of JDK 1.5 or JDK 1.6."
    echo $0", ERROR: You can also download latest JDK at: http://java.sun.com/getjava"

    exit 1
fi

#
# Check JDK.
#
if [ ! -e "$JAVA_HOME/bin/java" ]; then
    echo $0", ERROR: The JAVA is not found in $JAVA_HOME."
    echo $0", ERROR: Please modify your script so that JAVA_HOME would point"
    echo $0", ERROR: to valid location of Java installation."

    exit 1
fi

#
# Set propery JAR name during the Ant build.
#
ANT_AUGMENTED_GGJAR=gridgain-3.1.1c.jar

#
# Set GRIDGAIN_HOME, if needed.
#
if [ "${GRIDGAIN_HOME}" = "" ]; then
    export GRIDGAIN_HOME="$(dirname $(readlink -f $0))"/..
fi

#
# Check GRIDGAIN_HOME
#
if [ ! -d "${GRIDGAIN_HOME}/config" ]; then
    echo $0", ERROR: GRIDGAIN_HOME environment variable is not found or is not valid."
    echo $0", ERROR: GRIDGAIN_HOME variable must point to GridGain installation folder."

    exit 1
fi

#
# Set GRIDGAIN_LIBS.
#
. "${GRIDGAIN_HOME}"/bin/setenv.sh

#
# Set GROOVY_LIBS for version 1.5.7
#
GROOVY_LIBS=${GROOVY_HOME}/embeddable/groovy-all-1.5.7.jar

# OS specific support.
SEPARATOR=":";

case "`uname`" in
    CYGWIN*)
        SEPARATOR=";";
        ;;
esac

CP="${GRIDGAIN_LIBS}${SEPARATOR}${GRIDGAIN_HOME}/${ANT_AUGMENTED_GGJAR}${SEPARATOR}${GROOVY_LIBS}"

if [ "$1" = "" ]; then
    CONFIG="${GRIDGAIN_HOME}/config/default-spring.xml"
else
    CONFIG="$1"
fi

#
# This variable defines necessary parameters for JMX
# monitoring and management.
# ADD YOUR ADDITIONAL PARAMETERS/OPTIONS HERE
#
JMX_MON=-Dcom.sun.management.jmxremote

#
# Uncomment this to enable remote unsecure access
# to JConsole.
# ADD YOUR ADDITIONAL PARAMETERS/OPTIONS HERE
#
# JMX_MON="${JMX_MON} -Dcom.sun.management.jmxremote.port=49112 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"

#
# JVM options. See http://java.sun.com/javase/technologies/hotspot/vmoptions.jsp
# for more details. Note that default settings use parallel GC.
# ADD YOUR ADDITIONAL PARAMETERS/OPTIONS HERE
#

JVM_OPTS="-ea -XX:MaxPermSize=128m -XX:+UseParNewGC -XX:MaxNewSize=32m -XX:NewSize=32m -Xms256m -Xmx256m -XX:SurvivorRatio=128 -XX:MaxTenuringThreshold=0  -XX:+UseTLAB -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled"

JAVA5=`${JAVA_HOME}/bin/java -version 2>&1 | grep "1.5."`

if [ -n "${JAVA5}" ]
then
   JVM_OPTS="${JVM_OPTS} -XX:+CMSPermGenSweepingEnabled"
fi

# Remote debugging (JPDA).
# Uncomment and change if remote debugging is required.
# JVM_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,address=8787,server=y,suspend=n ${JVM_OPTS}"


"${JAVA_HOME}/bin/java" ${JVM_OPTS} ${JMX_MON} -DGRIDGAIN_HOME="${GRIDGAIN_HOME}" -DGRIDGAIN_PROG_NAME="$0" -cp "${CP}:." org.gridgain.grid.groovy.examples.helloworld.api.GridHelloWorldGroovyExample "${CONFIG}"
