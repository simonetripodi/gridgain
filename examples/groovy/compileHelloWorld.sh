#!/bin/sh
#
# Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html
#  _________        _____ __________________        _____
#  __  ____/___________(_)______  /__  ____/______ ____(_)_______
#  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
#  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
#  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
#
# Version: 3.1.1c.12062011
#

#
# Check GROOVY_HOME.
#
if [ "$GROOVY_HOME" = "" ]; then
    echo $0", ERROR: GROOVY_HOME environment variable is not found."
    echo $0", ERROR: Please create GROOVY_HOME variable pointing to location of groovy."
    exit 1
fi

#
# Set property JAR name during the Ant build.
#
ANT_AUGMENTED_GGJAR=gridgain-3.1.1c.jar

${GROOVY_HOME}/bin/groovyc -cp ${GRIDGAIN_HOME}/${ANT_AUGMENTED_GGJAR}  ./org/gridgain/grid/groovy/examples/helloworld/api/GridHelloWorldGroovyExample.groovy ./org/gridgain/grid/groovy/examples/helloworld/api/GridHelloWorldGroovyJob.groovy ./org/gridgain/grid/groovy/examples/helloworld/api/GridHelloWorldGroovyTask.groovy

