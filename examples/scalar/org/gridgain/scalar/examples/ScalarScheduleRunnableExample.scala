// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*
 * ________               ______                    ______   _______
 * __  ___/_____________ ____  /______ _________    __/__ \  __  __ \
 * _____ \ _  ___/_  __ `/__  / _  __ `/__  ___/    ____/ /  _  / / /
 * ____/ / / /__  / /_/ / _  /  / /_/ / _  /        _  __/___/ /_/ /
 * /____/  \___/  \__,_/  /_/   \__,_/  /_/         /____/_(_)____/
 *
 */

package org.gridgain.scalar.examples

import org.gridgain.scalar.scalar
import scalar._
import org.gridgain.grid._
import GridClosureCallMode._

/**
 * Demonstrates a cron-based `Runnable` execution scheduling.
 * Test runnable object broadcasts a phrase to all grid nodes every minute
 * ten times with initial scheduling delay equal to five seconds.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.5.0c.02092011
 */
object ScalarScheduleRunnableExample {
    /**
     * Example entry point. No arguments required.
     */
    def main(args: Array[String]) {
        scalar { g: Grid =>
            // Schedule output message every minute.
            g.scheduleLocalRun(
                () => g *< (BROADCAST, () => println("Howdy! :)")),
                "{5, 10} * * * * *" // Cron expression.
            )

            Thread.sleep(1000 * 60 * 2)

            println(">>>>> Check all nodes for hello message output.")
        }
    }
}
