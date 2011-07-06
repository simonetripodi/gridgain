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
import org.gridgain.grid.GridClosureCallMode._

/**
 * Shows the world's shortest MapReduce application that calculates non-space
 * length of the input string. This example works equally on one computer or
 * on thousands requiring no special configuration or deployment.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.1.1c.06072011
 */
object ScalarWorldShortestMapReduce {
    /**
     * Entry point. Just pass any string and it's non-space length will be
     * calculated on the grid/cloud.
     *
     * @param args Command line arguments, one is required.
     */
    def main(args: Array[String]): Unit = scalar {
        println("Non-space characters count: " +
            grid$ @< (SPREAD, for (w <- args(0).split(" ")) yield (() => w.length), (s: Seq[Int]) => s.sum)
        )
    }
}