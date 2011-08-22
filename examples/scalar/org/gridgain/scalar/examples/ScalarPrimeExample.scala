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
import java.util.Arrays
import scala.util.control.Breaks._

/**
 *
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.5.0c.22082011
 */
object ScalarPrimeExample {
    /**
     * Main entry point to application. No arguments required.
     *
     * @param args Command like argument (not used).
     */
    def main(args: Array[String]){
        scalar { g: Grid =>
            val start = System.currentTimeMillis

            // Values we want to check for prime.
            val checkVals = Array(32452841L, 32452843L, 32452847L, 32452849L, 236887699L, 217645199L)

            println(">>>")
            println(">>> Starting to check the following numbers for primes: " + Arrays.toString(checkVals))

            checkVals.foreach(checkVal => {
                val divisor = g @< (SPREAD, closures(g.size(), checkVal),
                    (s: Seq[Long]) => s.find(p => p != null))

                if (!divisor.isDefined)
                    println(">>> Value '" + checkVal + "' is a prime number")
                else
                    println(">>> Value '" + checkVal + "' is divisible by '" + divisor.get + '\'')
            })

            val totalTime = System.currentTimeMillis - start

            println(">>> Total time to calculate all primes (milliseconds): " + totalTime)
            println(">>>")
        }
    }

    /**
     * Creates closures for checking passed in value for prime.
     * <p>
     * Every closure gets a range of divisors to check. The lower and
     * upper boundaries of this range are passed into closure.
     * Closures invoke {@link GridPrimeChecker} to check if the value
     * passed in is divisible by any of the divisors in the range.
     * Refer to {@link GridPrimeChecker} for algorithm specifics (it is
     * very unsophisticated).
     *
     * @param gridSize Size of the grid.
     * @param val Value to check.
     * @return Collection of closures.
     */
    private def closures(gridSize: Int, checkVal: Long): Seq[() => Long] = {
        var cls = Seq.empty[() => Long]

        val taskMinRange = 2L
        val numbersPerTask = if (checkVal / gridSize < 10) 10L else checkVal / gridSize

        var minRange = 0L
        var maxRange = 0L

        var i = 0

        while (maxRange < checkVal) {
            minRange = i * numbersPerTask + taskMinRange
            maxRange = (i + 1) * numbersPerTask + taskMinRange - 1

            if (maxRange > checkVal)
                maxRange = checkVal

            cls +:= (() => {
                var divisor = null.asInstanceOf[Long]

                breakable {
                    (minRange to maxRange).foreach(d => {
                         if (d != 1 && d != checkVal && checkVal % d == 0) {
                             divisor = d

                             break()
                         }
                    })
                }

                divisor
            })

            i += 1
        }

        cls
    }
}
