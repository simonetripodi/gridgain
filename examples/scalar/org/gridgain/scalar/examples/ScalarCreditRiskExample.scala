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
import org.gridgain.grid.Grid
import java.util.Random

/**
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.5.0c.22082011
 */
object ScalarCreditRiskExample {
    def main(args: Array[String]) {
        scalar { g: Grid =>
            // Create portfolio.
            var portfolio: Array[Credit] = new Array[Credit](5000)



            var rnd: Random = new Random



            // Generate some test portfolio items.
            {
                var i: Int = 0
                while (i < portfolio.length) {
                    {
                        portfolio(i) = new Credit(50000 * rnd.nextDouble, rnd.nextInt(1000), rnd.nextDouble / 10, rnd.nextDouble / 20 + 0.02)
                    }
                    ({i += 1; i})
                }
            }



            // Forecast horizon in days.
            var horizon: Int = 365



            // Number of Monte-Carlo iterations.
            var iter: Int = 10000



            // Percentile.
            var percentile: Double = 0.95



            // Mark the stopwatch.
            var start: Long = System.currentTimeMillis
        }
    }
}
