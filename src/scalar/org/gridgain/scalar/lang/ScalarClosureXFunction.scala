// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*
 * ________               ______                    ______   _______
 * __  ___/_____________ ____  /______ _________    __/__ \  __  __ \
 * _____ \ _  ___/_  __ `/__  / _  __ `/__  ___/    ____/ /  _  / / /
 * ____/ / / /__  / /_/ / _  /  / /_/ / _  /        _  __/___/ /_/ /
 * /____/  \___/  \__,_/  /_/   \__,_/  /_/         /____/_(_)____/
 *
 */
 
package org.gridgain.scalar.lang

import org.gridgain.grid.lang.GridClosureX

/**
 * Wrapping Scala function for `GridClosureX`.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.1.1c.03072011
 */
class ScalarClosureXFunction[T, R](val inner: GridClosureX[T, R]) extends (T => R) {
    assert(inner != null)

    /**
     * Delegates to passed in grid closure.
     */
    def apply(t: T): R = {
        inner.applyx(t)
    }
}