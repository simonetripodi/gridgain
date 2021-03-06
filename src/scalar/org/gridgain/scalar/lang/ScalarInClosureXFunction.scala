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

import org.gridgain.grid.lang.GridInClosureX

/**
 * Wrapping Scala function for `GridInClosureX`.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.5.0c.22092011
 */
class ScalarInClosureXFunction[T](val inner: GridInClosureX[T]) extends (T => Unit) {
    assert(inner != null)

    /**
     * Delegates to passed in grid closure.
     */
    def apply(t: T) {
        inner.applyx(t)
    }
}