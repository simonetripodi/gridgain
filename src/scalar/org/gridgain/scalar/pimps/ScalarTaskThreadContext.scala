// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*
 * ________               ______                    ______   _______
 * __  ___/_____________ ____  /______ _________    __/__ \  __  __ \
 * _____ \ _  ___/_  __ `/__  / _  __ `/__  ___/    ____/ /  _  / / /
 * ____/ / / /__  / /_/ / _  /  / /_/ / _  /        _  __/___/ /_/ /
 * /____/  \___/  \__,_/  /_/   \__,_/  /_/         /____/_(_)____/
 *
 */

package org.gridgain.scalar.pimps

import org.gridgain.grid._
import scalaz._
import org.jetbrains.annotations._
import org.gridgain.scalar._

/**
 * This trait provide mixin for properly typed version of `GridProjection#withName(...)` method.
 *
 * Method on `GridProjection` always returns an instance of type `GridProjection` even when
 * called on a sub-class. This trait's method `withName$` return the instance of the same type
 * it was called on.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.1.1c.17062011
 */
trait ScalarTaskThreadContext[T <: GridProjection] extends ScalarMixin { this: PimpedType[T] =>
    /**
     * Properly typed version of `GridProjection#withName(...)` method.
     *
     * @param taskName Name of the task.
     */
    def withName$(@Nullable taskName: String): T =
        value.withName(taskName).asInstanceOf[T]

    /**
     * Properly typed version of `GridProjection#withResult(...)` method.
     *
     * @param res Ad-hoc implementation of `GridTask#result(...)` method.
     */
    def withResult$(@Nullable f: (GridJobResult, java.util.List[GridJobResult]) => GridJobResultPolicy): T =
        value.withResult(toClosure2X(f)).asInstanceOf[T]

}