// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*
 * ________               ______                    ______   _______
 * __  ___/_____________ ____  /______ _________    __/__ \  __  __ \
 * _____ \ _  ___/_  __ `/__  / _  __ `/__  ___/    ____/ /  _  / / /
 * ____/ / / /__  / /_/ / _  /  / /_/ / _  /        _  __/___/ /_/ /
 * /____/  \___/  \__,_/  /_/   \__,_/  /_/         /____/_(_)____/
 *
 */

package org.gridgain.scalar

import java.io._

/**
 * Mixin trait that makes object throw an assertion on attempt to serialize or deserialize it.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.1.1c.05062011
 */
trait ScalarNonDistributed {
    /**
     * Throws `IOException`.
     */
    private def writeObject(out: ObjectOutputStream) {
        throw new IOException("Type is non-distributed: " + getClass.getName)
    }

    /**
     * Throws `IOException`.
     */
    private def readObject(in: ObjectInputStream) {
        throw new IOException("Type is non-distributed: " + getClass.getName)
    }

    /**
     * Throws `IOException`.
     */
    private def readObjectNoData() {
        throw new IOException("Type is non-distributed: " + getClass.getName)
    };
}