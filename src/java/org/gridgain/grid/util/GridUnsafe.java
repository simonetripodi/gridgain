// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util;

import sun.misc.Unsafe;

import java.lang.reflect.*;

/**
 * Provides handle on Unsafe class from SUN which cannot be instantiated directly.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.5.0c.10082011
 */
public class GridUnsafe {
    /**
     * Ensure singleton.
     */
    private GridUnsafe() {
        // No-op.
    }

    /**
     * @return Instance of Unsafe class.
     */
    public static Unsafe unsafe() {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");

            field.setAccessible(true);

            return (Unsafe)field.get(null);
        }
        catch (Throwable e) {
           throw new AssertionError(e);
        }
    }
}
