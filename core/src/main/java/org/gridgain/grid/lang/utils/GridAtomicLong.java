// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.lang.utils;

import org.gridgain.grid.lang.*;

import java.util.concurrent.atomic.*;

/**
 * In addition to operations provided in java atomic data structures, this class
 * also addes greater than and less than atomic set operations.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.5.0c.22092011
 */
public class GridAtomicLong extends AtomicLong {
    /**
     * Creates a new AtomicLong with initial value {@code 0}.
     */
    public GridAtomicLong() {
        // No-op.
    }

    /**
     * Creates a new AtomicLong with the given initial value.
     *
     * @param initVal the initial value
     */
    public GridAtomicLong(long initVal) {
        super(initVal);
    }

    /**
     * Atomically updates value only if {@code check} value is greater
     * than current value.
     *
     * @param check Value to check against.
     * @param update Value to set.
     * @return {@code True} if value was set.
     */
    public boolean greaterAndSet(long check, long update) {
        while (true) {
            long cur = get();

            if (check > cur) {
                if (compareAndSet(cur, update))
                    return true;
            }
            else
                return false;
        }
    }

    /**
     * Atomically updates value only if {@code check} value is greater
     * than or equal to current value.
     *
     * @param check Value to check against.
     * @param update Value to set.
     * @return {@code True} if value was set.
     */
    public boolean greaterEqualsAndSet(long check, long update) {
        while (true) {
            long cur = get();

            if (check >= cur) {
                if (compareAndSet(cur, update))
                    return true;
            }
            else
                return false;
        }
    }

    /**
     * Atomically updates value only if {@code check} value is less
     * than current value.
     *
     * @param check Value to check against.
     * @param update Value to set.
     * @return {@code True} if value was set.
     */
    public boolean lessAndSet(long check, long update) {
        while (true) {
            long cur = get();

            if (check < cur) {
                if (compareAndSet(cur, update))
                    return true;
            }
            else
                return false;
        }
    }

    /**
     * Atomically updates value only if {@code check} value is less
     * than current value.
     *
     * @param check Value to check against.
     * @param update Value to set.
     * @return {@code True} if value was set.
     */
    public boolean lessEqualsAndSet(long check, long update) {
        while (true) {
            long cur = get();

            if (check <= cur) {
                if (compareAndSet(cur, update))
                    return true;
            }
            else
                return false;
        }
    }

    /**
     * Sets value only if it is greater than current one.
     *
     * @param update Value to set.
     * @return {@code True} if value was set.
     */
    public boolean setIfGreater(long update) {
        while (true) {
            long cur = get();

            if (update > cur) {
                if (compareAndSet(cur, update))
                    return true;
            }
            else
                return false;
        }
    }

    /**
     * Sets value only if it is greater than or equals to current one.
     *
     * @param update Value to set.
     * @return {@code True} if value was set.
     */
    public boolean setIfGreaterEquals(long update) {
        while (true) {
            long cur = get();

            if (update >= cur) {
                if (compareAndSet(cur, update))
                    return true;
            }
            else
                return false;
        }
    }

    /**
     * Sets value only if it is less than current one.
     *
     * @param update Value to set.
     * @return {@code True} if value was set.
     */
    public boolean setIfLess(long update) {
        while (true) {
            long cur = get();

            if (update < cur) {
                if (compareAndSet(cur, update))
                    return true;
            }
            else
                return false;
        }
    }

    /**
     * Sets value only if it is less than or equals to current one.
     *
     * @param update Value to set.
     * @return {@code True} if value was set.
     */
    public boolean setIfLessEquals(long update) {
        while (true) {
            long cur = get();

            if (update <= cur) {
                if (compareAndSet(cur, update))
                    return true;
            }
            else
                return false;
        }
    }


    /**
     * Sets value only if it is not equals to current one.
     *
     * @param update Value to set.
     * @return {@code True} if value was set.
     */
    public boolean setIfNotEquals(long update) {
        while (true) {
            long cur = get();

            if (update != cur) {
                if (compareAndSet(cur, update))
                    return true;
            }
            else
                return false;
        }
    }

    /**
     * Atomically updates value only if passed in predicate returns {@code true}.
     *
     * @param p Predicate to check.
     * @param update Value to set.
     * @return {@code True} if value was set.
     */
    public boolean checkAndSet(GridPredicate<Long> p, long update) {
        while (true) {
            long cur = get();

            if (p.apply(cur)) {
                if (compareAndSet(cur, update))
                    return true;
            }
            else
                return false;
        }
    }
}
