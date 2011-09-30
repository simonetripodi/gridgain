// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
*  __  ____/___________(_)______  /__  ____/______ ____(_)_______
*  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
*  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
*  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
*/

package org.gridgain.grid.util;

import org.gridgain.grid.util.tostring.*;

import java.util.concurrent.locks.*;

/**
 * Synchronization aid to track "busy" state of a subsystem that owns it.
 * <p>
 * For example, there may be a manager that have different threads for some
 * purposes and the manager must not be stopped while at least a single thread
 * is in "busy" state. In this situation each thread must enter to "busy"
 * state calling method {@link #enterBusy()} in critical pieces of code
 * which, i.e. use grid kernal functionality, notifying that the manager
 * and the whole grid kernal cannot be stopped while it's in progress. Once
 * the activity is done, the thread should leave "busy" state calling method
 * {@link #leaveBusy()}. The manager itself, when stopping, should call method
 * {@link #block} that blocks till all activities leave "busy" state.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.5.0c.22092011
 */
@GridToStringExclude
public class GridBusyLock {
    /** Underlying reentrant read-write lock. */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Enters "busy" state.
     *
     * @return {@code true} if entered to busy state.
     */
    public boolean enterBusy() {
        return lock.readLock().tryLock();
    }

    /**
     * Leaves "busy" state.
     */
    public void leaveBusy() {
        lock.readLock().unlock();
    }

    /**
     * Blocks current thread till all activities left "busy" state
     * and prevents them from further entering to "busy" state.
     */
    @SuppressWarnings({"LockAcquiredButNotSafelyReleased"})
    public void block() {
        lock.writeLock().lock();
    }

    /**
     * Makes possible for activities entering busy state again.
     */
    public void unblock() {
        lock.writeLock().unlock();
    }
}
