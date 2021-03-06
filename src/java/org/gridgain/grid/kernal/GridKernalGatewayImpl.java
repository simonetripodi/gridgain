// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.lang.utils.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.tostring.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

import static org.gridgain.grid.kernal.GridKernalState.*;

/**
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.5.0c.22092011
 */
@GridToStringExclude
public class GridKernalGatewayImpl implements GridKernalGateway, Serializable {
    /** */
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    /** */
    private final Collection<Runnable> lsnrs = new GridSetWrapper<Runnable>(new IdentityHashMap<Runnable, Object>());

    /** */
    private volatile GridKernalState state = STOPPED;

    /** */
    private final String gridName;

    /**
     * @param gridName Grid name.
     */
    public GridKernalGatewayImpl(String gridName) {
        this.gridName = gridName;
    }

    /**
     * {@inheritDoc}
     */
    @Override public void lightCheck() throws IllegalStateException {
        if (state != STARTED)
            throw illegalState();
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings({"LockAcquiredButNotSafelyReleased", "BusyWait"})
    @Override public void readLock() throws IllegalStateException {
        boolean interrupted = false;

        while (true) {
            try {
                rwLock.readLock().lockInterruptibly();

                break;
            }
            catch (InterruptedException ignore) {
                // Preserve interrupt status & ignore.
                // Note that interrupted flag is cleared.
                interrupted = true;
            }
        }

        if (interrupted)
            // Reset interrupted status.
            Thread.currentThread().interrupt();

        if (state != STARTED) {
            // Unlock just acquired lock.
            rwLock.readLock().unlock();

            throw illegalState();
        }

        enterThreadLocals();
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"BusyWait"})
    @Override public void writeLock() {
        enterThreadLocals();

        boolean interrupted = false;

        // Busy wait is intentional.
        while (true)
            try {
                if (rwLock.writeLock().tryLock(200, TimeUnit.MILLISECONDS))
                    break;
                else
                    Thread.sleep(200);
            }
            catch (InterruptedException ignore) {
                // Preserve interrupt status & ignore.
                // Note that interrupted flag is cleared.
                interrupted = true;
            }

        if (interrupted)
            Thread.currentThread().interrupt();
    }

    /**
     * Creates new illegal state exception.
     *
     * @return Newly created exception.
     */
    private IllegalStateException illegalState() {
        return new IllegalStateException("Grid is in invalid state to perform this operation. " +
            "It either not started yet or has already being or have stopped [gridName=" + gridName +
            ", state=" + state + ']');
    }

    /**
     * Enter thread locals.
     */
    private void enterThreadLocals() {
        GridThreadLocal.enter();
        GridThreadLocalEx.enter();
    }

    /**
     * Leave thread locals.
     */
    private void leaveThreadLocals() {
        GridThreadLocalEx.leave();
        GridThreadLocal.leave();
    }

    /** {@inheritDoc} */
    @Override public void readUnlock() {
        leaveThreadLocals();

        rwLock.readLock().unlock();
    }

    /** {@inheritDoc} */
    @Override public void writeUnlock() {
        rwLock.writeLock().unlock();

        leaveThreadLocals();
    }

    /** {@inheritDoc} */
    @Override public void setState(GridKernalState state) {
        assert state != null;

        // NOTE: this method should always be called within write lock.
        this.state = state;

        if (state == STOPPING) {
            Runnable[] runs;

            synchronized (lsnrs) {
                lsnrs.toArray(runs = new Runnable[lsnrs.size()]);
            }

            // In the same thread.
            for (Runnable r : runs)
                r.run();
        }
    }

    /** {@inheritDoc} */
    @Override public GridKernalState getState() {
        return state;
    }

    /** {@inheritDoc} */
    @Override public void addStopListener(Runnable lsnr) {
        assert lsnr != null;

        if (state == STARTING || state == STARTED)
            synchronized (lsnrs) {
                lsnrs.add(lsnr);
            }
        else
            // Call right away in the same thread.
            lsnr.run();
    }

    /** {@inheritDoc} */
    @Override public void removeStopListener(Runnable lsnr) {
        assert lsnr != null;

        synchronized (lsnrs) {
            lsnrs.remove(lsnr);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridKernalGatewayImpl.class, this);
    }
}
