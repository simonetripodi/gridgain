// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.lang;

import org.gridgain.grid.*;
import org.gridgain.grid.typedef.internal.*;

import java.io.*;

/**
 * Grid-aware adapter for {@link Runnable} implementations. It makes the
 * runnable object {@link Serializable} and also adds peer deployment hooks to make sure that
 * deployment information is not lost.
 * <p>
 * Note that this class implements {@link GridJob} interface for convenience and can be
 * used in {@link GridTask} implementations directly, if needed, as an alternative to
 * {@link GridJobAdapterEx}.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.5.0c.22092011
 */
public abstract class GridRunnable extends GridLambdaAdapter implements Runnable, GridJob {
    /**
     * Does nothing by default. Child classes may override this method
     * to provide implementation-specific cancellation logic.
     * <p>
     * Note that this method is here only to support {@link GridJob} interface
     * and only makes sense whenever this class is used as grid job or is
     * executed via any of {@link GridProjection} methods.
     * <p>
     * {@inheritDoc}
     */
    @Override public void cancel() {
        // No-op.
    }

    /**
     * Delegates to {@link #run()} method.
     * <p>
     * {@inheritDoc}
     *
     * @return {@inheritDoc}
     * @throws GridException {@inheritDoc}
     */
    @Override public final Object execute() throws GridException {
        try {
            run();
        }
        catch (Throwable e) {
            throw U.cast(e);
        }

        return null;
    }
}
