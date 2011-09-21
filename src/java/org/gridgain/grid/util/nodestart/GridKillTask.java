// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.nodestart;

import org.gridgain.grid.*;
import org.gridgain.grid.typedef.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.gridgain.grid.kernal.GridNodeAttributes.*;

/**
 * Special kill task that never fails over jobs.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.5.0c.20092011
 */
public class GridKillTask extends GridTaskNoReduceAdapter<Object> {
    /** Restart flag. */
    private final boolean restart;

    /**
     * @param restart Restart flag.
     */
    public GridKillTask(boolean restart) {
        this.restart = restart;
    }

    /** {@inheritDoc} */
    @Override public Map<? extends GridJob, GridNode> map(List<GridNode> subgrid, @Nullable Object arg)
        throws GridException {
        Map<GridJob, GridNode> jobs = new HashMap<GridJob, GridNode>(subgrid.size());

        for (GridNode n : subgrid)
            if (!daemon(n))
                jobs.put(new GridKillJob(), n);

        return jobs;
    }

    /**
     * Checks if given node is a daemon node.
     *
     * @param n Node.
     * @return Whether node is daemon.
     */
    private boolean daemon(GridNode n) {
        return "true".equalsIgnoreCase(n.<String>attribute(ATTR_DAEMON));
    }

    /** {@inheritDoc} */
    @Override public GridJobResultPolicy result(GridJobResult res, List<GridJobResult> rcvd) {
        return GridJobResultPolicy.WAIT;
    }

    /**
     * Kill job.
     *
     * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
     * @version 3.5.0c.20092011
     */
    private class GridKillJob extends GridJobAdapterEx {
        /** {@inheritDoc} */
        @Override public Object execute() throws GridException {
            return null;
        }

        /**
         * Restarts or kills nodes.
         */
        @GridJobAfterExecute
        public void afterSend() {
            if (restart)
                new Thread(new Runnable() {
                    @Override public void run() {
                        G.restart(true, false);
                    }
                },
                "grid-restarter").start();
            else
                new Thread(new Runnable() {
                    @Override public void run() {
                        G.kill(true, false);
                    }
                },
                "grid-stopper").start();
        }
    }
}
