// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.swapspace;

import org.gridgain.grid.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.typedef.*;
import org.jetbrains.annotations.*;
import java.io.*;
import java.util.*;

/**
 * This class defines grid task for this example. Grid task is responsible for
 * splitting the task into jobs.
 *
 * This task has {@link GridTaskSession} injected.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.1.1c.08062011
 */
public class GridSwapSpaceTask extends GridTaskSplitAdapter<String, String> {
    /** Grid task session. */
    @GridTaskSessionResource
    private GridTaskSession ses;

    /** {@inheritDoc} */
    @Override public Collection<? extends GridJob> split(int gridSize, String arg) throws GridException {
        // Writes data to swap space.
        ses.writeToSwap("example", arg, GridTaskSessionScope.SESSION_SCOPE);

        Collection<GridJob> jobs = new ArrayList<GridJob>(gridSize);

        for (int i = 0; i < gridSize; i++) {
            jobs.add(new GridJobAdapterEx() {
                /*
                 * Do none for the purpose of this example.
                 */
                @Nullable
                @Override public Serializable execute() {
                    return null;
                }
            });
        }

        // Reads data from swap space.
        String val = (String)ses.readFromSwap("example");

        X.println("Loaded data from swap space: " + val);

        return jobs;
    }

    /** {@inheritDoc} */
    @Override public String reduce(List<GridJobResult> results) throws GridException {
        // Since jobs don't do anything,
        // we have no returned data to reduce.
        // For the purpose of this example, we
        // return data stored in swap space.
        return ses.readFromSwap("example");
    }
}
