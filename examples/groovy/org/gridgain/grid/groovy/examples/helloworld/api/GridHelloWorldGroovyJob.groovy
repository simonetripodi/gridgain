// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.groovy.examples.helloworld.api

import org.gridgain.grid.*
import org.gridgain.grid.logger.*
import org.gridgain.grid.resources.*

/**
 * This class defines grid job for this example. Grid task is responsible for
 * splitting the task into jobs. This particular implementation splits given
 * string into individual words and creates grid jobs for each word. Every job
 * will print the word passed into it and return the number of letters in that
 * word.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.1.0c.30052011
 */
class GridHelloWorldGroovyJob extends GridJobAdapter {
    /** Grid logger. */
    @GridLoggerResource
    def GridLogger log = null

    /**
     * Constructor.
     */
    public GridHelloWorldGroovyJob(String word) {
        super()

        super.setArgument(word)
    }

    /** {@inheritDoc} */
    @Override public Serializable execute() {
        def arg = getArgument()

        if (log.isInfoEnabled() == true) {
            log.info(">>>");
            log.info(">>> Printing '" + arg + "' on this node from grid job.");
            log.info(">>>");
        }

        return arg.size()
    }
}
