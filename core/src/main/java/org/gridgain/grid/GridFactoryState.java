// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid;

import org.gridgain.grid.segmentation.*;
import org.jetbrains.annotations.*;

/**
 * Possible states of {@link GridFactory}. You can register a listener for
 * state change notifications via {@link GridFactory#addListener(GridFactoryListener)}
 * method.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.5.0c.22092011
 */
public enum GridFactoryState {
    /**
     * Grid factory started.
     */
    STARTED,

    /**
     * Grid factory stopped.
     */
    STOPPED,

    /**
     * Grid factory stopped due to network segmentation issues.
     * <p>
     * Notification on this state will be fired only when segmentation policy is
     * set to {@link GridSegmentationPolicy#STOP} or {@link GridSegmentationPolicy#RESTART_JVM}
     * and node is stopped from internals of GridGain after segment becomes invalid.
     */
    STOPPED_ON_SEGMENTATION;

    /** Enumerated values. */
    private static final GridFactoryState[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value.
     */
    @Nullable public static GridFactoryState fromOrdinal(byte ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
