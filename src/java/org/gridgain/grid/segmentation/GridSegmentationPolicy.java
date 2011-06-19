// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.segmentation;

import org.gridgain.grid.*;
import org.gridgain.grid.loaders.cmdline.*;

/**
 * Policy that defines how node will react on topology segmentation.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.1.1c.19062011
 */
public enum GridSegmentationPolicy {
    /**
     * When segmentation policy is {@code RESTART_JVM}, all listeners will receive
     * {@link GridEventType#EVT_NODE_SEGMENTED} event and then JVM will be restarted.
     * Note, that this will work only if GridGain is started with {@link GridCommandLineLoader}
     * via standard ggstart.{sh|bat} shell script.
     */
    RESTART_JVM,

    /**
     * When segmentation policy is {@code STOP}, all listeners will receive
     * {@link GridEventType#EVT_NODE_SEGMENTED} event and then particular grid node
     * will be restarted via call to {@link GridFactory#stop(boolean, boolean)}.
     */
    STOP,

    /**
     * When segmentation policy is {@code RECONNECT}, all listeners will receive
     * {@link GridEventType#EVT_NODE_SEGMENTED} and then discovery manager will
     * try to reconnect discovery SPI to topology (issuing
     * {@link GridEventType#EVT_NODE_RECONNECTED} event on reconnect.
     * <p>
     * Note, that this policy is not recommended when data grid is enabled.
     */
    RECONNECT,

    /**
     * When segmentation policy is {@code NOOP}, all listeners will receive
     * {@link GridEventType#EVT_NODE_SEGMENTED} event and it is up to user to
     * a implement logic to handle this event.
     * <p>
     * This policy is intended to use when it is needed to perform user-defined
     * specific node stop and then start.
     */
    NOOP
}

