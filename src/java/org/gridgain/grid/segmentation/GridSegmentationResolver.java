// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.segmentation;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;

/**
 * This is the base class for segmentation (a.k.a "split-brain" problem) resolvers.
 * <p>
 * Each segmentation resolver checks segment for validity, using its inner logic.
 * Typically, resolver should run light-weight single check (i.e. one IP address or
 * one shared folder). Compound segment checks may be performed using several
 * resolvers.
 * <p>
 * Note that GridGain support a logical segmentation and not limited to network
 * related segmentation only. For example, a particular segmentation resolver
 * can check for specific application or service present on the network and
 * mark the topology as segmented in case it is not available.
 * <p>
 * The following implementations are provided (Enterprise edition only):
 * <ul>
 *     <li>{@code GridReachabilitySegmentationResolver}</li>
 *     <li>{@code GridSharedFsSegmentationResolver}</li>
 *     <li>{@code GridTcpSegmentationResolver}</li>
 * </ul>
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.1.1c.19062011
 * @see GridConfiguration#getSegmentationResolvers()
 * @see GridConfiguration#getSegmentationPolicy()
 * @see GridConfiguration#getSegmentCheckFrequency()
 * @see GridConfiguration#isAllSegmentationResolversPassRequired()
 * @see GridConfiguration#isWaitForSegmentOnStart()
 */
public abstract class GridSegmentationResolver extends GridAbsPredicate {
    /**
     * Checks whether segment is valid.
     *
     * @return {@code True} if segment is correct, {@code false} otherwise.
     * @throws GridException If an error occurred.
     */
    public abstract boolean isValidSegment() throws GridException;

    /**
     * Calls {@link #isValidSegment()}.
     *
     * @return Result of {@link #isValidSegment()} method call.
     * @throws GridRuntimeException If an error occurred.
     */
    @Override public boolean apply() throws GridRuntimeException {
        try {
            return isValidSegment();
        }
        catch (GridException e) {
            throw new GridRuntimeException("Failed to check segment validity.", e);
        }
    }
}
