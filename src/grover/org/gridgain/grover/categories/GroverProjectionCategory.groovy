// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*
 * _________
 * __  ____/______________ ___   _______ ________
 * _  / __  __  ___/_  __ \__ | / /_  _ \__  ___/
 * / /_/ /  _  /    / /_/ /__ |/ / /  __/_  /
 * \____/   /_/     \____/ _____/  \___/ /_/
 *
 */

package org.gridgain.grover.categories

import java.util.concurrent.*
import org.gridgain.grid.*
import org.gridgain.grid.lang.*
import org.gridgain.grover.lang.*
import org.jetbrains.annotations.*

/**
 * Extensions for {@code GridProjection}.
 * <p>
 * To access methods from the category target class has
 * to be annotated: {@code @Use(GroverProjectionCategory)}.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.1.1c.11072011
 */
@Typed
class GroverProjectionCategory {
    /**
     * Asynchronous closure call on this projection with return value.
     * This call will block until all results are received and ready.
     *
     * @param mode Closure call mode.
     * @param s Optional closure to call. If {@code null} - this method is no-op and returns {@code null}.
     * @param p Optional node filter predicate. If none provided or {@code null} - all
     *      nodes in projection will be used.
     * @return Sequence of result values from all nodes where given closures were executed
     *      or {@code null} (see above).
     */
    static <R> GridFuture<Collection<R>> callAsync$(GridProjection p, GridClosureCallMode mode,
        @Nullable GridOutClosure<R> c, @Nullable GridPredicate<? super GridRichNode>... f) {
        assert p != null

        p.callAsync(mode, [c], f)
    }

    /**
     * Synchronous closure call on this projection with return value.
     * This call will block until all results are received and ready.
     *
     * @param mode Closure call mode.
     * @param s Optional closure to call. If {@code null} - this method is no-op and returns {@code null}.
     * @param p Optional node filter predicate. If none provided or {@code null} - all
     *      nodes in projection will be used.
     * @return Sequence of result values from all nodes where given closures were executed
     *      or {@code null} (see above).
     */
    static <R> Collection<R> call$(GridProjection p, GridClosureCallMode mode,
        @Nullable GridOutClosure<R> c, @Nullable GridPredicate<? super GridRichNode>... f) {
        assert p != null

        callAsync$(p, mode, c, f).get()
    }

    /**
     * Synchronous closures call on this projection with return value.
     * This call will block until all results are received and ready. If this projection
     * is empty than {@code dflt} closure will be executed and its result returned.
     *
     * @param mode Closure call mode.
     * @param s Optional sequence of closures to call. If empty or {@code null} - this
     *      method is no-op and returns {@code null}.
     * @param dflt Closure to execute if projection is empty.
     * @param p Optional node filter predicate. If none provided or {@code null} - all
     *      nodes in projection will be used.
     * @return Sequence of result values from all nodes where given closures were executed
     *      or {@code null} (see above).
     */
    static <R> Collection<R> callSafe(GridProjection p, GridClosureCallMode mode,
        @Nullable GridOutClosure<R> c, Callable<Collection<R>> dflt,
        @Nullable GridPredicate<? super GridRichNode>... f) {
        assert p != null
        assert dflt != null

        try {
            call$(p, mode, c, f)
        }
        catch (GridEmptyProjectionException ignored) {
            dflt.call()
        }
    }

    /**
     * Synchronous closures call on this projection with return value.
     * This call will block until all results are received and ready. If this projection
     * is empty than {@code dflt} closure will be executed and its result returned.
     *
     * @param mode Closure call mode.
     * @param s Optional sequence of closures to call. If empty or {@code null} - this
     *      method is no-op and returns {@code null}.
     * @param dflt Closure to execute if projection is empty.
     * @param p Optional node filter predicate. If none provided or {@code null} - all
     *      nodes in projection will be used.
     * @return Sequence of result values from all nodes where given closures were executed
     *      or {@code null} (see above).
     */
    static <R> Collection<R> callSafe(GridProjection p, GridClosureCallMode mode,
        @Nullable Collection<GridOutClosure<R>> c, Callable<Collection<R>> dflt,
        @Nullable GridPredicate<? super GridRichNode>... f) {
        assert p != null
        assert dflt != null

        try {
            p.call(mode, c, f)
        }
        catch (GridEmptyProjectionException ignored) {
            dflt.call()
        }
    }

    /**
     * Synchronous closure call on this projection without return value.
     * This call will block until all executions are complete. If this projection
     * is empty than {@code dflt} closure will be executed.
     *
     * @param mode Closure call mode.
     * @param s Optional closure to call. If empty or {@code null} - this method is no-op.
     * @param dflt Closure to execute if projection is empty.
     * @param p Optional node filter predicate. If none provided or {@code null} - all
     *      nodes in projection will be used.
     */
    static void runSafe(GridProjection p, GridClosureCallMode mode, @Nullable GridAbsClosure c,
        Runnable dflt, @Nullable GridPredicate<? super GridRichNode>... f) {
        assert p != null
        assert dflt != null

        try {
            p.run(mode, c, f)
        }
        catch (GridEmptyProjectionException ignored) {
            dflt.run()
        }
    }

    /**
     * Synchronous closures call on this projection without return value.
     * This call will block until all executions are complete. If this projection
     * is empty than {@code dflt} closure will be executed.
     *
     * @param mode Closure call mode.
     * @param s Optional sequence of closures to call. If empty or {@code null} - this
     *      method is no-op.
     * @param p Optional node filter predicate. If none provided or {@code null} - all
     *      nodes in projection will be used.
     * @param dflt Closure to execute if projection is empty.
     */
    static void runSafe(GridProjection p, GridClosureCallMode mode, @Nullable Collection<GridAbsClosure> c,
        Runnable dflt, @Nullable GridPredicate<? super GridRichNode>... f) {
        assert p != null
        assert dflt != null

        try {
            p.run(mode, c, f)
        }
        catch (GridEmptyProjectionException ignored) {
            dflt.run()
        }
    }

    /**
     * Asynchronous closures execution on this projection with reduction. This call will
     * return immediately with the future that can be used to wait asynchronously for the results.
     *
     * @param mode Closure call mode.
     * @param s Optional sequence of closures to call. If empty or {@code null} - this method
     *      is no-op and will return finished future over {@code null}.
     * @param r Optional reduction function. If {@code null} - this method
     *      is no-op and will return finished future over {@code null}.
     * @param p Optional node filter predicate. If none provided or {@code null} - all
     *      nodes in projection will be used.
     * @return Future over the reduced result or {@code null} (see above).
     */
    static <R1, R2> GridFuture<R2> reduceAsync$(GridProjection p, GridClosureCallMode mode,
        @Nullable Collection<GridOutClosure<R1>> c, @Nullable GroverReducer<R1, R2> r,
        @Nullable GridPredicate<? super GridRichNode>... f) {
        p.reduceAsync(mode, c, r, f)
    }

    /**
     * Synchronous closures execution on this projection with reduction.
     * This call will block until all results are reduced.
     *
     * @param mode Closure call mode.
     * @param s Optional sequence of closures to call. If empty or {@code null} - this method
     *      is no-op and will return {@code null}.
     * @param r Optional reduction function. If {@code null} - this method
     *      is no-op and will return {@code null}.
     * @param p Optional node filter predicate. If none provided or {@code null} - all
     *      nodes in projection will be used.
     * @return Reduced result or {@code null} (see above).
     */
    static <R1, R2> R2 reduce$(GridProjection p, GridClosureCallMode mode,
        @Nullable Collection<GridOutClosure<R1>> c, @Nullable GroverReducer<R1, R2> r,
        @Nullable GridPredicate<? super GridRichNode>... f) {
        assert p != null

        p.reduceAsync(mode, c, r, f).get()
    }

    /**
     * Synchronous closures execution on this projection with reduction.
     * This call will block until all results are reduced. If this projection
     * is empty than {@code dflt} closure will be executed and its result returned.
     *
     * @param mode Closure call mode.
     * @param s Optional sequence of closures to call. If empty or {@code null} - this method
     *      is no-op and will return {@code null}.
     * @param r Optional reduction function. If {@code null} - this method
     *      is no-op and will return {@code null}.
     * @param dflt Closure to execute if projection is empty.
     * @param p Optional node filter predicate. If none provided or {@code null} - all
     *      nodes in projection will be used.
     * @return Reduced result or {@code null} (see above).
     */
    static <R1, R2> R2 reduceSafe(GridProjection p, GridClosureCallMode mode,
        @Nullable Collection<GridOutClosure<R1>> c, @Nullable GroverReducer<R1, R2> r,
        Callable<R2> dflt, @Nullable GridPredicate<? super GridRichNode>... f) {
        assert p != null
        assert dflt != null

        try {
            reduce$(p, mode, c, r, f)
        }
        catch (GridEmptyProjectionException ignored) {
            dflt.call()
        }
    }

    /**
     * Asynchronous closures execution on this projection with mapping and reduction.
     * This call will return immediately with the future that can be used to wait asynchronously for
     * the results.
     *
     * @param m Mapping function.
     * @param s Optional sequence of closures to call. If empty or {@code null} - this method
     *      is no-op and will return {@code null}.
     * @param r Optional reduction function. If {@code null} - this method
     *      is no-op and will return {@code null}.
     * @param p Optional node filter predicate. If none provided or {@code null} - all
     *      nodes in projection will be used.
     * @return Future over the reduced result or {@code null} (see above).
     */
    static <R1, R2> GridFuture<R2> mapreduceAsync$(GridProjection p, GroverMapper<GridOutClosure<R1>, GridRichNode> m,
        @Nullable Collection<GridOutClosure<R1>> c, @Nullable GroverReducer<R1, R2> r,
        @Nullable GridPredicate<? super GridRichNode>... f) {
        assert p != null
        assert m != null

        p.mapreduceAsync(m, c, r, f)
    }

    /**
     * Synchronous closures execution on this projection with mapping and reduction.
     * This call will block until all results are reduced.
     *
     * @param m Mapping function.
     * @param s Optional sequence of closures to call. If empty or {@code null} - this method
     *      is no-op and will return {@code null}.
     * @param r Optional reduction function. If {@code null} - this method
     *      is no-op and will return {@code null}.
     * @param p Optional node filter predicate. If none provided or {@code null} - all
     *      nodes in projection will be used.
     * @return Reduced result or {@code null} (see above).
     */
    static <R1, R2> R2 mapreduce$(GridProjection p, GroverMapper<GridOutClosure<R1>, GridRichNode> m,
        @Nullable Collection<GridOutClosure<R1>> c, @Nullable GroverReducer<R1, R2> r,
        @Nullable GridPredicate<? super GridRichNode>... f) {
        assert p != null
        assert m != null

        mapreduceAsync$(p, m, c, r, f).get()
    }

    /**
     * Synchronous closures execution on this projection with mapping and reduction.
     * This call will block until all results are reduced. If this projection
     * is empty than {@code dflt} closure will be executed and its result returned.
     *
     * @param m Mapping function.
     * @param s Optional sequence of closures to call. If empty or {@code null} - this method
     *      is no-op and will return {@code null}.
     * @param r Optional reduction function. If {@code null} - this method
     *      is no-op and will return {@code null}.
     * @param dflt Closure to execute if projection is empty.
     * @param p Optional node filter predicate. If none provided or {@code null} - all
     *      nodes in projection will be used.
     * @return Reduced result or {@code null} (see above).
     */
    static <R1, R2> R2 mapreduceSafe(GridProjection p, GroverMapper<GridOutClosure<R1>, GridRichNode> m,
        @Nullable Collection<GridOutClosure<R1>> c, @Nullable GroverReducer<R1, R2> r, Callable<R2> dflt,
        @Nullable GridPredicate<? super GridRichNode>... f) {
        assert p != null
        assert m != null
        assert dflt != null

        try {
            mapreduce$(p, m, c, r, f)
        }
        catch (GridEmptyProjectionException ignored) {
            dflt.call()
        }
    }
}
