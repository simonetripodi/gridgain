// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.thread;

import org.gridgain.grid.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.worker.*;

import java.io.*;
import java.net.*;
import java.text.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.GridSystemProperties.*;

/**
 * This class adds some necessary plumbing on top of the {@link Thread} class.
 * Specifically, it adds:
 * <ul>
 *      <li>Consistent naming of threads</li>
 *      <li>Dedicated parent thread group</li>
 *      <li>Backing interrupted flag</li>
 * </ul>
 * <b>Note</b>: this class is intended for internal use only.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.5.0c.22092011
 */
public class GridThread extends Thread {
    /** */
    private static final Map<Thread, String> emailHeaders = new HashMap<Thread, String>();

    /** Default thread's group. */
    private static final ThreadGroup DFLT_GRP = new ThreadGroup("gridgain") {
        @Override public void uncaughtException(Thread t, Throwable e) {
            super.uncaughtException(t, e);

            if (System.getProperty(GG_ASSERT_SEND_DISABLED) != null &&
                !"false".equalsIgnoreCase(System.getProperty(GG_ASSERT_SEND_DISABLED)))
                return;

            if (e instanceof AssertionError) {
                SB params = new SB();

                params.a("header=").a(emailHeaders.get(t)).a("thread=").a(t.getName()).
                    a("&").a("message=").a(e.getMessage());

                StackTraceElement[] trace = e.getStackTrace();

                params.a("&trace_header=").a(e.toString());

                int length = 0;

                for (StackTraceElement elem : trace)
                    if (elem.getClassName().startsWith("org.") ||
                        elem.getClassName().startsWith("java.") ||
                        elem.getClassName().startsWith("javax.") ||
                        elem.getClassName().startsWith("scala.") ||
                        elem.getClassName().startsWith("groovy."))
                        params.a("&trace_line_").a(length++).a("=").a(elem.toString());

                params.a("&trace_size=").a(length);

                HttpURLConnection conn = null;

                try {
                    URL url = new URL("http://localhost:81/assert.php");

                    conn = (HttpURLConnection)url.openConnection();

                    conn.setRequestMethod("POST");
                    conn.setDoOutput(true);
                    conn.setReadTimeout(5000);

                    DataOutputStream out = new DataOutputStream(conn.getOutputStream());

                    out.writeBytes(params.toString());

                    out.close();

                    conn.getInputStream().read();
                }
                catch (IOException ignored) {
                    // No-op
                }
                finally {
                    if (conn != null)
                        conn.disconnect();
                }
            }
        }
    };

    /** Number of all grid threads in the system. */
    private static final AtomicLong threadCntr = new AtomicLong(0);

    /**
     * Creates thread with given worker.
     *
     * @param worker Runnable to create thread with.
     */
    public GridThread(GridWorker worker) {
        this(DFLT_GRP, worker.gridName(), worker.name(), worker);
    }

    /**
     * Creates grid thread with given name for a given grid.
     *
     * @param gridName Name of grid this thread is created for.
     * @param threadName Name of thread.
     * @param r Runnable to execute.
     */
    public GridThread(String gridName, String threadName, Runnable r) {
        this(DFLT_GRP, gridName, threadName, r);
    }

    /**
     * Creates grid thread with given name for a given grid with specified
     * thread group.
     *
     * @param grp Thread group.
     * @param gridName Name of grid this thread is created for.
     * @param threadName Name of thread.
     * @param r Runnable to execute.
     */
    public GridThread(ThreadGroup grp, String gridName, String threadName, Runnable r) {
        super(grp, r, createName(threadCntr.incrementAndGet(), threadName, gridName));

//        if (grp == DFLT_GRP)
//            emailHeaders.put(this, createEmailHeader(G.grid(gridName)));
    }

    /**
     * Creates assertion email header for current grid.
     *
     * @param grid Grid.
     * @return Header for assertion email.
     */
    private static String createEmailHeader(Grid grid) {
        GridEnterpriseLicense lic = grid.license();

        SB sb = new SB();

        sb.a("Error time: ").a(new SimpleDateFormat("MM/dd/yy, HH:mm:ss").format(new Date())).a("\n").
            a("Grid name: ").a(grid.name()).a("\n").
            a("Edition: ").a(lic != null ? "Enterprise" : "Community").a("\n");

        if (lic != null)
            sb.a("License ID: ").a(lic.getId().toString().toUpperCase()).a("\n").
                a("Licensed to: ").a(lic.getUserOrganization());

        return sb.toString().trim();
    }

    /**
     * Creates new thread name.
     *
     * @param num Thread number.
     * @param threadName Thread name.
     * @param gridName Grid name.
     * @return New thread name.
     */
    private static String createName(long num, String threadName, String gridName) {
        return threadName + "-#" + num + '%' + gridName + '%';
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridThread.class, this, "name", getName());
    }
}
