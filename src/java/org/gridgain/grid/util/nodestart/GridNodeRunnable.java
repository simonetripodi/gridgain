// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.nodestart;

import com.jcraft.jsch.*;
import org.gridgain.grid.lang.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * SSH-based node starter.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.5.0c.02092011
 */
public class GridNodeRunnable implements Runnable {
    /** Default start script path for Windows. */
    private static final String DFLT_SCRIPT_WIN = "bin\\ggstart.bat -v";

    /** Default start script path for Linux. */
    private static final String DFLT_SCRIPT_LINUX = "bin/ggstart.sh -v";

    /** Default log location for Windows. */
    private static final String DFLT_LOG_PATH_WIN = "%GRIDGAIN_HOME%\\work\\log\\gridgain.log";

    /** Default log location for Linux. */
    private static final String DFLT_LOG_PATH_LINUX = "$GRIDGAIN_HOME/work/log/gridgain.log";

    /** Node number. */
    private final int i;

    /** Hostname. */
    private final String host;

    /** Port number. */
    private final int port;

    /** Username. */
    private final String uname;

    /** Password. */
    private final String passwd;

    /** Private key file. */
    private final File key;

    /** Start script path. */
    private final String script;

    /** Configuration file path. */
    private final String cfg;

    /** Log file path. */
    private final String log;

    /** Start results. */
    private final Collection<GridTuple3<String, Boolean, String>> res;

    /**
     * Constructor.
     *
     * @param i Node number.
     * @param host Hostname.
     * @param port Port number.
     * @param uname Username.
     * @param passwd Password.
     * @param key Private key file.
     * @param script Start script path.
     * @param cfg Configuration file path.
     * @param log Log file path.
     * @param res Start results.
     */
    public GridNodeRunnable(int i, String host, int port, String uname, String passwd,
        @Nullable File key, @Nullable String script, @Nullable String cfg, @Nullable String log,
        Collection<GridTuple3<String, Boolean, String>> res) {
        assert host != null;
        assert port > 0;
        assert uname != null;
        assert passwd != null;
        assert res != null;

        this.i = i;
        this.host = host;
        this.port = port;
        this.uname = uname;
        this.passwd = passwd;
        this.key = key;
        this.script = script;
        this.cfg = cfg;
        this.log = log;
        this.res = res;
    }

    /** {@inheritDoc} */
    @Override public void run() {
        JSch ssh = new JSch();

        Session ses = null;

        try {
            if (key != null)
                ssh.addIdentity(key.getAbsolutePath());

            ses = ssh.getSession(uname, host, port);

            if (passwd != null)
                ses.setPassword(passwd);

            ses.setConfig("StrictHostKeyChecking", "no");

            ses.connect();

            ChannelExec ch = (ChannelExec)ses.openChannel("exec");

            if (isWindows(host, ses))
                ch.setCommand("%GRIDGAIN_HOME%\\" + (script != null ? script : DFLT_SCRIPT_WIN) + " " +
                    (cfg != null ? cfg : "") + " > " + (log != null ? log : DFLT_LOG_PATH_WIN) + "." + i);
            else
                ch.setCommand("$GRIDGAIN_HOME/" + (script != null ? script : DFLT_SCRIPT_LINUX) + " " +
                    (cfg != null ? cfg : "") + " > " + (log != null ? log : DFLT_LOG_PATH_LINUX) + "." + i +
                    " 2>& 1 &");

            try {
                ch.connect();
            }
            finally {
                if (ch.isConnected())
                    ch.disconnect();
            }

            synchronized (res) {
                res.add(new GridTuple3<String, Boolean, String>(host, true, null));
            }
        }
        catch (JSchException e) {
            synchronized (res) {
                res.add(new GridTuple3<String, Boolean, String>(host, false, e.getMessage()));
            }
        }
        finally {
            if (ses.isConnected())
                ses.disconnect();
        }
    }

    /**
     * TODO
     *
     * @param host
     * @param ses
     * @return
     */
    private boolean isWindows(String host, Session ses) {
        return false;
    }
}
