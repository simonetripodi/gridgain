// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.deployment.uri;

import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;

/**
 * Helper class which encodes given string.
 * It replaces all occurrences of space with '%20', percent sign
 * with '%25' and semicolon with '%3B' if given string corresponds to
 * expected format.
 * <p>
 * Expected format is (schema):(//)URL(?|#)(parameters)
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.5.0c.22092011
 */
class GridUriDeploymentUriParser {
    /** Input string which should be parsed and encoded. */
    private final String input;

    /** Encoded string. */
    @GridToStringExclude private String encoded;

    /**
     * Creates new instance of parser for the given input string.
     *
     * @param input Input string which will be parsed.
     */
    GridUriDeploymentUriParser(String input) {
        assert input != null;

        this.input = input;

        encoded = input;
    }

    /**
     * Parses {@link #input} by extracting URL without schema and parameters
     * and than encodes this URL.
     * <p>
     * Expected {@link #input} format is (schema):(//)URL(?|#)(parameters)
     *
     * @return Either encoded string or unchanged if it does not match format.
     */
    String parse() {
        int n = input.length();

        // Scheme.
        int p = scan(0, n, "/?#", ":");

        if (p > 0 && at(p, n, ':')) {
            p++;            // Skip ':'

            if (at(p, n, '/')) {
                if (at(p, n, '/') == true && at(p + 1, n, '/')) {
                    p += 2;

                    // Seek authority.
                    int q = scan(p, n, "", "/?#");

                    if (q > p) {
                        p = q;
                    }
                }

                int q = scan(p, n, "", "?#");

                StringBuilder buf = new StringBuilder(input.substring(0, p));

                buf.append(encodePath(input.substring(p, q)));
                buf.append(input.substring(q, n));

                encoded = buf.toString();
            }
        }

        return encoded;
    }

    /**
     * Scan forward from the given start position.  Stop at the first char
     * in the err string (in which case -1 is returned), or the first char
     * in the stop string (in which case the index of the preceding char is
     * returned), or the end of the input string (in which case the length
     * of the input string is returned).  May return the start position if
     * none matches.
     *
     * @param start Start scan position.
     * @param end End scan position.
     * @param err Error characters.
     * @param stop Stoppers.
     * @return {@code -1} if character from the error characters list was found;
     *      index of first character occurrence is on stop character list; end
     *      position if {@link #input} does not contain any characters
     *      from {@code error} or {@code stop}; start if start > end.
     */
    private int scan(int start, int end, String err, String stop) {
        int p = start;

        while (p < end) {
            char c = input.charAt(p);

            if (err.indexOf(c) >= 0) {
                return -1;
            }

            if (stop.indexOf(c) >= 0) {
                break;
            }

            p++;
        }

        return p;
    }

    /**
     * Tests whether {@link #input} contains {@code c} at position {@code start}
     * and {@code start} less than {@code end}.
     *
     * @param start Start position.
     * @param end End position.
     * @param c Character {@link #input} is tested against
     * @return {@code true} only if {@link #input} contains {@code c} at position
     *      {@code start} and {@code start} less than {@code end}.
     */
    private boolean at(int start, int end, char c) {
        return start < end && input.charAt(start) == c;
    }

    /**
     * Encodes given path by replacing all occurrences of space with '%20',
     * percent sign with '%25' and semicolon with '%3B'.
     *
     * @param path Path to be encoded.
     * @return Encoded path.
     */
    private String encodePath(String path) {
        StringBuilder buf = new StringBuilder(path.length());

        for (int i = 0; i < path.length() ; i++) {
            char c = path.charAt(i);

            switch(c) {
                case ' ': {
                    buf.append("%20"); break;
                }

                case '%': {
                    buf.append("%25"); break;
                }
                case ';':{
                    buf.append("%3B"); break;
                }

                default: {
                    buf.append(c);
                }
            }
        }

        return  buf.toString();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridUriDeploymentUriParser.class, this);
    }
}
