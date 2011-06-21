// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.groovy.examples.helloworld.api

import org.gridgain.grid.*

/**
 * This class defines grid task for this example. Grid task is responsible for
 * splitting the task into jobs. This particular implementation splits given
 * string into individual words and creates grid jobs for each word. Every job
 * will print the word passed into it and return the number of letters in that
 * word.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.1.1c.21062011
 */
class GridHelloWorldGroovyTask extends GridTaskSplitAdapter<String, Integer> {
    /**
     * Splits the passed in phrase into words and creates a job for every
     * word. Every job will print out the word and return number of letters in that
     * word.
     *
     * @param gridSize Number of nodes in the grid.
     * @param phrase Any phrase (for this example we pass in <tt>"Hello World"</tt>).
     * @return Created grid jobs for remote execution.
     * @throws GridException If split failed.
     */
    public Collection split(int gridSize, String phrase) throws GridException {
        // Split the passed in phrase into multiple words separated by spaces.
        String[] words = ((String)phrase).split(" ");

        List<GridJob> jobs = new ArrayList<GridJob>(words.length);

        for (String word : words) {
            // Every job gets its own word as an argument.
            jobs.add(new GridHelloWorldGroovyJob(word));
        }


        return jobs;
    }

    /**
     * Sums up all characters returns from all jobs and returns a
     * total number of characters in the phrase.
     *
     * @param results Job results.
     * @return Number of characters for the phrase passed into
     *      <tt>split(gridSize, phrase)</tt> method above.
     * @throws GridException If reduce failed.
     */
    public Integer reduce(List<GridJobResult> results) throws GridException {
        int totalCharCnt = 0;

        for (GridJobResult res : ((List<GridJobResult>)results)) {
            // Every job returned a number of letters
            // for the word it was responsible for.
            Integer charCnt = res.getData();

            totalCharCnt += charCnt;
        }

        // Account for spaces. For simplicity we assume one space between words.
        totalCharCnt += results.size() - 1;

        // Total number of characters in the phrase
        // passed into task execution.
        return (Integer)totalCharCnt;
    }
}
