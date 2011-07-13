// Copyright (C) GridGain Systems, Inc. Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.lang.utils;

import org.gridgain.grid.typedef.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

/**
 * Concurrent queue which fixes infinite growth of JDK
 * {@link ConcurrentLinkedQueue} implementation. On top of that, it provides
 * public access to internal {@code 'nodes'}, so middle elements of the queue
 * can be accessed immediately.
 * <p>
 * The problem of JDK implementation is that it must follow regular queue
 * semantics and will only work if you call {@link Queue#offer(Object)} and
 * {@link Queue#poll()}, and will leave nullified nodes mingling around if
 * {@link Collection#remove(Object)} was called which can lead to memory leaks.
 *
 * @author 2005-2011 Copyright (C) GridGain Systems, Inc.
 * @version 3.1.1c.14072011
 */
public class GridConcurrentLinkedQueue<E> extends GridSerializableCollection<E> implements Queue<E> {
    /** List head. */
    @GridToStringExclude
    protected final AtomicReference<Node<E>> head = new AtomicReference<Node<E>>(new Node<E>());

    /** List tail. */
    @GridToStringExclude
    protected final AtomicReference<Node<E>> tail = new AtomicReference<Node<E>>(head.get());

    /** Current size. */
    @GridToStringInclude
    private final AtomicInteger size = new AtomicInteger();

    /** Compacting flag.*/
    private final AtomicBoolean compacting = new AtomicBoolean(false);

    /** Counter of void nodes ready to be GC'ed. */
    private final AtomicInteger eden = new AtomicInteger(0);

    /** Count of created nodes. */
    private final AtomicLong createCnt = new AtomicLong(0);

    /** Count of GC'ed nodes. */
    private final AtomicLong gcCnt = new AtomicLong(0);

    /** Lock. */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** GC time. */
    private volatile long avgGcTime;

    /** GC calls. */
    private volatile long gcCalls;

    /**
     * Gets count of cleared nodes that are stuck in the queue and should be removed.
     * To clear, call {@link #gc(int)} method.
     *
     * @return Eden count.
     */
    public int eden() {
        return eden.get();
    }

    /**
     * Gets time spent on GC'ing the queue.
     *
     * @return Time spent on GC'ing the queue.
     */
    public long averageGcTime() {
        return avgGcTime;
    }

    /**
     * Gets number of nodes GC'ed (discarded) by this queue.
     *
     * @return Number of nodes GC'ed (discarded) by this queue.
     */
    public long nodesGced() {
        return gcCnt.get();
    }

    /**
     * Gets number of nodes created by this queue.
     *
     * @return Number of nodes created by this queue.
     */
    public long nodesCreated() {
        return createCnt.get();
    }

    /**
     * Gets number of times GC has executed.
     *
     * @return Number of times GC has executed.
     */
    public long gcCalls() {
        return gcCalls;
    }

    /**
     * Gets internal read-write lock to control GC.
     *
     * @return Internal read-write lock to control GC.
     */
    protected ReadWriteLock lock() {
        return lock;
    }

    /**
     * @param t Capsule to check.
     * @return {@code True} if this capsule is first.
     */
    public boolean isFirst(E t) {
        while (true) {
            Node<E> n = peekNode();

            if (n == null)
                return false;

            E cap = n.value();

            if (cap == null)
                continue; // Try again.

            return cap == t;
        }
    }

    /**
     * This method needs to be periodically called whenever elements are
     * cleared or removed from the middle of the queue. It will make sure
     * that all empty nodes that exceed {@code maxEden} counter will be
     * cleared.
     *
     * @param maxEden Maximum number of void nodes in this collection.
     */
    @SuppressWarnings( {"TooBroadScope"})
    public void gc(int maxEden) {
        int evictCnt = eden.get() - maxEden;

        if (evictCnt < maxEden)
            return; // Nothing to evict.

        if (compacting.compareAndSet(false, true)) {
            long start = -1;
            long calls = -1;

            lock.writeLock().lock();

            try {
                start = System.currentTimeMillis();

                calls = ++gcCalls;

                for (Node<E> prev = head.get(); prev != null;) {
                    Node<E> next = prev.next();

                    if (next == null)
                        return; // Don't mess with tail pointer.

                    assert !prev.removed() || prev == head.get();

                    if (next.cleared()) {
                        Node<E> h = head.get();
                        Node<E> t = tail.get();

                        Node<E> n = next.next();

                        if (n == null)
                            return; // Don't mess with tail pointer.

                        if (prev == h) {
                            if (h == t)
                                return; // Nothing to do if queue is empty.

                            boolean b = casHead(h, next);

                            assert b;

                            if (gcNode(next))
                                evictCnt--;

                            prev = head.get();
                        }
                        else {
                            boolean b = prev.casNext(next, n);

                            assert b;

                            if (gcNode(next))
                                evictCnt--;
                        }

                        if (evictCnt == 0)
                            return;
                    }
                    else
                        prev = next;
                }
            }
            finally {
                if (start > 0)
                    avgGcTime = ((avgGcTime * (calls - 1) + (System.currentTimeMillis() - start)) / calls);

                lock.writeLock().unlock();

                compacting.set(false);
            }
        }
    }

    /**
     * String representation of this queue, including cleared and removed nodes if
     * they are still present.
     *
     * @return String representation of this queue.
     */
    public String toShortString() {
        return "Queue [size=" + size() + ", eden=" + eden() + ", avgGcTime=" + averageGcTime() +
            ", gcCalls=" + gcCalls + ", createCnt=" + createCnt + ", rmvCnt=" + gcCnt + ", head=" + head +
            ", tail=" + tail + ']';
    }

    /**
     * Prints full queue to standard out.
     */
    public void printFullQueue() {
        X.println("Queue [size=" + size() + ", eden=" + eden() + ", avgGcTime=" + averageGcTime() + ", head=" + head +
            ", tail=" + tail + ", nodes=[");

        for (Node<E> n = head.get(); n != null; n = n.next())
            X.println(n.toString());

        X.println("]");
    }

    /**
     * @param n Node to GC.
     * @return {@code True} if node was GC'ed by this call.
     */
    private boolean gcNode(Node<E> n) {
        assert !n.active();

        if (n.remove()) {
            eden.decrementAndGet();
            gcCnt.incrementAndGet();

            return true;
        }

        return false;
    }

    /**
     * @param n Node to clear.
     * @return New size.
     */
    public boolean clearNode(Node<E> n) {
        if (n.clear() != null) {
            eden.incrementAndGet();
            size.decrementAndGet();

            return true;
        }

        return false;
    }

    /**
     * Node to add to the queue (cannot be {@code null}).
     *
     * @param n New node.
     * @return The same node as passed in.
     */
    public Node<E> addNode(Node<E> n) {
        A.notNull(n, "n");

        createCnt.incrementAndGet();

        lock.readLock().lock();

        try {
            while (true) {
                Node<E> t = tail.get();

                Node<E> s = t.next();

                if (t == tail.get()) {
                    if (s == null) {
                        if (t.casNext(s, n)) {
                            casTail(t, n);

                            size.incrementAndGet();

                            return n;
                        }
                    }
                    else
                        casTail(t, s);
                }
            }
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * New element to add (cannot be {@code null}).
     *
     * @param e New element.
     * @return Created node.
     */
    public Node<E> addNode(E e) {
        return addNode(new Node<E>(e));
    }

    /**
     * @return First node.
     */
    @Nullable public Node<E> peekNode() {
        lock.readLock().lock();

        try {
            while (true) {
                Node<E> h = head.get();
                Node<E> t = tail.get();

                Node<E> first = h.next();

                if (h == head.get()) {
                    if (h == t) {
                        if (first == null)
                            return null;
                        else
                            casTail(t, first);
                    }
                    else {
                        assert first != null : "LRU queue [head=" + head + ", tail=" + tail + ", size=" + size()  +
                            ", eden=" + eden() + ']';

                        if (first.active())
                            return first;
                        else
                            // This is GC step to remove obsolete nodes.
                            if (casHead(h, first)) {
                                gcNode(first);

                                salvageNode(h);
                            }
                    }
                }
            }
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Gets first queue element (head of the queue).
     *
     * @return First queue element.
     */
    @Override @Nullable public E peek() {
        while (true) {
            Node<E> n = peekNode();

            if (n == null)
                return null;

            E e = n.value();

            if (e != null)
                return e;
        }
    }

    /**
     * @return Previous head of the queue.
     */
    @Override @Nullable public E poll() {
        lock.readLock().lock();

        try {
            while (true) {
                Node<E> h = head.get();
                Node<E> t = tail.get();

                Node<E> first = h.next();

                if (h == head.get()) {
                    if (h == t) {
                        if (first == null)
                            return null;
                        else
                            casTail(t, first);
                    }
                    else if (casHead(h, first)) { // Move head pointer.
                        assert first != null;

                        E c = first.value();

                        if (c != null) {
                            if (removeNode(first)) {
                                assert !first.active();

                                gcNode(first);

                                salvageNode(h);

                                return c;
                            }
                        }
                        else
                            assert !first.active();

                        gcNode(first);

                        salvageNode(h);
                    }
                }
            }
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean add(E e) {
        return addNode(e) != null;
    }

    /** {@inheritDoc} */
    @Override public boolean offer(E e) {
        return add(e);
    }

    /** {@inheritDoc} */
    @Nullable @Override public E remove() {
        return poll();
    }

    /** {@inheritDoc} */
    @Override public E element() {
        E e = peek();

        if (e == null)
            throw new NoSuchElementException();

        return e;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return size.get();
    }

    /** {@inheritDoc} */
    @Override public Iterator<E> iterator() {
        return new QueueIterator(false);
    }

    /**
     * If {@code 'readOnly'} flag is {@code true}, then returned
     * iterator {@code 'remove()'} method will throw
     * {@link UnsupportedOperationException}.
     *
     * @param readOnly Read-only flag.
     * @return Iterator over this queue.
     */
    public Iterator<E> iterator(boolean readOnly) {
        return new QueueIterator(readOnly);
    }

    /**
     * Removes node from queue by calling {@link #clearNode(Node)} on the
     * passed in node. Child classes that override this method
     * should always make sure to call {@link #clearNode(Node)}}.
     *
     * @param n Node to remove.
     * @return {@code True} if node was cleared by this method call, {@code false}
     *      if it was already cleared.
     */
    protected boolean removeNode(Node<E> n) {
        return clearNode(n);
    }

    /**
     * @param old Old value.
     * @param val New value.
     * @return {@code True} if value was set.
     */
    private boolean casTail(Node<E> old, Node<E> val) {
        assert val != null : toShortString();

        return tail.compareAndSet(old, val);
    }

    /**
     * @param old Old value.
     * @param val New value.
     * @return {@code True} if value was set.
     */
    private boolean casHead(Node<E> old, Node<E> val) {
        assert val != null : toShortString();

        return head.compareAndSet(old, val);
    }

    /**
     * Attempts to clear and remove node and decrements eden if needed.
     *
     * @param n Node to unlink.
     */
    private void salvageNode(Node<E> n) {
        assert n != null;

        clearNode(n);
        gcNode(n);
    }

    /**
     * Iterator.
     */
    private class QueueIterator implements Iterator<E> {
        /** Next node to return item for. */
        private Node<E> nextNode;

        /** Next cache entry to return. */
        private E nextItem;

        /** Last node. */
        private Node<E> lastNode;

        /** Read-only flag. */
        private final boolean readOnly;

        /**
         * @param readOnly Read-only flag.
         */
        QueueIterator(boolean readOnly) {
            this.readOnly = readOnly;

            advance();
        }

        /**
         * @return Next entry.
         */
        private E advance() {
            lastNode = nextNode;

            E x = nextItem;

            Node<E> p = (nextNode == null) ? peekNode() : nextNode.next();

            while (true) {
                if (p == null) {
                    nextNode = null;

                    nextItem = null;

                    return x;
                }

                E e = p.value();

                if (e != null) {
                    nextNode = p;

                    nextItem = e;

                    return x;
                }

                p = p.next();
            }
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return nextNode != null;
        }

        @Override public E next() {
            if (nextNode == null)
                throw new NoSuchElementException();

            return advance();
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            if (readOnly)
                throw new UnsupportedOperationException();

            Node<E> last = lastNode;

            if (last == null)
                throw new IllegalStateException();

            // Future gc() or first() calls will remove it.
            clearNode(last);

            lastNode = null;
        }
    }

    /**
     * Concurrent linked queue node. It can be effectively cleared (nullified)
     * by calling {@link GridConcurrentLinkedQueue#clearNode(Node)} which allows
     * to clear nodes without having to iterate through the queue. Note that
     * clearing a node only nullifies its value and does not remove the node from
     * the queue. To make sure that memory is reclaimed you must call
     * {@link GridConcurrentLinkedQueue#gc(int)} explicitly.
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static final class Node<E> extends AtomicStampedReference<Node<E>> {
        /** Entry. */
        @GridToStringInclude
        private volatile E e;

        /**
         * Head constructor.
         */
        private Node() {
            super(null, 0);

            e = null;

            boolean b = compareAndSet(null, null, 0, 2); // Mark removed right away.

            assert b;
        }

        /**
         * @param e Capsule.
         */
        public Node(E e) {
            super(null, 0);

            A.notNull(e, "e");

            this.e = e;
        }

        /**
         * @return Entry.
         */
        @Nullable public E value() {
            return !active() ? null : e;
        }

        /**
         * @return {@code True} if node is active.
         */
        public boolean active() {
            return getStamp() == 0;
        }

        /**
         * @return {@code True} if entry is cleared from node.
         */
        public boolean cleared() {
            return getStamp() == 1;
        }

        /**
         * Atomically clears entry.
         *
         * @return Non-null value if cleared or {@code null} if was already clear.
         */
        @Nullable E clear() {
            if (e != null) {
                while (true) {
                    Node<E> next = next();

                    if (compareAndSet(next, next, 0, 1)) {
                        E ret = e;

                        e = null;

                        return ret;
                    }

                    if (next == next())
                        return null;
                }
            }

            return null;
        }

        /**
         * @return {@code True} if node was removed.
         */
        boolean removed() {
            return getStamp() == 2;
        }

        /**
         * @return {@code True} if node was {@code removed} by this call.
         */
        boolean remove() {
            assert !active();

            while (true) {
                Node<E> next = next();

                if (compareAndSet(next, next, 1, 2))
                    return true;

                if (next == next())
                    return false;
            }
        }

        /**
         * @param cur Current value.
         * @param next New value.
         * @return {@code True} if set.
         */
        boolean casNext(Node<E> cur, Node<E> next) {
            assert next != null;
            assert next != this;

            while (true) {
                int stamp = getStamp();

                if (compareAndSet(cur, next, stamp, stamp))
                    return true;

                if (stamp == getStamp())
                    return false;
            }
        }

        /**
         * @return Next linked node.
         */
        @Nullable Node<E> next() {
            return getReference();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            return o == this;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Node.class, this,
                "hash", System.identityHashCode(this),
                "stamp", getStamp(),
                "nextNull", (next() == null));
        }
    }
}
