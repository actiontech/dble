/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.util;

import java.util.*;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class FairLinkedBlockingDeque<E> extends AbstractQueue<E> implements BlockingDeque<E>, java.io.Serializable {

    /*
     * Implemented as a simple doubly-linked list protected by a single lock and
     * using conditions to manage blocking.
     *
     * To implement weakly consistent iterators, it appears we need to keep all
     * Nodes GC-reachable from a predecessor dequeued Node. That would cause two
     * problems: - allow a rogue Iterator to cause unbounded memory retention -
     * cause cross-generational linking of old Nodes to new Nodes if a Node was
     * tenured while live, which generational GCs have a hard time dealing with,
     * causing repeated major collections. However, only non-deleted Nodes need
     * to be reachable from dequeued Nodes, and reachability does not
     * necessarily have to be of the kind understood by the GC. We use the trick
     * of linking a Node that has just been dequeued to itself. Such a self-link
     * implicitly means to advance to head.
     */

    /*
     * We have "diamond" multiple interface/abstract class inheritance here, and
     * that introduces ambiguities. Often we want the BlockingDeque javadoc
     * combined with the AbstractQueue implementation, so a lot of method specs
     * are duplicated here.
     */

    private static final long serialVersionUID = -387911632671998426L;

    /**
     * Pointer to first node
     */
    transient Node<E> first;
    /**
     * Pointer to last node
     */
    transient Node<E> last;
    /**
     * Number of items in the deque
     */
    private transient int count;
    /**
     * Maximum number of items in the deque
     */
    private final int capacity;
    /**
     * Main lock guarding all access
     */
    final ReentrantLock lock = new ReentrantLock(true);
    /**
     * Condition for waiting takes
     */
    private final Condition notEmpty = lock.newCondition();
    /**
     * Condition for waiting puts
     */
    private final Condition notFull = lock.newCondition();

    /**
     * Creates a <tt>LinkedBlockingDeque</tt> with a capacity of
     * {@link Integer#MAX_VALUE}.
     */
    public FairLinkedBlockingDeque() {
        this(Integer.MAX_VALUE);
    }

    /**
     * Creates a <tt>LinkedBlockingDeque</tt> with the given (fixed) capacity.
     *
     * @param capacity the capacity of this deque
     * @throws IllegalArgumentException if <tt>capacity</tt> is less than 1
     */
    public FairLinkedBlockingDeque(int capacity) {
        if (capacity <= 0)
            throw new IllegalArgumentException();
        this.capacity = capacity;
    }

    /**
     * Creates a <tt>LinkedBlockingDeque</tt> with a capacity of
     * {@link Integer#MAX_VALUE}, initially containing the elements of the given
     * collection, added in traversal order of the collection's iterator.
     *
     * @param c the collection of elements to initially contain
     * @throws NullPointerException if the specified collection or any of its elements are null
     */
    public FairLinkedBlockingDeque(Collection<? extends E> c) {
        this(Integer.MAX_VALUE);
        final ReentrantLock reentrantLock = this.lock;
        reentrantLock.lock(); // Never contended, but necessary for visibility
        try {
            for (E e : c) {
                if (e == null)
                    throw new NullPointerException();
                if (!linkLast(e))
                    throw new IllegalStateException("Deque full");
            }
        } finally {
            reentrantLock.unlock();
        }
    }

    /**
     * wait until the queue's size >= size
     */
    public void waitUtilCount(int size) {
        final ReentrantLock reentrantLock = this.lock;
        reentrantLock.lock();
        try {
            while (count < size) {
                try {
                    notEmpty.await();
                } catch (InterruptedException e) {
                    notEmpty.signalAll();
                }
            }
        } finally {
            reentrantLock.unlock();
        }
    }

    // Basic linking and unlinking operations, called only while holding lock

    /**
     * Links e as first element, or returns false if full.
     */
    private boolean linkFirst(E e) {
        // assert lock.isHeldByCurrentThread();
        if (count >= capacity)
            return false;
        Node<E> f = first;
        Node<E> x = new Node<>(e, null, f);
        first = x;
        if (last == null)
            last = x;
        else
            f.prev = x;
        ++count;
        notEmpty.signal();
        return true;
    }

    /**
     * Links e as last element, or returns false if full.
     */
    private boolean linkLast(E e) {
        // assert lock.isHeldByCurrentThread();
        if (count >= capacity)
            return false;
        Node<E> l = last;
        Node<E> x = new Node<>(e, l, null);
        last = x;
        if (first == null)
            first = x;
        else
            l.next = x;
        ++count;
        notEmpty.signal();
        return true;
    }

    /**
     * Removes and returns first element, or null if empty.
     */
    private E unlinkFirst() {
        // assert lock.isHeldByCurrentThread();
        Node<E> f = first;
        if (f == null)
            return null;
        Node<E> n = f.next;
        E item = f.item;
        f.item = null;
        f.next = f; // help GC
        first = n;
        if (n == null)
            last = null;
        else
            n.prev = null;
        --count;
        notFull.signal();
        return item;
    }

    /**
     * Removes and returns last element, or null if empty.
     */
    private E unlinkLast() {
        // assert lock.isHeldByCurrentThread();
        Node<E> l = last;
        if (l == null)
            return null;
        Node<E> p = l.prev;
        E item = l.item;
        l.item = null;
        l.prev = l; // help GC

        last = p;
        if (p == null)
            first = null;
        else
            p.next = null;
        --count;
        notFull.signal();
        return item;
    }

    /**
     * Unlinks x
     */
    void unlink(Node<E> x) {
        // assert lock.isHeldByCurrentThread();
        Node<E> p = x.prev;
        Node<E> n = x.next;
        if (p == null) {
            unlinkFirst();
        } else if (n == null) {
            unlinkLast();
        } else {
            p.next = n;
            n.prev = p;
            x.item = null;
            // Don't mess with x's links. They may still be in use by
            // an iterator.
            --count;
            notFull.signal();
        }
    }

    // BlockingDeque methods

    /**
     * @throws IllegalStateException {@inheritDoc}
     * @throws NullPointerException  {@inheritDoc}
     */
    public void addFirst(E e) {
        if (!offerFirst(e))
            throw new IllegalStateException("Deque full");
    }

    /**
     * @throws IllegalStateException {@inheritDoc}
     * @throws NullPointerException  {@inheritDoc}
     */
    public void addLast(E e) {
        if (!offerLast(e))
            throw new IllegalStateException("Deque full");
    }

    /**
     * @throws NullPointerException {@inheritDoc}
     */
    public boolean offerFirst(E e) {
        if (e == null)
            throw new NullPointerException();
        final ReentrantLock reentrantLock = this.lock;
        reentrantLock.lock();
        try {
            return linkFirst(e);
        } finally {
            reentrantLock.unlock();
        }
    }

    /**
     * @throws NullPointerException {@inheritDoc}
     * @throws InterruptedException {@inheritDoc}
     */
    public boolean offerFirst(E e, long timeout, TimeUnit unit) throws InterruptedException {
        if (e == null)
            throw new NullPointerException();
        long nanos = unit.toNanos(timeout);
        final ReentrantLock reentrantLock = this.lock;
        reentrantLock.lockInterruptibly();
        try {
            while (!linkFirst(e)) {
                if (nanos <= 0)
                    return false;
                nanos = notFull.awaitNanos(nanos);
            }
            return true;
        } finally {
            reentrantLock.unlock();
        }
    }

    /**
     * @throws NullPointerException {@inheritDoc}
     */
    public boolean offerLast(E e) {
        if (e == null)
            throw new NullPointerException();
        final ReentrantLock reentrantLock = this.lock;
        reentrantLock.lock();
        try {
            return linkLast(e);
        } finally {
            reentrantLock.unlock();
        }
    }

    /**
     * @throws NullPointerException {@inheritDoc}
     * @throws InterruptedException {@inheritDoc}
     */
    public boolean offerLast(E e, long timeout, TimeUnit unit) throws InterruptedException {
        if (e == null)
            throw new NullPointerException();
        long nanos = unit.toNanos(timeout);
        final ReentrantLock reentrantLock = this.lock;
        reentrantLock.lockInterruptibly();
        try {
            while (!linkLast(e)) {
                if (nanos <= 0)
                    return false;
                nanos = notFull.awaitNanos(nanos);
            }
            return true;
        } finally {
            reentrantLock.unlock();
        }
    }

    /**
     * @throws NullPointerException {@inheritDoc}
     * @throws InterruptedException {@inheritDoc}
     */
    public void putFirst(E e) throws InterruptedException {
        if (e == null)
            throw new NullPointerException();
        final ReentrantLock reentrantLock = this.lock;
        reentrantLock.lock();
        try {
            while (!linkFirst(e))
                notFull.await();
        } finally {
            reentrantLock.unlock();
        }
    }

    /**
     * @throws NullPointerException {@inheritDoc}
     * @throws InterruptedException {@inheritDoc}
     */
    public void putLast(E e) throws InterruptedException {
        if (e == null)
            throw new NullPointerException();
        final ReentrantLock reentrantLock = this.lock;
        reentrantLock.lock();
        try {
            while (!linkLast(e))
                notFull.await();
        } finally {
            reentrantLock.unlock();
        }
    }

    /**
     * @throws NoSuchElementException {@inheritDoc}
     */
    public E removeFirst() {
        E x = pollFirst();
        if (x == null)
            throw new NoSuchElementException();
        return x;
    }

    /**
     * @throws NoSuchElementException {@inheritDoc}
     */
    public E removeLast() {
        E x = pollLast();
        if (x == null)
            throw new NoSuchElementException();
        return x;
    }

    /**
     * add to last, if full, replace it
     */
    public E addOrReplaceLast(E e) throws InterruptedException {
        if (e == null)
            throw new NullPointerException();
        final ReentrantLock reentrantLock = this.lock;
        reentrantLock.lockInterruptibly();
        try {
            boolean added = linkLast(e);
            if (!added) {
                Node<E> l = last;
                if (l == null)
                    return null;
                E item = l.item;
                l.item = e;
                return item;
            } else {
                return null;
            }
        } finally {
            reentrantLock.unlock();
        }
    }

    public E pollFirst() {
        final ReentrantLock reentrantLock = this.lock;
        reentrantLock.lock();
        try {
            return unlinkFirst();
        } finally {
            reentrantLock.unlock();
        }
    }

    public E pollFirst(long timeout, TimeUnit unit) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        final ReentrantLock reentrantLock = this.lock;
        reentrantLock.lockInterruptibly();
        try {
            E x;
            while ((x = unlinkFirst()) == null) {
                if (nanos <= 0)
                    return null;
                nanos = notEmpty.awaitNanos(nanos);
            }
            return x;
        } finally {
            reentrantLock.unlock();
        }
    }

    public E pollLast() {
        final ReentrantLock reentrantLock = this.lock;
        reentrantLock.lock();
        try {
            return unlinkLast();
        } finally {
            reentrantLock.unlock();
        }
    }

    public E pollLast(long timeout, TimeUnit unit) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        final ReentrantLock reentrantLock = this.lock;
        reentrantLock.lockInterruptibly();
        try {
            E x;
            while ((x = unlinkLast()) == null) {
                if (nanos <= 0)
                    return null;
                nanos = notEmpty.awaitNanos(nanos);
            }
            return x;
        } finally {
            reentrantLock.unlock();
        }
    }

    public E takeFirst() throws InterruptedException {
        final ReentrantLock reentrantLock = this.lock;
        reentrantLock.lock();
        try {
            E x;
            while ((x = unlinkFirst()) == null)
                notEmpty.await();
            return x;
        } finally {
            reentrantLock.unlock();
        }
    }

    public E takeLast() throws InterruptedException {
        final ReentrantLock reentrantLock = this.lock;
        reentrantLock.lock();
        try {
            E x;
            while ((x = unlinkLast()) == null)
                notEmpty.await();
            return x;
        } finally {
            reentrantLock.unlock();
        }
    }

    /**
     * @throws NoSuchElementException {@inheritDoc}
     */
    public E getFirst() {
        E x = peekFirst();
        if (x == null)
            throw new NoSuchElementException();
        return x;
    }

    /**
     * @throws NoSuchElementException {@inheritDoc}
     */
    public E getLast() {
        E x = peekLast();
        if (x == null)
            throw new NoSuchElementException();
        return x;
    }

    public E peekFirst() {
        final ReentrantLock reentrantLock = this.lock;
        reentrantLock.lock();
        try {
            return (first == null) ? null : first.item;
        } finally {
            reentrantLock.unlock();
        }
    }

    public E peekLast() {
        final ReentrantLock reentrantLock = this.lock;
        reentrantLock.lock();
        try {
            return (last == null) ? null : last.item;
        } finally {
            reentrantLock.unlock();
        }
    }

    public boolean removeFirstOccurrence(Object o) {
        if (o == null)
            return false;
        final ReentrantLock reentrantLock = this.lock;
        reentrantLock.lock();
        try {
            for (Node<E> p = first; p != null; p = p.next) {
                if (o.equals(p.item)) {
                    unlink(p);
                    return true;
                }
            }
            return false;
        } finally {
            reentrantLock.unlock();
        }
    }

    public boolean removeLastOccurrence(Object o) {
        if (o == null)
            return false;
        final ReentrantLock reentrantLock = this.lock;
        reentrantLock.lock();
        try {
            for (Node<E> p = last; p != null; p = p.prev) {
                if (o.equals(p.item)) {
                    unlink(p);
                    return true;
                }
            }
            return false;
        } finally {
            reentrantLock.unlock();
        }
    }

    // BlockingQueue methods

    /**
     * Inserts the specified element at the end of this deque unless it would
     * violate capacity restrictions. When using a capacity-restricted deque, it
     * is generally preferable to use method {@link #offer(Object) offer}.
     * <p>
     * <p>
     * This method is equivalent to {@link #addLast}.
     *
     * @throws IllegalStateException if the element cannot be added at this time due to capacity
     *                               restrictions
     * @throws NullPointerException  if the specified element is null
     */
    public boolean add(E e) {
        addLast(e);
        return true;
    }

    /**
     * @throws NullPointerException if the specified element is null
     */
    public boolean offer(E e) {
        return offerLast(e);
    }

    /**
     * @throws NullPointerException {@inheritDoc}
     * @throws InterruptedException {@inheritDoc}
     */
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        return offerLast(e, timeout, unit);
    }

    /**
     * @throws NullPointerException {@inheritDoc}
     * @throws InterruptedException {@inheritDoc}
     */
    public void put(E e) throws InterruptedException {
        putLast(e);
    }

    /**
     * Retrieves and removes the head of the queue represented by this deque.
     * This method differs from {@link #poll poll} only in that it throws an
     * exception if this deque is empty.
     * <p>
     * <p>
     * This method is equivalent to {@link #removeFirst() removeFirst}.
     *
     * @return the head of the queue represented by this deque
     * @throws NoSuchElementException if this deque is empty
     */
    public E remove() {
        return removeFirst();
    }

    /**
     * Removes the first occurrence of the specified element from this deque. If
     * the deque does not contain the element, it is unchanged. More formally,
     * removes the first element <tt>e</tt> such that <tt>o.equals(e)</tt> (if
     * such an element exists). Returns <tt>true</tt> if this deque contained
     * the specified element (or equivalently, if this deque changed as a result
     * of the call).
     * <p>
     * <p>
     * This method is equivalent to {@link #removeFirstOccurrence(Object)
     * removeFirstOccurrence}.
     *
     * @param o element to be removed from this deque, if present
     * @return <tt>true</tt> if this deque changed as a result of the call
     */
    public boolean remove(Object o) {
        return removeFirstOccurrence(o);
    }

    public E poll() {
        return pollFirst();
    }

    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        return pollFirst(timeout, unit);
    }

    public E take() throws InterruptedException {
        return takeFirst();
    }

    /**
     * Retrieves, but does not remove, the head of the queue represented by this
     * deque. This method differs from {@link #peek peek} only in that it throws
     * an exception if this deque is empty.
     * <p>
     * <p>
     * This method is equivalent to {@link #getFirst() getFirst}.
     *
     * @return the head of the queue represented by this deque
     * @throws NoSuchElementException if this deque is empty
     */
    public E element() {
        return getFirst();
    }

    public E peek() {
        return peekFirst();
    }

    /**
     * Returns the number of additional elements that this deque can ideally (in
     * the absence of memory or resource constraints) accept without blocking.
     * This is always equal to the initial capacity of this deque less the
     * current <tt>size</tt> of this deque.
     * <p>
     * <p>
     * Note that you <em>cannot</em> always tell if an attempt to insert an
     * element will succeed by inspecting <tt>remainingCapacity</tt> because it
     * may be the case that another thread is about to insert or remove an
     * element.
     */
    public int remainingCapacity() {
        final ReentrantLock reentrantLock = this.lock;
        reentrantLock.lock();
        try {
            return capacity - count;
        } finally {
            reentrantLock.unlock();
        }
    }

    /**
     * @throws UnsupportedOperationException {@inheritDoc}
     * @throws ClassCastException            {@inheritDoc}
     * @throws NullPointerException          {@inheritDoc}
     * @throws IllegalArgumentException      {@inheritDoc}
     */
    public int drainTo(Collection<? super E> c) {
        return drainTo(c, Integer.MAX_VALUE);
    }

    /**
     * @throws UnsupportedOperationException {@inheritDoc}
     * @throws ClassCastException            {@inheritDoc}
     * @throws NullPointerException          {@inheritDoc}
     * @throws IllegalArgumentException      {@inheritDoc}
     */
    public int drainTo(Collection<? super E> c, int maxElements) {
        if (c == null)
            throw new NullPointerException();
        if (c == this)
            throw new IllegalArgumentException();
        final ReentrantLock reentrantLock = this.lock;
        reentrantLock.lock();
        try {
            int n = Math.min(maxElements, count);
            for (int i = 0; i < n; i++) {
                c.add(first.item); // In this order, in case add() throws.
                unlinkFirst();
            }
            return n;
        } finally {
            reentrantLock.unlock();
        }
    }

    // Stack methods

    /**
     * @throws IllegalStateException {@inheritDoc}
     * @throws NullPointerException  {@inheritDoc}
     */
    public void push(E e) {
        addFirst(e);
    }

    /**
     * @throws NoSuchElementException {@inheritDoc}
     */
    public E pop() {
        return removeFirst();
    }

    // Collection methods

    /**
     * Returns the number of elements in this deque.
     *
     * @return the number of elements in this deque
     */
    public int size() {
        final ReentrantLock reentrantLock = this.lock;
        reentrantLock.lock();
        try {
            return count;
        } finally {
            reentrantLock.unlock();
        }
    }

    /**
     * Returns <tt>true</tt> if this deque contains the specified element. More
     * formally, returns <tt>true</tt> if and only if this deque contains at
     * least one element <tt>e</tt> such that <tt>o.equals(e)</tt>.
     *
     * @param o object to be checked for containment in this deque
     * @return <tt>true</tt> if this deque contains the specified element
     */
    public boolean contains(Object o) {
        if (o == null)
            return false;
        final ReentrantLock reentrantLock = this.lock;
        reentrantLock.lock();
        try {
            for (Node<E> p = first; p != null; p = p.next)
                if (o.equals(p.item))
                    return true;
            return false;
        } finally {
            reentrantLock.unlock();
        }
    }

    /*
     * TODO: Add support for more efficient bulk operations.
     *
     * We don't want to acquire the lock for every iteration, but we also want
     * other threads a chance to interact with the collection, especially when
     * count is close to capacity.
     */

    // /**
    // * Adds all of the elements in the specified collection to this
    // * queue. Attempts to addAll of a queue to itself result in
    // * {@code IllegalArgumentException}. Further, the behavior of
    // * this operation is undefined if the specified collection is
    // * modified while the operation is in progress.
    // *
    // * @param c collection containing elements to be added to this queue
    // * @return {@code true} if this queue changed as a result of the call
    // * @throws ClassCastException {@inheritDoc}
    // * @throws NullPointerException {@inheritDoc}
    // * @throws IllegalArgumentException {@inheritDoc}
    // * @throws IllegalStateException {@inheritDoc}
    // * @see #add(Object)
    // */
    // public boolean addAll(Collection<? extends E> c) {
    // if (c == null)
    // throw new NullPointerException();
    // if (c == this)
    // throw new IllegalArgumentException();
    // final ReentrantLock lock = this.lock;
    // lock.lock();
    // try {
    // boolean modified = false;
    // for (E e : c)
    // if (linkLast(e))
    // modified = true;
    // return modified;
    // } finally {
    // lock.unlock();
    // }
    // }

    /**
     * Returns an array containing all of the elements in this deque, in proper
     * sequence (from first to last element).
     * <p>
     * <p>
     * The returned array will be "safe" in that no references to it are
     * maintained by this deque. (In other words, this method must allocate a
     * new array). The caller is thus free to modify the returned array.
     * <p>
     * <p>
     * This method acts as bridge between array-based and collection-based APIs.
     *
     * @return an array containing all of the elements in this deque
     */
    // @SuppressWarnings("unchecked")
    public Object[] toArray() {
        final ReentrantLock reentrantLock = this.lock;
        reentrantLock.lock();
        try {
            Object[] a = new Object[count];
            int k = 0;
            for (Node<E> p = first; p != null; p = p.next)
                a[k++] = p.item;
            return a;
        } finally {
            reentrantLock.unlock();
        }
    }

    /**
     * Returns an array containing all of the elements in this deque, in proper
     * sequence; the runtime type of the returned array is that of the specified
     * array. If the deque fits in the specified array, it is returned therein.
     * Otherwise, a new array is allocated with the runtime type of the
     * specified array and the size of this deque.
     * <p>
     * <p>
     * If this deque fits in the specified array with room to spare (i.e., the
     * array has more elements than this deque), the element in the array
     * immediately following the end of the deque is set to <tt>null</tt>.
     * <p>
     * <p>
     * Like the {@link #toArray()} method, this method acts as bridge between
     * array-based and collection-based APIs. Further, this method allows
     * precise control over the runtime type of the output array, and may, under
     * certain circumstances, be used to save allocation costs.
     * <p>
     * <p>
     * Suppose <tt>x</tt> is a deque known to contain only strings. The
     * following code can be used to dump the deque into a newly allocated array
     * of <tt>String</tt>:
     * <p>
     * <pre>
     * String[] y = x.toArray(new String[0]);
     * </pre>
     * <p>
     * Note that <tt>toArray(new Object[0])</tt> is identical in function to
     * <tt>toArray()</tt>.
     *
     * @param a the array into which the elements of the deque are to be
     *          stored, if it is big enough; otherwise, a new array of the
     *          same runtime type is allocated for this purpose
     * @return an array containing all of the elements in this deque
     * @throws ArrayStoreException  if the runtime type of the specified array is not a supertype
     *                              of the runtime type of every element in this deque
     * @throws NullPointerException if the specified array is null
     */
    public <T> T[] toArray(T[] a) {
        final ReentrantLock reentrantLock = this.lock;
        reentrantLock.lock();
        try {
            if (a.length < count)
                a = (T[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), count);

            int k = 0;
            for (Node<E> p = first; p != null; p = p.next)
                a[k++] = (T) p.item;
            if (a.length > k)
                a[k] = null;
            return a;
        } finally {
            reentrantLock.unlock();
        }
    }

    public String toString() {
        final ReentrantLock reentrantLock = this.lock;
        reentrantLock.lock();
        try {
            return super.toString();
        } finally {
            reentrantLock.unlock();
        }
    }

    /**
     * Atomically removes all of the elements from this deque. The deque will be
     * empty after this call returns.
     */
    public void clear() {
        final ReentrantLock reentrantLock = this.lock;
        reentrantLock.lock();
        try {
            for (Node<E> f = first; f != null; ) {
                f.item = null;
                Node<E> n = f.next;
                f.prev = null;
                f.next = null;
                f = n;
            }
            first = last = null;
            count = 0;
            notFull.signalAll();
        } finally {
            reentrantLock.unlock();
        }
    }

    /**
     * Returns an iterator over the elements in this deque in proper sequence.
     * The elements will be returned in order from first (head) to last (tail).
     * The returned <tt>Iterator</tt> is a "weakly consistent" iterator that
     * will never throw {@link ConcurrentModificationException}, and guarantees
     * to traverse elements as they existed upon construction of the iterator,
     * and may (but is not guaranteed to) reflect any modifications subsequent
     * to construction.
     *
     * @return an iterator over the elements in this deque in proper sequence
     */
    public Iterator<E> iterator() {
        return new Itr();
    }

    /**
     * Returns an iterator over the elements in this deque in reverse sequential
     * order. The elements will be returned in order from last (tail) to first
     * (head). The returned <tt>Iterator</tt> is a "weakly consistent" iterator
     * that will never throw {@link ConcurrentModificationException}, and
     * guarantees to traverse elements as they existed upon construction of the
     * iterator, and may (but is not guaranteed to) reflect any modifications
     * subsequent to construction.
     */
    public Iterator<E> descendingIterator() {
        return new DescendingItr();
    }

    /**
     * Save the state of this deque to a stream (that is, serialize it).
     *
     * @param s the stream
     * @serialData The capacity (int), followed by elements (each an
     * <tt>Object</tt>) in the proper order, followed by a null
     */
    private void writeObject(java.io.ObjectOutputStream s) throws java.io.IOException {
        final ReentrantLock reentrantLock = this.lock;
        reentrantLock.lock();
        try {
            // Write out capacity and any hidden stuff
            s.defaultWriteObject();
            // Write out all elements in the proper order.
            for (Node<E> p = first; p != null; p = p.next)
                s.writeObject(p.item);
            // Use trailing null as sentinel
            s.writeObject(null);
        } finally {
            reentrantLock.unlock();
        }
    }

    /**
     * Reconstitute this deque from a stream (that is, deserialize it).
     *
     * @param s the stream
     */
    private void readObject(java.io.ObjectInputStream s) throws java.io.IOException, ClassNotFoundException {
        s.defaultReadObject();
        count = 0;
        first = null;
        last = null;
        // Read in all elements and place in queue
        for (; ; ) {
            // @SuppressWarnings("unchecked")
            E item = (E) s.readObject();
            if (item == null)
                break;
            add(item);
        }
    }

    /**
     * Base class for Iterators for LinkedBlockingDeque
     */
    private abstract class AbstractItr implements Iterator<E> {
        /**
         * The next node to return in next()
         */
        Node<E> next;

        /**
         * nextItem holds on to item fields because once we claim that an
         * element exists in hasNext(), we must return item read under lock (in
         * advance()) even if it was in the process of being removed when
         * hasNext() was called.
         */
        E nextItem;

        /**
         * Node returned by most recent call to next. Needed by remove. Reset to
         * null if this element is deleted by a call to remove.
         */
        private Node<E> lastRet;

        abstract Node<E> firstNode();

        abstract Node<E> nextNode(Node<E> n);

        AbstractItr() {
            // set to initial position
            final ReentrantLock reentrantLock = FairLinkedBlockingDeque.this.lock;
            reentrantLock.lock();
            try {
                next = firstNode();
                nextItem = (next == null) ? null : next.item;
            } finally {
                reentrantLock.unlock();
            }
        }

        /**
         * Advances next.
         */

        void advance() {
            final ReentrantLock reentrantLock = FairLinkedBlockingDeque.this.lock;
            reentrantLock.lock();
            try {
                // assert next != null;
                Node<E> s = nextNode(next);
                if (s == next) {
                    next = firstNode();
                } else {
                    // Skip over removed nodes.
                    // May be necessary if multiple interior Nodes are removed.
                    while (s != null && s.item == null)
                        s = nextNode(s);
                    next = s;
                }
                nextItem = (next == null) ? null : next.item;
            } finally {
                reentrantLock.unlock();
            }
        }

        public boolean hasNext() {
            return next != null;
        }

        public E next() {
            if (next == null)
                throw new NoSuchElementException();
            lastRet = next;
            E x = nextItem;
            advance();
            return x;
        }

        public void remove() {
            Node<E> n = lastRet;
            if (n == null)
                throw new IllegalStateException();
            lastRet = null;

            final ReentrantLock reentrantLock = FairLinkedBlockingDeque.this.lock;
            reentrantLock.lock();
            try {
                if (n.item != null)
                    unlink(n);
            } finally {
                reentrantLock.unlock();
            }
        }
    }

    /**
     * Forward iterator
     */
    private class Itr extends AbstractItr {
        Node<E> firstNode() {
            return first;
        }

        Node<E> nextNode(Node<E> n) {
            return n.next;
        }
    }

    /**
     * Descending iterator
     */
    private class DescendingItr extends AbstractItr {
        Node<E> firstNode() {
            return last;
        }

        Node<E> nextNode(Node<E> n) {
            return n.prev;
        }
    }

    /**
     * Doubly-linked list node class
     */
    static final class Node<E> {
        /**
         * The item, or null if this node has been removed.
         */

        E item;

        /**
         * One of: - the real predecessor Node - this Node, meaning the
         * predecessor is tail - null, meaning there is no predecessor
         */

        Node<E> prev;

        /**
         * One of: - the real successor Node - this Node, meaning the successor
         * is head - null, meaning there is no successor
         */

        Node<E> next;

        Node(E x, Node<E> p, Node<E> n) {
            item = x;
            prev = p;
            next = n;
        }
    }
}
