/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.util;

import com.actiontech.dble.util.MinHeap;

import java.util.*;

@SuppressWarnings("unchecked")
public class ArrayMinHeap<E> implements MinHeap<E> {

    private static final int DEFAULT_INITIAL_CAPACITY = 3;

    private Object[] heap;
    private int size = 0;
    private Comparator<E> comparator;

    public ArrayMinHeap(Comparator<E> comparator) {
        this(DEFAULT_INITIAL_CAPACITY, comparator);
    }

    public ArrayMinHeap(int initialCapacity, Comparator<E> comparator) {
        if (initialCapacity < 1)
            throw new IllegalArgumentException();
        this.heap = new Object[initialCapacity];
        this.comparator = comparator;
    }

    public E find(E e) {
        if (e != null) {
            for (int i = 0; i < size; i++)
                if (comparator.compare(e, (E) heap[i]) == 0)
                    return (E) heap[i];
        }
        return null;
    }

    @Override
    public int size() {
        return this.size;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean contains(Object o) {
        return indexOf(o) != -1;
    }

    @Override
    public Iterator<E> iterator() {
        return new Itr();
    }

    @Override
    public Object[] toArray() {
        return Arrays.copyOf(heap, size);
    }

    @Override
    public <T> T[] toArray(T[] a) {
        if (a.length < size)
            // Make a new array of a's runtime type, but my contents:
            return (T[]) Arrays.copyOf(heap, size, a.getClass());
        System.arraycopy(heap, 0, a, 0, size);
        if (a.length > size)
            a[size] = null;
        return a;
    }

    private void grow(int minCapacity) {
        if (minCapacity < 0) // overflow
            throw new OutOfMemoryError();
        int oldCapacity = heap.length;
        // Double size if small; else grow by 50%
        int newCapacity = ((oldCapacity < 64) ? ((oldCapacity + 1) * 2) : ((oldCapacity / 2) * 3));
        if (newCapacity < 0) // overflow int
            newCapacity = Integer.MAX_VALUE;
        if (newCapacity < minCapacity)
            newCapacity = minCapacity;
        heap = Arrays.copyOf(heap, newCapacity);
    }

    @Override
    public boolean add(E e) {
        return offer(e);
    }

    @Override
    public void replaceTop(E e) {
        if (size == 0)
            return;

        siftDown(0, e);
    }

    public boolean offer(E e) {
        if (e == null)
            throw new NullPointerException();
        int i = size;
        if (i >= heap.length)
            grow(i + 1);
        size = i + 1;
        if (i == 0)
            heap[0] = e;
        else
            siftUp(i, e);
        return true;
    }

    private void siftUp(int k, E x) {
        while (k > 0) {
            int parent = (k - 1) >>> 1;
            Object e = heap[parent];
            if (comparator.compare(x, (E) e) >= 0)
                break;
            heap[k] = e;
            k = parent;
        }
        heap[k] = x;
    }

    private int indexOf(Object o) {
        if (o != null) {
            for (int i = 0; i < size; i++)
                if (o.equals(heap[i]))
                    return i;
        }
        return -1;
    }

    @Override
    public E peak() {
        if (size == 0)
            return null;
        return (E) heap[0];
    }

    @Override
    public E poll() {
        if (size == 0)
            return null;
        int s = --size;
        E result = (E) heap[0];
        E x = (E) heap[s];
        heap[s] = null;
        if (s != 0)
            siftDown(0, x);
        return result;
    }

    private E removeAt(int i) {
        assert i >= 0 && i < size;
        int s = --size;
        if (s == i) // removed last element
            heap[i] = null;
        else {
            E moved = (E) heap[s];
            heap[s] = null;
            siftDown(i, moved);
            if (heap[i] == moved) {
                siftUp(i, moved);
                if (heap[i] != moved)
                    return moved;
            }
        }
        return null;
    }

    private void siftDown(int k, E x) {
        // the last element's parent index
        int half = size >>> 1;
        while (k < half) {
            int child = (k << 1) + 1;
            Object c = heap[child];
            int right = child + 1;
            if (right < size && comparator.compare((E) c, (E) heap[right]) > 0)
                c = heap[child = right];
            if (comparator.compare(x, (E) c) <= 0)
                break;
            heap[k] = c;
            k = child;
        }
        heap[k] = x;
    }

    boolean removeEq(Object o) {
        for (int i = 0; i < size; i++) {
            if (o == heap[i]) {
                removeAt(i);
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean remove(Object o) {
        int i = indexOf(o);
        if (i == -1)
            return false;
        else {
            removeAt(i);
            return true;
        }
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        for (Object aC : c)
            if (!contains(aC))
                return false;
        return true;
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        boolean modified = false;
        for (E aC : c) {
            if (add(aC))
                modified = true;
        }
        return modified;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        boolean modified = false;
        Iterator<?> e = iterator();
        while (e.hasNext()) {
            if (c.contains(e.next())) {
                e.remove();
                modified = true;
            }
        }
        return modified;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        boolean modified = false;
        Iterator<E> e = iterator();
        while (e.hasNext()) {
            if (!c.contains(e.next())) {
                e.remove();
                modified = true;
            }
        }
        return modified;
    }

    @Override
    public void clear() {
        while (poll() != null) {
            //do nothing
        }
    }

    private final class Itr implements Iterator<E> {
        /**
         * Index (into queue array) of element to be returned by subsequent call
         * to next.
         */
        private int cursor = 0;

        /**
         * Index of element returned by most recent call to next, unless that
         * element came from the forgetMeNot list. Set to -1 if element is
         * deleted by a call to remove.
         */
        private int lastRet = -1;

        /**
         * A queue of elements that were moved from the unvisited portion of the
         * heap into the visited portion as a result of "unlucky" element
         * removals during the iteration. (Unlucky element removals are those
         * that require a siftup instead of a siftdown.) We must visit all of
         * the elements in this list to complete the iteration. We do this after
         * we've completed the "normal" iteration.
         * <p>
         * We expect that most iterations, even those involving removals, will
         * not need to store elements in this field.
         */
        private ArrayDeque<E> forgetMeNot = null;

        /**
         * Element returned by the most recent call to next iff that element was
         * drawn from the forgetMeNot list.
         */
        private E lastRetElt = null;

        public boolean hasNext() {
            return cursor < size || (forgetMeNot != null && !forgetMeNot.isEmpty());
        }

        public E next() {
            if (cursor < size)
                return (E) heap[lastRet = cursor++];
            if (forgetMeNot != null) {
                lastRet = -1;
                lastRetElt = forgetMeNot.poll();
                if (lastRetElt != null)
                    return lastRetElt;
            }
            throw new NoSuchElementException();
        }

        public void remove() {
            if (lastRet != -1) {
                E moved = ArrayMinHeap.this.removeAt(lastRet);
                lastRet = -1;
                if (moved == null)
                    cursor--;
                else {
                    if (forgetMeNot == null)
                        forgetMeNot = new ArrayDeque<>();
                    forgetMeNot.add(moved);
                }
            } else if (lastRetElt != null) {
                ArrayMinHeap.this.removeEq(lastRetElt);
                lastRetElt = null;
            } else {
                throw new IllegalStateException();
            }
        }
    }

}
