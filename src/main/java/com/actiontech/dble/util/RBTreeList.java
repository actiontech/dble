/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.util;

import java.util.*;

public class RBTreeList<E> extends AbstractList<E> {

    private final Comparator<E> comparator;
    private static final boolean RED = false;
    private static final boolean BLACK = true;

    private Object[] elementData;
    private int size;
    private RBTNode<E> root;
    private List<E> orderList = null;

    public RBTreeList(int initialCapacity, Comparator<E> comparator) {
        super();
        this.comparator = comparator;
        this.elementData = new Object[initialCapacity];
    }

    public void ensureCapacity(int minCapacity) {
        modCount++;
        int oldCapacity = elementData.length;
        if (minCapacity > oldCapacity) {
            Object[] oldData = elementData;
            int newCapacity = (oldCapacity * 3) / 2 + 1;
            newCapacity = Math.max(newCapacity, minCapacity);
            // minCapacity is usually close to size, so this is a win:
            elementData = Arrays.copyOf(oldData, newCapacity);
        }
    }

    @Override
    public boolean add(E e) {
        ensureCapacity(size + 1); // Increments modCount!!
        int index = size++;
        elementData[index] = e;
        RBTNode<E> node = new RBTNode<>(index, BLACK, e);
        insert(node);
        return true;
    }

    @Override
    public void add(int index, E element) {
        if (index > size || index < 0)
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);

        ensureCapacity(size + 1); // Increments modCount!!
        System.arraycopy(elementData, index, elementData, index + 1, size - index);
        elementData[index] = element;
        RBTNode<E> node = new RBTNode<>(index, BLACK, element);
        insert(node);
        size++;
    }

    private void insert(RBTNode<E> node) {
        if (root == null) {
            root = node;
            return;
        }

        int cmp;
        RBTNode<E> x = this.root;
        RBTNode<E> parent;
        do {
            parent = x;
            cmp = comparator.compare(node.getValue(), x.getValue());
            if (cmp < 0)
                x = x.left;
            else
                x = x.right;
        } while (x != null);
        node.parent = parent;
        if (cmp < 0)
            parent.left = node;
        else
            parent.right = node;

        fixAfterInsertion(node);
    }

    private void fixAfterInsertion(RBTNode<E> x) {
        x.color = RED;
        while (x != null && x != root && x.parent.color == RED) {
            // parent is grandparent's left child
            if (parentOf(x) == leftOf(parentOf(parentOf(x)))) {

                RBTNode<E> y = rightOf(parentOf(parentOf(x)));
                // uncle's color is red
                if (colorOf(y) == RED) {
                    setColor(parentOf(x), BLACK);
                    setColor(y, BLACK);
                    setColor(parentOf(parentOf(x)), RED);
                    x = parentOf(parentOf(x));
                } else {
                    if (x == rightOf(parentOf(x))) {
                        x = parentOf(x);
                        rotateLeft(x);
                    }
                    setColor(parentOf(x), BLACK);
                    setColor(parentOf(parentOf(x)), RED);
                    rotateRight(parentOf(parentOf(x)));
                }
            } else {
                RBTNode<E> y = leftOf(parentOf(parentOf(x)));
                if (colorOf(y) == RED) {
                    setColor(parentOf(x), BLACK);
                    setColor(y, BLACK);
                    setColor(parentOf(parentOf(x)), RED);
                    x = parentOf(parentOf(x));
                } else {
                    if (x == leftOf(parentOf(x))) {
                        x = parentOf(x);
                        rotateRight(x);
                    }
                    setColor(parentOf(x), BLACK);
                    setColor(parentOf(parentOf(x)), RED);
                    rotateLeft(parentOf(parentOf(x)));
                }
            }
        }
        root.color = BLACK;
    }

    /**
     * <pre>
     *
     *      px                              px
     *     /                               /
     *    x                               y
     *   /  \      --(rotate left)-.     / \                #
     *  lx   y                          x  ry
     *     /   \                       /  \
     *    ly   ry                     lx  ly
     * </pre>
     *
     * @param p
     */
    private void rotateLeft(RBTNode<E> p) {
        if (p != null) {
            RBTNode<E> r = p.right;
            p.right = r.left;
            if (r.left != null)
                r.left.parent = p;
            r.parent = p.parent;
            if (p.parent == null)
                root = r;
            else if (p.parent.left == p)
                p.parent.left = r;
            else
                p.parent.right = r;
            r.left = p;
            p.parent = r;
        }
    }

    /**
     * <pre>
     *
     *            py                               py
     *           /                                /
     *          y                                x
     *         /  \      --(rotate right)-.     /  \                     #
     *        x   ry                           lx   y
     *       / \                                   / \                   #
     *      lx  rx                                rx  ry
     * </pre>
     *
     * @param p
     */
    private void rotateRight(RBTNode<E> p) {
        if (p != null) {
            RBTNode<E> l = p.left;
            p.left = l.right;
            if (l.right != null)
                l.right.parent = p;
            l.parent = p.parent;
            if (p.parent == null)
                root = l;
            else if (p.parent.right == p)
                p.parent.right = l;
            else
                p.parent.left = l;
            l.right = p;
            p.parent = l;
        }
    }

    private boolean colorOf(RBTNode<E> node) {
        return (node == null ? BLACK : node.color);
    }

    private RBTNode<E> parentOf(RBTNode<E> node) {
        return (node == null ? null : node.parent);
    }

    private void setColor(RBTNode<E> node, boolean c) {
        if (node != null)
            node.color = c;
    }

    private RBTNode<E> leftOf(RBTNode<E> node) {
        return (node == null) ? null : node.left;
    }

    private RBTNode<E> rightOf(RBTNode<E> node) {
        return (node == null) ? null : node.right;
    }

    @Override
    public E set(int index, E element) {
        rangeCheck(index);
        E oldValue = (E) elementData[index];
        elementData[index] = element;
        RBTNode<E> oldNode = find(oldValue);
        delete(oldNode);
        RBTNode<E> newNode = new RBTNode<>(index, BLACK, element);
        insert(newNode);
        return oldValue;
    }

    @Override
    public E remove(int index) {
        rangeCheck(index);

        modCount++;
        E oldValue = (E) elementData[index];

        int numMoved = size - index - 1;
        if (numMoved > 0)
            System.arraycopy(elementData, index + 1, elementData, index, numMoved);
        elementData[--size] = null; // Let gc do its work
        RBTNode<E> node = find(oldValue);
        if (node != null)
            delete(node);
        return oldValue;
    }

    /**
     * find the minimum node which value >= t.value
     *
     * @param t
     * @return
     */
    private RBTNode<E> successor(RBTNode<E> t) {
        if (t == null)
            return null;

        if (t.right != null) {
            RBTNode<E> p = t.right;
            while (p.left != null)
                p = p.left;
            return p;
        }

        RBTNode<E> p = t.parent;
        RBTNode<E> ch = t;
        // only child is parent's left node can return parent
        while (p != null && ch == p.right) {
            ch = p;
            p = p.parent;
        }
        return p;

    }

    /**
     * find the maximum node which value <= t.value
     *
     * @param t
     * @return
     */
    public RBTNode<E> predecessor(RBTNode<E> t) {
        if (t == null)
            return null;

        if (t.left != null) {
            RBTNode<E> p = t.left;
            while (p.right != null)
                p = p.right;
            return p;
        }

        RBTNode<E> p = t.parent;
        RBTNode<E> ch = t;
        // only child is parent's right node can return parent
        while (p != null && ch == p.left) {
            ch = p;
            p = p.parent;
        }
        return p;

    }

    private void delete(RBTNode<E> node) {
        // If strictly internal, copy successor's element to node and then make
        // p
        // point to successor.
        if (node.left != null && node.right != null) {
            RBTNode<E> s = successor(node);
            node.index = s.index;
            node.value = s.value;
            node = s;
        } // node has 2 children

        // Start fixup at replacement node, if it exists.
        RBTNode<E> replacement = (node.left != null ? node.left : node.right);

        if (replacement != null) {
            // Link replacement to parent
            replacement.parent = node.parent;
            if (node.parent == null)
                root = replacement;
            else if (node == node.parent.left)
                node.parent.left = replacement;
            else
                node.parent.right = replacement;

            // Null out links so they are OK to use by fixAfterDeletion.
            node.left = node.right = node.parent = null;

            // Fix replacement
            if (node.color == BLACK)
                fixAfterDeletion(replacement);
        } else if (node.parent == null) { // return if we are the only node.
            root = null;
        } else { // No children. Use self as phantom replacement and unlink.
            if (node.color == BLACK)
                fixAfterDeletion(node);

            if (node.parent != null) {
                if (node == node.parent.left)
                    node.parent.left = null;
                else if (node == node.parent.right)
                    node.parent.right = null;
                node.parent = null;
            }
        }
    }

    private void fixAfterDeletion(RBTNode<E> x) {
        while (x != root && colorOf(x) == BLACK) {
            if (x == leftOf(parentOf(x))) {
                RBTNode<E> sib = rightOf(parentOf(x));

                if (colorOf(sib) == RED) {
                    setColor(sib, BLACK);
                    setColor(parentOf(x), RED);
                    rotateLeft(parentOf(x));
                    sib = rightOf(parentOf(x));
                }

                if (colorOf(leftOf(sib)) == BLACK && colorOf(rightOf(sib)) == BLACK) {
                    setColor(sib, RED);
                    x = parentOf(x);
                } else {
                    if (colorOf(rightOf(sib)) == BLACK) {
                        setColor(leftOf(sib), BLACK);
                        setColor(sib, RED);
                        rotateRight(sib);
                        sib = rightOf(parentOf(x));
                    }
                    setColor(sib, colorOf(parentOf(x)));
                    setColor(parentOf(x), BLACK);
                    setColor(rightOf(sib), BLACK);
                    rotateLeft(parentOf(x));
                    x = root;
                }
            } else { // symmetric
                RBTNode<E> sib = leftOf(parentOf(x));

                if (colorOf(sib) == RED) {
                    setColor(sib, BLACK);
                    setColor(parentOf(x), RED);
                    rotateRight(parentOf(x));
                    sib = leftOf(parentOf(x));
                }

                if (colorOf(rightOf(sib)) == BLACK && colorOf(leftOf(sib)) == BLACK) {
                    setColor(sib, RED);
                    x = parentOf(x);
                } else {
                    if (colorOf(leftOf(sib)) == BLACK) {
                        setColor(rightOf(sib), BLACK);
                        setColor(sib, RED);
                        rotateLeft(sib);
                        sib = leftOf(parentOf(x));
                    }
                    setColor(sib, colorOf(parentOf(x)));
                    setColor(parentOf(x), BLACK);
                    setColor(leftOf(sib), BLACK);
                    rotateRight(parentOf(x));
                    x = root;
                }
            }
        }

        setColor(x, BLACK);
    }

    private RBTNode<E> find(E e) {
        RBTNode<E> t = root;
        if (t == null)
            return t;
        while (t != null) {
            int cmp = comparator.compare(e, t.value);
            if (cmp < 0)
                t = t.left;
            else if (cmp > 0)
                t = t.right;
            else
                return t;
        }
        return t;

    }

    @Override
    public boolean contains(Object o) {
        return indexOf(o) >= 0;
    }

    @Override
    public int indexOf(Object o) {
        RBTNode<E> t = find((E) o);
        if (t == null)
            return -1;
        return t.getIndex();
    }

    @Override
    public void clear() {
        modCount++;

        // Let gc do its work
        for (int i = 0; i < size; i++)
            elementData[i] = null;

        destroy(root);
        root = null;
        size = 0;
    }

    private void destroy(RBTNode<E> node) {
        if (node == null)
            return;

        if (node.left != null) {
            destroy(node.left);
            node.left = null;
        }
        if (node.right != null) {
            destroy(node.right);
            node.right = null;
        }
        node.parent = null;
        node.value = null;
    }

    @Override
    public boolean addAll(int index, Collection<? extends E> c) {
        if (index > size || index < 0)
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);

        Object[] a = c.toArray();
        int numNew = a.length;
        ensureCapacity(size + numNew); // Increments modCount

        int numMoved = size - index;
        if (numMoved > 0)
            System.arraycopy(elementData, index, elementData, index + numNew, numMoved);

        System.arraycopy(a, 0, elementData, index, numNew);
        for (int i = 0; i < a.length; i++) {
            int idx = index + i;
            RBTNode<E> node = new RBTNode<>(idx, BLACK, (E) a[i]);
            insert(node);
        }
        size += numNew;
        return numNew != 0;
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        Object[] a = c.toArray();
        int numNew = a.length;
        ensureCapacity(size + numNew); // Increments modCount
        System.arraycopy(a, 0, elementData, size, numNew);
        for (int i = 0; i < a.length; i++) {
            int idx = size + i;
            RBTNode<E> node = new RBTNode<>(idx, BLACK, (E) a[i]);
            insert(node);
        }
        size += numNew;
        return numNew != 0;
    }

    @Override
    public E get(int index) {
        rangeCheck(index);
        return (E) elementData[index];
    }

    @Override
    public int size() {
        return this.size;
    }

    @Override
    public Object[] toArray() {
        Object[] obj = inOrder();
        return Arrays.copyOf(obj, size);
    }

    @Override
    public <T> T[] toArray(T[] a) {
        Object[] obj = inOrder();
        if (a.length < size)
            // Make a new array of a's runtime type, but my contents:
            return (T[]) Arrays.copyOf(obj, size, a.getClass());
        System.arraycopy(obj, 0, a, 0, size);
        if (a.length > size)
            a[size] = null;
        return a;
    }

    private void inOrder(RBTNode<E> node, List<E> list) {
        if (node != null) {
            inOrder(node.left, list);
            list.add(node.getValue());
            inOrder(node.right, list);
        }
    }

    private Object[] inOrder() {
        List<E> list = new ArrayList<>(size);
        inOrder(root, list);
        return list.toArray();
    }

    private void rangeCheck(int index) {
        if (index >= size)
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
    }

    public E inOrderOf(int index) {
        if (orderList == null) {
            List<E> list = new ArrayList<>(size);
            inOrder(root, list);
            orderList = list;
        }
        return orderList.get(index);
    }

    static class RBTNode<E> {
        private int index;
        private boolean color;
        private E value;
        private RBTNode<E> left;
        private RBTNode<E> right;
        private RBTNode<E> parent;

        RBTNode(int index, boolean color, E value) {
            this.index = index;
            this.color = color;
            this.value = value;
        }

        public E getValue() {
            return value;
        }

        public int getIndex() {
            return index;
        }

    }

}
