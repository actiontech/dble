/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.util;

import com.actiontech.dble.util.MinHeap;

import java.util.*;

@SuppressWarnings("unchecked")
public class RBTMinHeap<E> implements MinHeap<E> {

    private static final boolean RED = false;
    private static final boolean BLACK = true;

    private RBTNode<E> root;
    private int size = 0;
    private Comparator<E> comparator;

    public RBTMinHeap(Comparator<E> comparator) {
        this.comparator = comparator;
    }

    public E find(E e) {
        RBTNode<E> node = search(e);
        if (node == null)
            return null;
        return node.value;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean contains(Object o) {
        return search((E) o) == null;
    }

    private RBTNode<E> search(E e) {
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
    public Iterator<E> iterator() {
        throw new RuntimeException("unsupport iterator in RBTMinHeap");
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

    @Override
    public boolean add(E e) {
        size++;
        RBTNode<E> node = new RBTNode<>(BLACK, e);
        insert(node);
        return true;
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

    /**
     * unused current version
     */
    @Override
    public E peak() {
        RBTNode<E> minNode = findMin(root);
        if (minNode == null)
            return null;
        E e = minNode.value;
        return e;
    }

    /**
     * need optimizer, unused current version
     */
    @Override
    public void replaceTop(E e) {
        // find minNode
        RBTNode<E> minNode = findMin(root);
        if (minNode == null)
            return;
        // delete minNode
        delete(minNode);
        // add minNode
        RBTNode<E> node = new RBTNode<>(BLACK, e);
        insert(node);
    }

    @Override
    public E poll() {
        RBTNode<E> minNode = findMin(root);
        if (minNode == null)
            return null;
        size--;
        E e = minNode.value;
        delete(minNode);
        return e;
    }

    private RBTNode<E> findMin(RBTNode<E> node) {
        if (node == null)
            return null;
        while (node.left != null) {
            node = node.left;
        }
        return node;
    }

    @Override
    public boolean remove(Object o) {
        size--;
        RBTNode<E> node = search((E) o);
        if (node != null)
            delete(node);
        return true;
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

    private void delete(RBTNode<E> node) {
        // If strictly internal, copy successor's element to node and then make
        // p
        // point to successor.
        if (node.left != null && node.right != null) {
            RBTNode<E> s = successor(node);
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
        destory(root);
        root = null;
        size = 0;
    }

    private void destory(RBTNode<E> node) {
        if (node == null)
            return;

        if (node.left != null) {
            destory(node.left);
            node.left = null;
        }
        if (node.right != null) {
            destory(node.right);
            node.right = null;
        }
        node.parent = null;
        node.value = null;
    }

    static class RBTNode<E> {
        private boolean color;
        private E value;
        private RBTNode<E> left;
        private RBTNode<E> right;
        private RBTNode<E> parent;

        RBTNode(boolean color, E value) {
            this.color = color;
            this.value = value;
        }

        public E getValue() {
            return value;
        }

    }
}
