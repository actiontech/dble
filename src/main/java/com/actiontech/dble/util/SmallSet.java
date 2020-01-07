/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.util;

import java.io.Serializable;
import java.util.*;

/**
 * usually one element
 *
 * @author mycat
 */
public final class SmallSet<E> extends AbstractSet<E> implements Set<E>, Cloneable, Serializable {

    private static final long serialVersionUID = 2037649294658559180L;

    private final int initSize;
    private ArrayList<E> list;
    private E single;
    private int size;

    public SmallSet() {
        this(2);
    }

    public SmallSet(int initSize) {
        this.initSize = initSize;
    }

    @Override
    public boolean add(E e) {
        switch (size) {
            case 0:
                ++size;
                single = e;
                return true;
            case 1:
                if (isEquals(e, single)) {
                    return false;
                }
                list = new ArrayList<>(initSize);
                list.add(single);
                list.add(e);
                ++size;
                return true;
            default:
                for (E e1 : list) {
                    if (isEquals(e1, e)) {
                        return false;
                    }
                }
                list.add(e);
                ++size;
                return true;
        }
    }

    private boolean isEquals(E e1, E e2) {
        if (e1 == null) {
            return e2 == null;
        }
        return e1.equals(e2);
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    @Override
    public Iterator<E> iterator() {
        return new Iterator<E>() {
            private int i;
            private boolean next;

            @Override
            public boolean hasNext() {
                return i < size;
            }

            @Override
            public E next() {
                next = true;
                switch (size) {
                    case 0:
                        throw new NoSuchElementException();
                    case 1:
                        switch (i) {
                            case 0:
                                ++i;
                                return single;
                            default:
                                throw new NoSuchElementException();
                        }
                    default:
                        try {
                            E e = list.get(i);
                            ++i;
                            return e;
                        } catch (IndexOutOfBoundsException e) {
                            throw new NoSuchElementException(e.getMessage());
                        }
                }
            }

            @Override
            public void remove() {
                if (!next) {
                    throw new IllegalStateException();
                }
                switch (size) {
                    case 0:
                        throw new IllegalStateException();
                    case 1:
                        size = i = 0;
                        single = null;
                        if (list != null && !list.isEmpty()) {
                            list.remove(0);
                        }
                        break;
                    default:
                        //list can not be null
                        list.remove(--i);
                        if (--size == 1) {
                            single = list.get(0);
                        }
                        break;
                }
                next = false;
            }
        };
    }

    @Override
    public int size() {
        return size;
    }

}
