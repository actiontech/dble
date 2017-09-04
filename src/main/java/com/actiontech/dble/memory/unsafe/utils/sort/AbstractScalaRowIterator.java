package com.actiontech.dble.memory.unsafe.utils.sort;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created by zagnix 2016/6/6.
 */
public class AbstractScalaRowIterator<T> implements Iterator<T> {
    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public T next() {
        throw new NoSuchElementException();
    }

    @Override
    public void remove() {

    }
}
