/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

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
