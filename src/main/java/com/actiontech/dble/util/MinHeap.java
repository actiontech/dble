/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.util;

import java.util.Collection;

public interface MinHeap<E> extends Collection<E> {

    E poll();

    E peak();

    void replaceTop(E e);

    E find(E e);
}
