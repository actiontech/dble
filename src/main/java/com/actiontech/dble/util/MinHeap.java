package com.actiontech.dble.util;

import java.util.Collection;

public interface MinHeap<E> extends Collection<E> {

    E poll();

    E peak();

    void replaceTop(E e);

    E find(E e);
}
