package io.mycat.util;

import java.util.Collection;

public interface MinHeap<E> extends Collection<E> {

	public E poll();

	public E peak();

	public void replaceTop(E e);

	public E find(E e);
}