/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.common.item.function;

import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.plan.common.exception.MySQLOutPutException;
import com.oceanbase.obsharding_d.plan.common.item.Item;

import java.util.*;
import java.util.function.UnaryOperator;

/**
 * @author dcy
 * Create Date: 2021-01-08
 */
public class ItemListDelegate implements List<Item> {
    List<Item> list;
    String funName;

    public ItemListDelegate(List<Item> list, String funName) {
        this.list = list;
        this.funName = funName;
    }

    @Override
    public int size() {
        return list.size();
    }

    @Override
    public boolean isEmpty() {
        return list.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return list.contains(o);
    }


    @Override
    public Iterator<Item> iterator() {
        return list.iterator();
    }


    @Override
    public Object[] toArray() {
        return list.toArray();
    }


    @Override
    public <T> T[] toArray(T[] a) {
        return list.toArray(a);
    }

    @Override
    public boolean add(Item item) {
        return list.add(item);
    }

    @Override
    public void add(int index, Item element) {
        list.add(index, element);
    }

    @Override
    public boolean remove(Object o) {
        return list.remove(o);
    }

    @Override
    public Item remove(int index) {
        return list.remove(index);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return list.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends Item> c) {
        return list.addAll(c);
    }

    @Override
    public boolean addAll(int index, Collection<? extends Item> c) {
        return list.addAll(index, c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return list.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return list.retainAll(c);
    }

    @Override
    public void replaceAll(UnaryOperator<Item> operator) {
        list.replaceAll(operator);
    }

    @Override
    public void sort(Comparator<? super Item> c) {
        list.sort(c);
    }

    @Override
    public void clear() {
        list.clear();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof ItemListDelegate))
            return false;
        return list.equals(obj);
    }

    @Override
    public int hashCode() {
        return list.hashCode();
    }

    @Override
    public Item get(int index) {
        try {
            return list.get(index);
        } catch (IndexOutOfBoundsException e) {
            throw new MySQLOutPutException(ErrorCode.ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT, "42000", "Incorrect parameter count in the call to native function '" + funName.toUpperCase() + "'");
        }

    }

    @Override
    public Item set(int index, Item element) {
        return list.set(index, element);
    }


    @Override
    public int indexOf(Object o) {
        return list.indexOf(o);
    }

    @Override
    public int lastIndexOf(Object o) {
        return list.lastIndexOf(o);
    }


    @Override
    public ListIterator<Item> listIterator() {
        return list.listIterator();
    }


    @Override
    public ListIterator<Item> listIterator(int index) {
        return list.listIterator(index);
    }


    @Override
    public List<Item> subList(int fromIndex, int toIndex) {
        return list.subList(fromIndex, toIndex);
    }

    @Override
    public Spliterator<Item> spliterator() {
        return list.spliterator();
    }
}
