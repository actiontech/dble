/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.config.util;

import java.util.*;

/**
 * @author mycat
 */
public class OrderRetainingMap<K, V> extends HashMap<K, V> {
    private static final long serialVersionUID = 1L;

    private Set<K> keyOrder = new ArraySet<>();
    private List<V> valueOrder = new ArrayList<>();

    @Override
    public V put(K key, V value) {
        keyOrder.add(key);
        valueOrder.add(value);
        return super.put(key, value);
    }

    @Override
    public Collection<V> values() {
        return Collections.unmodifiableList(valueOrder);
    }

    @Override
    public Set<K> keySet() {
        return Collections.unmodifiableSet(keyOrder);
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        throw new UnsupportedOperationException();
    }

    /**
     * @author mycat
     */
    private static class ArraySet<T> extends ArrayList<T> implements Set<T> {

        private static final long serialVersionUID = 1L;
    }

}
