/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.route.parser.util;

/**
 * (created at 2010-7-21)
 *
 * @author mycat
 */
public final class Pair<K, V> {

    private final K key;
    private final V value;

    public Pair(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "(" + key + ", " + value + ")";
    }

    private static final int HASH_CONST = 37;

    @Override
    public int hashCode() {
        int hash = 17;
        if (key == null) {
            hash += HASH_CONST;
        } else {
            hash = hash << 5 + hash << 1 + hash + key.hashCode();
        }
        if (value == null) {
            hash += HASH_CONST;
        } else {
            hash = hash << 5 + hash << 1 + hash + value.hashCode();
        }
        return hash;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Pair)) {
            return false;
        }
        Pair that = (Pair) obj;
        return isEquals(this.key, that.key) && isEquals(this.value, that.value);
    }

    private boolean isEquals(Object o1, Object o2) {
        if (o1 == o2) {
            return true;
        }
        if (o1 == null) {
            return o2 == null;
        }
        return o1.equals(o2);
    }

}
