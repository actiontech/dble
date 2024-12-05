/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.util;

import java.util.*;

/**
 * @author mycat
 */
public final class CollectionUtil {
    private CollectionUtil() {
    }

    /**
     * @param orig if null, return intersect
     */
    public static Set<?> intersectSet(Set<?> orig, Set<?> intersect) {
        if (orig == null) {
            return intersect;
        }
        if (intersect == null || orig.isEmpty()) {
            return Collections.emptySet();
        }
        Set<Object> set = new HashSet<>(orig.size());
        for (Object p : orig) {
            if (intersect.contains(p)) {
                set.add(p);
            }
        }
        return set;
    }

    public static boolean isEmpty(Collection<?> collection) {
        return collection == null || collection.isEmpty();
    }

    public static boolean isEmpty(Map<?, ?> map) {
        return map == null || map.isEmpty();
    }

    public static boolean containDuplicate(Collection list, Object obj) {
        boolean findOne = false;
        for (Object x : list) {
            if (x.equals(obj)) {
                if (!findOne) {
                    findOne = true;
                } else {
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean contaionSpecificObject(Collection collection, Object obj) {
        for (Object x : collection) {
            if (x == obj) {
                return true;
            }
        }
        return false;
    }


    public static boolean equalsWithEmpty(Collection org1, Collection org2) {
        if (isEmpty(org1)) {
            return isEmpty(org2);
        }
        return org1.equals(org2);
    }

    public static Set<String> retainAll(List<Set<String>> sets) {
        Set<String> set = new HashSet<>(sets.get(0));
        for (int i = 1; i < sets.size(); i++) {
            if (isEmpty(sets)) return null;
            set.retainAll(sets.get(i));
        }
        return set;
    }
}
