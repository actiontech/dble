/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.util;

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

}
