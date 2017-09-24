/*
* Copyright (C) 2016-2017 ActionTech.
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
}
