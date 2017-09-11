/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.util;

import java.util.HashMap;
import java.util.Map;

/**
 * @author mycat
 */
public class HashMapMain {

    public void t() {
        String[] keys = new String[]{"a", "b", "c", "d", "e"};
        long t = System.currentTimeMillis();
        int count = 1000000;
        Map<String, String> m = new HashMap<String, String>();
        t = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            for (String key : keys) {
                m.put(key, "String.value");
            }
            for (String key : keys) {
                m.remove(key);
            }
        }
        System.out.println((System.currentTimeMillis() - t) * 1000 * 1000 / (count * keys.length * 2) + " ns");
    }

}