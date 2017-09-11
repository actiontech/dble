/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author mycat
 */
public class ConcurrentHashMapMain {

    private final ConcurrentMap<String, String> cm;

    public ConcurrentHashMapMain() {
        cm = new ConcurrentHashMap<String, String>();
        cm.put("abcdefg", "abcdefghijk");
    }

    public void tGet() {
        for (int i = 0; i < 1000000; i++) {
            cm.get("abcdefg");
        }
    }

    public void tGetNone() {
        for (int i = 0; i < 1000000; i++) {
            cm.get("abcdefghijk");
        }
    }

    public void tEmpty() {
        for (int i = 0; i < 1000000; i++) {
            cm.isEmpty();
        }
    }

    public void tRemove() {
        for (int i = 0; i < 1000000; i++) {
            cm.remove("abcdefg");
        }
    }

}