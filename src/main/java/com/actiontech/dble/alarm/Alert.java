/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.alarm;

import java.util.Map;

public interface Alert {
    enum AlertLevel {
        NOTICE, WARN, CRITICAL
    }
    void alertSelf(String code, AlertLevel level, String desc, Map<String, String> labels);
    void alert(String code, AlertLevel level, String desc, String alertComponentType, String alertComponentId, Map<String, String> labels);
    boolean alertResolve(String code, AlertLevel level, String alertComponentType, String alertComponentId, Map<String, String> labels);
    boolean alertSelfResolve(String code, AlertLevel level, Map<String, String> labels);
}
