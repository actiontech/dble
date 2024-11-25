/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.log.general;

import com.oceanbase.obsharding_d.services.FrontendService;

public final class GeneralLogHelper {
    private GeneralLogHelper() {
    }

    public static void putGLog(FrontendService service, byte[] data) {
        GeneralLogProcessor.getInstance().putGeneralLog(service, data);
    }

    public static void putGLog(FrontendService service, String type, String sql) {
        GeneralLogProcessor.getInstance().putGeneralLog(service.getConnection().getId(), type, sql);
    }

    public static void putGLog(String content) {
        GeneralLogProcessor.getInstance().putGeneralLog(content);
    }

    public static void putGLog(long connID, String type, String sql) {
        GeneralLogProcessor.getInstance().putGeneralLog(connID, type, sql);
    }
}
