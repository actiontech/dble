package com.actiontech.dble.log.general;

import com.actiontech.dble.services.FrontendService;

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
