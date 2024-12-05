/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.btrace.provider;

public final class GeneralProvider {
    private GeneralProvider() {
    }

    public static void getGeneralLogQueueSize(int queueSize) {
    }

    public static void onOffGeneralLog() {
    }

    public static void updateGeneralLogFile() {
    }

    public static void showGeneralLog() {
    }

    public static void beforeAuthSuccess() {

    }

    public static void beforeChangeUserSuccess() {

    }

    // Because issues-1231, so this btrace method is required for validation
    public static void sqlJobDoFinished() {

    }


    public static void afterDelayServiceMarkDoing() {

    }

    public static void showTableByNodeUnitHandlerFinished() {
    }

    public static void beforeSlowLogClose() {
    }

    public static void afterSlowLogClose() {
    }

    public static void runFlushLogTask() {
    }
}
