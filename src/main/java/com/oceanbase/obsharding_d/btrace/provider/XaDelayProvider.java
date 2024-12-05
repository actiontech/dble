/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.btrace.provider;

public final class XaDelayProvider {

    private XaDelayProvider() {
    }

    public static void delayBeforeXaStart(String dnName, String xaId) {

    }

    public static void delayBeforeXaEnd(String dnName, String xaId) {

    }

    public static void delayBeforeXaPrepare(String dnName, String xaId) {

    }

    public static void delayBeforeXaCommit(String dnName, String xaId) {

    }

    public static void delayBeforeXaRollback(String dnName, String xaId) {

    }

    public static void beforeAddXaToQueue(int count, String xaId) {

    }

    public static void afterAddXaToQueue(int count, String xaId) {

    }

    public static void beforeInnerRetry(int count, String xaId) {

    }

}
