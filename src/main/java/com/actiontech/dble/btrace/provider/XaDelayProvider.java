/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.btrace.provider;

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
