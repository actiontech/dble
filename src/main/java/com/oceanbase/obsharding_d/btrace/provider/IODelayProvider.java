/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.btrace.provider;

import com.oceanbase.obsharding_d.net.service.Service;
import com.oceanbase.obsharding_d.net.service.ServiceTask;

/**
 * @author dcy
 * Create Date: 2021-05-13
 */
public final class IODelayProvider {
    private IODelayProvider() {
    }


    public static void beforeErrorResponse(Service service) {

    }


    public static void inReadReachEnd() {

    }

    /**
     * begin create the fake close packet
     *
     * @param serviceTask
     * @param service
     */
    public static void beforePushInnerServiceTask(ServiceTask serviceTask, Service service) {

    }

    /**
     * before create the normal packet
     *
     * @param serviceTask
     * @param service
     */
    public static void beforePushServiceTask(ServiceTask serviceTask, Service service) {

    }


    /**
     * begin process the fake close packet
     *
     * @param serviceTask
     * @param service
     */
    public static void beforeInnerClose(ServiceTask serviceTask, Service service) {

    }

    /**
     * after process the fake close packet and the connection is closed.
     *
     * @param serviceTask
     * @param service
     */
    public static void afterImmediatelyClose(ServiceTask serviceTask, Service service) {

    }

}
