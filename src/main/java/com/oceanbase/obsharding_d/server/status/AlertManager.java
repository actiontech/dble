/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.server.status;

import com.oceanbase.obsharding_d.alarm.AlertBlockQueue;
import com.oceanbase.obsharding_d.alarm.AlertSender;
import com.oceanbase.obsharding_d.alarm.AlertTask;
import com.oceanbase.obsharding_d.util.ExecutorUtil;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;

/**
 * Created by szf on 2019/3/25.
 */
public class AlertManager {
    private BlockingQueue<AlertTask> alertQueue = new AlertBlockQueue<>(1024);
    private static final AlertManager INSTANCE = new AlertManager();
    private ExecutorService alertSenderExecutor;

    public static AlertManager getInstance() {
        return INSTANCE;
    }

    public BlockingQueue<AlertTask> getAlertQueue() {
        return alertQueue;
    }

    public void startAlert() {
        alertSenderExecutor = ExecutorUtil.createCached("alertSenderExecutor", 1);
        alertSenderExecutor.execute(new AlertSender(alertQueue));
    }


}
