/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config;

import com.actiontech.dble.config.model.UserConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import static com.actiontech.dble.config.FrontendUserManager.CheckStatus.OK;
import static com.actiontech.dble.config.FrontendUserManager.CheckStatus.SERVER_MAX;
import static com.actiontech.dble.config.FrontendUserManager.CheckStatus.USER_MAX;

/**
 * Created by szf on 2018/6/27.
 */
public final class FrontendUserManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(FrontendUserManager.class);

    private static final FrontendUserManager INSTANCE = new FrontendUserManager();

    private ReentrantLock maxConLock = new ReentrantLock();

    private Map<String, Integer> userConnectionMap = new ConcurrentHashMap<>();

    private int serverMaxConnection;

    private int serverConnection = 0;

    private FrontendUserManager() {

    }

    public void countDown(String user, boolean isManager) {
        maxConLock.lock();
        try {
            if (!isManager) {
                serverConnection--;
            }
            int usercount = userConnectionMap.get(user).intValue();
            userConnectionMap.put(user, --usercount);
        } catch (Throwable e) {
            //error ignore
            LOGGER.warn("Frontend lose", e);
        } finally {
            maxConLock.unlock();
        }
    }

    public void initForLatest(Map<String, UserConfig> userConfigMap, int serverLimit) {
        serverMaxConnection = serverLimit;
        for (String userName : userConfigMap.keySet()) {
            if (!userConnectionMap.containsKey(userName)) {
                userConnectionMap.put(userName, 0);
            }
        }

    }


    public CheckStatus maxConnectionCheck(String user, int userLimit, boolean isManager) {

        maxConLock.lock();
        try {
            int userConnection = userConnectionMap.get(user);
            if (userLimit > 0) {
                if (userConnection >= userLimit) {
                    return USER_MAX;
                }
            }
            if (!isManager) {
                if (serverMaxConnection > 0 && serverMaxConnection <= serverConnection) {
                    return SERVER_MAX;
                }
                serverConnection++;
            }
            userConnectionMap.put(user, ++userConnection);

        } catch (Throwable e) {
            //error ignore
            LOGGER.warn("user maxCon check", e);
        } finally {
            maxConLock.unlock();
        }

        return OK;
    }

    public enum CheckStatus {
        OK, SERVER_MAX, USER_MAX
    }

    public static FrontendUserManager getInstance() {
        return INSTANCE;
    }
}
