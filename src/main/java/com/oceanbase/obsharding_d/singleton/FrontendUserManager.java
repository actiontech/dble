/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.singleton;

import com.oceanbase.obsharding_d.config.model.user.UserConfig;
import com.oceanbase.obsharding_d.config.model.user.UserName;
import com.oceanbase.obsharding_d.services.manager.response.ChangeItem;
import com.oceanbase.obsharding_d.services.manager.response.ChangeItemType;
import com.oceanbase.obsharding_d.services.manager.response.ChangeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import static com.oceanbase.obsharding_d.singleton.FrontendUserManager.CheckStatus.*;

/**
 * Created by szf on 2018/6/27.
 */
public final class FrontendUserManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(FrontendUserManager.class);

    private static final FrontendUserManager INSTANCE = new FrontendUserManager();

    private ReentrantLock maxConLock = new ReentrantLock();

    private Map<UserName, Integer> userConnectionMap = new ConcurrentHashMap<>();

    private int serverMaxConnection;

    private int serverConnection = 0;

    private FrontendUserManager() {

    }

    public void countDown(UserName user, boolean isManager) {
        maxConLock.lock();
        try {
            if (!isManager) {
                serverConnection--;
            }
            int userCount = userConnectionMap.get(user) - 1;
            userConnectionMap.put(user, userCount);
        } catch (Throwable e) {
            //error ignore
            LOGGER.warn("Frontend lose", e);
        } finally {
            maxConLock.unlock();
        }
    }

    public void initForLatest(Map<UserName, UserConfig> userConfigMap, int serverLimit) {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("init-for-user-manager");
        try {
            serverMaxConnection = serverLimit;
            for (UserName user : userConfigMap.keySet()) {
                if (!userConnectionMap.containsKey(user)) {
                    userConnectionMap.put(user, 0);
                }
            }
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }

    public void changeUser(List<ChangeItem> changeItemList, int serverLimit) {
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("init-for-user-manager");
        try {
            serverMaxConnection = serverLimit;

            for (ChangeItem changeItem : changeItemList) {
                ChangeType type = changeItem.getType();
                Object item = changeItem.getItem();
                boolean isUser = changeItem.getItemType() == ChangeItemType.USERNAME;
                if (!isUser) {
                    continue;
                }
                UserName userName = (UserName) item;
                if (type == ChangeType.ADD) {
                    //add
                    if (!userConnectionMap.containsKey(userName)) {
                        userConnectionMap.put(userName, 0);
                    }
                } else if (type == ChangeType.DELETE) {
                    //delete
                    if (userConnectionMap.containsKey(userName)) {
                        userConnectionMap.remove(userName);
                    }
                }
            }
        } finally {
            TraceManager.finishSpan(traceObject);
        }
    }


    public CheckStatus maxConnectionCheck(UserName user, int userLimit, boolean isManager) {

        maxConLock.lock();
        try {
            Integer userConnection = userConnectionMap.get(user);
            if (null == userConnection) {
                userConnection = 0;
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("user:" + user + ",userLimit=" + userLimit + ",userConnection=" + userConnection);
            }
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

    public Map<UserName, Integer> getUserConnectionMap() {
        return userConnectionMap;
    }

    public enum CheckStatus {
        OK, SERVER_MAX, USER_MAX
    }

    public static FrontendUserManager getInstance() {
        return INSTANCE;
    }
}
