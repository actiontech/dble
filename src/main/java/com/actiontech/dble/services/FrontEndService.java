/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services;

import com.actiontech.dble.config.Capabilities;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.mysql.AuthPacket;
import com.actiontech.dble.net.service.AuthResultInfo;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.singleton.FrontendUserManager;
import com.actiontech.dble.statistic.CommandCount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class FrontEndService extends VariablesService {
    private static final Logger LOGGER = LoggerFactory.getLogger(FrontEndService.class);

    protected volatile String schema;
    protected UserName user;
    protected UserConfig userConfig;
    protected final CommandCount commands;

    public FrontEndService(AbstractConnection connection) {
        super(connection);
        this.commands = connection.getProcessor().getCommands();
    }

    public void initFromAuthInfo(AuthResultInfo info) {
        AuthPacket auth = info.getMysqlAuthPacket();
        this.user = new UserName(auth.getUser(), auth.getTenant());
        this.schema = info.getMysqlAuthPacket().getDatabase();
        this.userConfig = info.getUserConfig();
        this.connection.initCharsetIndex(info.getMysqlAuthPacket().getCharsetIndex());
        boolean clientCompress = Capabilities.CLIENT_COMPRESS == (Capabilities.CLIENT_COMPRESS & auth.getClientFlags());
        boolean usingCompress = SystemConfig.getInstance().getUseCompression() == 1;
        if (clientCompress && usingCompress) {
            this.setSupportCompress(true);
        }
        if (LOGGER.isDebugEnabled()) {
            StringBuilder s = new StringBuilder();
            s.append(this).append('\'').append(auth.getUser()).append("' login success");
            byte[] extra = auth.getExtra();
            if (extra != null && extra.length > 0) {
                s.append(",extra:").append(new String(extra));
            }
            LOGGER.debug(s.toString());
        }
    }

    public void userConnectionCount() {
        FrontendUserManager.getInstance().countDown(user, this instanceof ManagerService);
    }

    public UserName getUser() {
        return user;
    }

    public UserConfig getUserConfig() {
        return userConfig;
    }

    public abstract String getExecuteSql();

    public abstract void killAndClose(String reason);

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }
}
