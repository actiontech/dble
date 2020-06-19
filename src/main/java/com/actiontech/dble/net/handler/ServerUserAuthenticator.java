/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.net.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.Capabilities;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.user.ServerUserConfig;
import com.actiontech.dble.config.model.user.ShardingUserConfig;
import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.net.mysql.AuthPacket;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.server.ServerConnection;
import com.actiontech.dble.server.ServerQueryHandler;
import com.actiontech.dble.server.handler.ServerLoadDataInfileHandler;
import com.actiontech.dble.server.handler.ServerPrepareHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ManagerAuthenticator
 *
 * @author mycat
 */
public class ServerUserAuthenticator extends FrontendAuthenticator {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerUserAuthenticator.class);
    public ServerUserAuthenticator(ServerConnection source) {
        super(source);
    }

    protected void setConnProperties(AuthPacket auth) {
        UserName user = new UserName(auth.getUser(), auth.getTenant());
        UserConfig userConfig = DbleServer.getInstance().getConfig().getUsers().get(user);
        ServerConnection sc = (ServerConnection) source;
        sc.setUserConfig((ServerUserConfig) userConfig);
        sc.setUser(user);
        if (userConfig instanceof ShardingUserConfig) {
            sc.setQueryHandler(new ServerQueryHandler(sc));
            sc.setLoadDataInfileHandler(new ServerLoadDataInfileHandler(sc));
            sc.setPrepareHandler(new ServerPrepareHandler(sc));
            SystemConfig sys = SystemConfig.getInstance();
            sc.setTxIsolation(sys.getTxIsolation());
            sc.setSession2(new NonBlockingSession(sc));
            sc.getSession2().setRowCount(0);
            sc.setAuthenticated(true);
            sc.setSchema(auth.getDatabase());
            sc.initCharsetIndex(auth.getCharsetIndex());
            sc.setHandler(new ShardingUserCommandHandler(sc));
            sc.setMultiStatementAllow(auth.isMultStatementAllow());
            sc.setClientFlags(auth.getClientFlags());
            boolean clientCompress = Capabilities.CLIENT_COMPRESS == (Capabilities.CLIENT_COMPRESS & auth.getClientFlags());
            boolean usingCompress = SystemConfig.getInstance().getUseCompression() == 1;
            if (clientCompress && usingCompress) {
                sc.setSupportCompress(true);
            }
            if (LOGGER.isDebugEnabled()) {
                StringBuilder s = new StringBuilder();
                s.append(sc).append('\'').append(auth.getUser()).append("' login success");
                byte[] extra = auth.getExtra();
                if (extra != null && extra.length > 0) {
                    s.append(",extra:").append(new String(extra));
                }
                LOGGER.debug(s.toString());
            }
        }
    }
}
