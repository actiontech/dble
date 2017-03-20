/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese 
 * opensource volunteers. you can redistribute it and/or modify it under the 
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * Any questions about this component can be directed to it's project Web address 
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.net.handler;

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.Set;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import io.mycat.MycatServer;
import io.mycat.backend.mysql.SecurityUtil;
import io.mycat.config.Capabilities;
import io.mycat.config.ErrorCode;
import io.mycat.net.FrontendConnection;
import io.mycat.net.NIOHandler;
import io.mycat.net.NIOProcessor;
import io.mycat.net.mysql.AuthPacket;
import io.mycat.net.mysql.MySQLPacket;
import io.mycat.net.mysql.QuitPacket;

/**
 * manager认证处理器
 * 
 * @author mycat
 */
public class ManagerAuthenticator extends FrontendAuthenticator {
	
    private static final Logger LOGGER = LoggerFactory.getLogger(ManagerAuthenticator.class);
    private static final byte[] AUTH_OK = new byte[] { 7, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0 };
    
    // protected final FrontendConnection source;

    public ManagerAuthenticator(FrontendConnection source) {
	super(source);
    }

    @Override
     protected void success(AuthPacket auth) {
        source.setAuthenticated(true);
        source.setUser(auth.user);
        source.setSchema(auth.database);
        source.setCharsetIndex(auth.charsetIndex);
        source.setHandler(new ManagerCommandHandler(source));

        if (LOGGER.isInfoEnabled()) {
            StringBuilder s = new StringBuilder();
            s.append(source).append('\'').append(auth.user).append("' login success");
            byte[] extra = auth.extra;
            if (extra != null && extra.length > 0) {
                s.append(",extra:").append(new String(extra));
            }
            LOGGER.info(s.toString());
        }

        ByteBuffer buffer = source.allocate();
        source.write(source.writeToBuffer(AUTH_OK, buffer));
        boolean clientCompress = Capabilities.CLIENT_COMPRESS==(Capabilities.CLIENT_COMPRESS & auth.clientFlags);
        boolean usingCompress= MycatServer.getInstance().getConfig().getSystem().getUseCompression()==1 ;
        if(clientCompress&&usingCompress)
        {
            source.setSupportCompress(true);
        }
    }
}
