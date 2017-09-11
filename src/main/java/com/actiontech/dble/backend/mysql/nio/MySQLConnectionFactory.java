/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.backend.mysql.nio;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.config.model.DBHostConfig;
import com.actiontech.dble.net.NIOConnector;
import com.actiontech.dble.net.factory.BackendConnectionFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.NetworkChannel;

/**
 * @author mycat
 */
public class MySQLConnectionFactory extends BackendConnectionFactory {
    @SuppressWarnings({"unchecked", "rawtypes"})
    public MySQLConnection make(MySQLDataSource pool, ResponseHandler handler,
                                String schema) throws IOException {

        DBHostConfig dsc = pool.getConfig();
        NetworkChannel channel = openSocketChannel(DbleServer.getInstance().isAIO());

        MySQLConnection c = new MySQLConnection(channel, pool.isReadNode());
        DbleServer.getInstance().getConfig().setSocketParams(c, false);
        c.setHost(dsc.getIp());
        c.setPort(dsc.getPort());
        c.setUser(dsc.getUser());
        c.setPassword(dsc.getPassword());
        c.setSchema(schema);
        c.setHandler(new MySQLConnectionAuthenticator(c, handler));
        c.setPool(pool);
        c.setIdleTimeout(pool.getConfig().getIdleTimeout());
        if (channel instanceof AsynchronousSocketChannel) {
            ((AsynchronousSocketChannel) channel).connect(
                    new InetSocketAddress(dsc.getIp(), dsc.getPort()), c,
                    (CompletionHandler) DbleServer.getInstance().getConnector());
        } else {
            ((NIOConnector) DbleServer.getInstance().getConnector()).postConnect(c);
        }
        return c;
    }

}
