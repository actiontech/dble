package com.actiontech.dble.net.netty;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.backend.mysql.nio.MySQLConnectionAuthenticator;
import com.actiontech.dble.backend.mysql.nio.MySQLDataSource;
import com.actiontech.dble.backend.mysql.nio.handler.ResponseHandler;
import com.actiontech.dble.config.model.DBHostConfig;
import com.actiontech.dble.net.NIOProcessor;
import com.actiontech.dble.net.SocketConnector;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.handler.codec.bytes.ByteArrayDecoder;
import io.netty.handler.codec.bytes.ByteArrayEncoder;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by szf on 2019/7/5.
 */
public class NettyConnector extends Thread implements SocketConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyAcceptor.class);
    private final EventLoopGroup group;

    public NettyConnector(int backendProcessorCount) {
        this.group = new EpollEventLoopGroup(backendProcessorCount);
    }

    public MySQLConnection createNewConnection(MySQLDataSource pool, String schema, ResponseHandler handler) {

        DBHostConfig dsc = pool.getConfig();
        CreateCallBack callBack = new CreateCallBack();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group).
                    channel(EpollSocketChannel.class).
                    handler(new ChannelInitializer<io.netty.channel.epoll.EpollSocketChannel>() {
                        @Override
                        public void initChannel(io.netty.channel.epoll.EpollSocketChannel ch) throws Exception {
                            ChannelPipeline p = ch.pipeline();
                            MySQLConnection c = new MySQLConnection(p, pool.isReadNode(), schema == null);
                            c.setSocketParams(false);
                            c.setHost(dsc.getIp());
                            c.setPort(dsc.getPort());
                            c.setUser(dsc.getUser());
                            c.setPassword(dsc.getPassword());
                            c.setSchema(schema);
                            c.setHandler(new MySQLConnectionAuthenticator(c, handler));
                            c.setPool(pool);
                            c.setIdleTimeout(pool.getConfig().getIdleTimeout());
                            p.addLast("framer", new MySQLPacketBasedFrameDecoder(65535, 0, 3));
                            p.addLast("idleStateHandler", new IdleStateHandler(30, 30, 10));
                            p.addLast("decoder", new ByteArrayDecoder());
                            p.addLast("encoder", new ByteArrayEncoder());
                            p.addLast(new NettyBackHandler(c));
                            NIOProcessor processor = DbleServer.getInstance().nextBackendProcessor();
                            c.setProcessor(processor);
                            callBack.setConn(c);
                        }
                    });

            // Start the client.
            ChannelFuture f = b.connect(dsc.getIp(), dsc.getPort()).sync();
            return callBack.getConn();
        } catch (Exception e) {
            LOGGER.info("create connection error", e);
            callBack.getConn().close("create connection error");
            callBack.getConn().onConnectFailed(e);
        }
        return null;
    }


    private class CreateCallBack {
        private volatile MySQLConnection conn;

        private MySQLConnection getConn() {
            return conn;
        }

        private void setConn(MySQLConnection conn) {
            this.conn = conn;
        }
    }
}
