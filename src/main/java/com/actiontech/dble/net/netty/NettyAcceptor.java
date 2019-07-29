package com.actiontech.dble.net.netty;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.net.NIOProcessor;
import com.actiontech.dble.net.SocketAcceptor;
import com.actiontech.dble.net.factory.FrontendConnectionFactory;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.handler.codec.bytes.ByteArrayDecoder;
import io.netty.handler.codec.bytes.ByteArrayEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by szf on 2019/7/3.
 */
public class NettyAcceptor extends Thread implements SocketAcceptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyAcceptor.class);
    private final int port;
    // private final AsynchronousServerSocketChannel serverChannel;
    private final FrontendConnectionFactory factory;
    private final EventLoopGroup workerGroup;


    public NettyAcceptor(String name, String ip, int port, int backlog,
                         FrontendConnectionFactory factory, EventLoopGroup workerGroup)
            throws IOException {
        super.setName(name);
        this.port = port;
        this.factory = factory;
        this.workerGroup = workerGroup;
    }


    @Override
    public void run() {
        EventLoopGroup bossGroup = new EpollEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup).
                    channel(EpollServerSocketChannel.class).
                    childHandler(new ChannelInitializer<io.netty.channel.socket.SocketChannel>() {
                        @Override
                        public void initChannel(io.netty.channel.socket.SocketChannel ch) throws Exception {
                            FrontendConnection c = factory.make(ch.pipeline());
                            c.setAccepted(true);
                            c.setId(ch.getClass().hashCode());
                            NIOProcessor processor = DbleServer.getInstance().nextFrontProcessor();
                            c.setProcessor(processor);
                            ch.pipeline().addLast("framer", new MySQLPacketBasedFrameDecoder(65535, 0, 3));
                            ch.pipeline().addLast("decoder", new ByteArrayDecoder());
                            ch.pipeline().addLast("encoder", new ByteArrayEncoder());
                            ch.pipeline().addLast(new NettyFrontHandler(c));
                            c.register();
                        }
                    }).
                    option(ChannelOption.SO_BACKLOG, 128).
                    childOption(ChannelOption.SO_KEEPALIVE, true);

            ChannelFuture f = b.bind(new InetSocketAddress("0.0.0.0", port)).sync();
            LOGGER.info("netty start listen in port " + port);
            f.channel().closeFuture().sync();
        } catch (Throwable e) {
            LOGGER.error("ERROR in netty server " + this.getName() + " close");
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }


    public int getPort() {
        return port;
    }
}
