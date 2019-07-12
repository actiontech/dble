package com.actiontech.dble.net.netty;

import com.actiontech.dble.net.AbstractConnection;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Created by szf on 2019/7/3.
 */
public class NettyFrontHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyFrontHandler.class);
    private final AbstractConnection c;

    NettyFrontHandler(AbstractConnection c) {
        this.c = c;
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        c.close("frontend closed");
        ctx.fireChannelUnregistered();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        LOGGER.warn("get Netty stack ++++", new Exception());
        byte[] data = (byte[]) msg;
        c.handle(data);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {

        //in netty mode only greeting will be write out
        ctx.flush();
        TimeUnit.MILLISECONDS.sleep(200);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}
