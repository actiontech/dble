package com.actiontech.dble.net;

import com.actiontech.dble.util.TimeUtil;
import io.netty.channel.ChannelPipeline;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by szf on 2019/7/3.
 */
public class NettySocketWR extends SocketWR {

    protected final AbstractConnection con;
    private final ChannelPipeline channelPipeline;
    private final AtomicBoolean writing = new AtomicBoolean(false);

    public NettySocketWR(AbstractConnection conn, ChannelPipeline channelPipeline) {
        this.con = conn;
        this.channelPipeline = channelPipeline;
    }

    @Override
    public void asyncRead() throws IOException {
        // no asyncRead
    }

    @Override
    public void doNextWriteCheck() {

        if (!writing.compareAndSet(false, true)) {
            return;
        }
        try {
            write0();
            writing.set(false);
        } catch (IOException e) {
            con.close("err:" + e);
        }
    }

    @Override
    public boolean registerWrite(ByteBuffer buffer) {
        // final ChannelFuture future = ctx.write(buffer);
        int length = buffer.position();
        byte[] x = new byte[length];
        buffer.flip();
        buffer.get(x, 0, length);
        channelPipeline.writeAndFlush(x);
        return false;
    }


    private boolean write0() throws IOException {
        ByteBuffer buffer = con.writeBuffer;
        if (buffer != null) {
            while (buffer.hasRemaining()) {
                int length = buffer.position();
                byte[] x = new byte[length];
                buffer.get(x, 0, length);
                channelPipeline.writeAndFlush(x);
                con.lastWriteTime = TimeUtil.currentTimeMillis();
            }

            if (buffer.hasRemaining()) {
                return false;
            } else {
                con.writeBuffer = null;
                con.recycle(buffer);
            }
        }
        while ((buffer = con.writeQueue.poll()) != null) {
            if (buffer.limit() == 0) {
                con.recycle(buffer);
                con.close("quit send");
                return true;
            }

            int length = buffer.position();
            byte[] x = new byte[length];
            buffer.flip();
            buffer.get(x, 0, length);
            channelPipeline.writeAndFlush(x);
            con.lastWriteTime = TimeUtil.currentTimeMillis();

            con.recycle(buffer);
        }
        return true;
    }
}
