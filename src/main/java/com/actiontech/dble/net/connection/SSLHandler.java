package com.actiontech.dble.net.connection;

import com.actiontech.dble.net.service.*;
import com.actiontech.dble.net.factory.SSLEngineFactory;
import com.actiontech.dble.util.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.NetworkChannel;
import java.nio.channels.SocketChannel;
import javax.net.ssl.SSLEngineResult.Status;

public class SSLHandler {
    protected static final Logger LOGGER = LoggerFactory.getLogger(SSLHandler.class);
    private FrontendConnection con;
    private NetworkChannel channel;

    private volatile ByteBuffer decryptOut;

    private volatile SSLEngine engine;
    private volatile boolean isHandshakeSuccess = false;

    public SSLHandler(FrontendConnection con) {
        this.con = con;
        this.channel = con.getChannel();
    }

    public void init() throws IOException {
        this.engine = SSLEngineFactory.appleSSLEngine(true);
        if (this.channel instanceof SocketChannel) {
            ((SocketChannel) this.channel).configureBlocking(false);
        }
        this.decryptOut = con.allocate();
    }

    public void handShark(byte[] data) throws SSLException {
        unwrapNonAppData(data);
    }

    /**
     * receive and process the SSL handshake protocol initiated by the client
     */
    private void unwrapNonAppData(byte[] data) throws SSLException {
        ByteBuffer in = con.allocate(data.length);
        in.put(data);
        in.flip();

        try {
            for (; ; ) {
                final SSLEngineResult result = unwrap(engine, in);
                final Status status = result.getStatus();
                final SSLEngineResult.HandshakeStatus handshakeStatus = result.getHandshakeStatus();
                final int produced = result.bytesProduced();
                final int consumed = result.bytesConsumed();
                if (status == Status.CLOSED) {
                    return;
                }

                switch (handshakeStatus) {
                    case NEED_WRAP:
                        wrapNonAppData();
                        break;
                    case NEED_TASK:
                        runDelegatedTasks();
                        break;
                    case FINISHED:
                        /*setHandshakeSuccess();
                        continue;*/
                    case NEED_UNWRAP:
                    case NOT_HANDSHAKING:
                        break;
                    default:
                        throw new IllegalStateException("unknown handshake status: " + handshakeStatus);
                }
                if (status == Status.BUFFER_UNDERFLOW || consumed == 0 && produced == 0) {
                    break;
                }
            }
        } catch (SSLException e) {
            LOGGER.error("during the handshark, unwrap data exception: {}", e);
            con.close("during the handshark, unwrap data fail");
        } finally {
            con.recycle(in);
        }
    }

    /**
     * unwrap ssl application data sent by client
     */
    public void unwrapAppData(byte[] appData) throws SSLException {
        ByteBuffer in = ByteBuffer.allocate(appData.length);
        in.put(appData);
        in.flip();
        try {
            for (; ; ) {
                SSLEngineResult result = unwrap(engine, in);

                Status status = result.getStatus();
                int produced = result.bytesProduced();
                int consumed = result.bytesConsumed();

                if (status == Status.CLOSED) {
                    if (!con.isClosed())
                        con.close("SSL closed");
                    return;
                }

                if (consumed == 0 && produced == 0) {
                    return;
                }
            }
        } catch (SSLException e) {
            LOGGER.error("during the interaction, unwrap data exception: {}", e);
            con.close("during the interaction, unwrap data fail");
            throw e;
        } finally {
            con.recycle(in);
        }
    }

    private SSLEngineResult unwrap(SSLEngine engine0, ByteBuffer in) throws SSLException {
        int overflows = 0;
        ByteBuffer outBuffer = con.findReadBuffer();
        for (; ; ) {
            SSLEngineResult result = engine0.unwrap(in, outBuffer);
            switch (result.getStatus()) {
                case BUFFER_OVERFLOW:
                    int max = engine0.getSession().getApplicationBufferSize();
                    switch (overflows++) {
                        case 0:
                            outBuffer = con.ensureReadBufferFree(outBuffer, outBuffer.capacity() * 2);
                            break;
                        default:
                            outBuffer = con.ensureReadBufferFree(outBuffer, max);
                    }
                    break;
                default:
                    return result;
            }
        }
    }

    /**
     * dble responds as a server to the ssl handshake protocol
     */
    private void wrapNonAppData() throws SSLException {
        try {
            for (; ; ) {
                decryptOut.clear();
                SSLEngineResult result = wrap(engine, ByteBufferUtil.EMPTY_BYTE_BUFFER);
                if (result.bytesProduced() > 0) {
                    decryptOut.flip();
                    ByteBuffer outBuffer = con.allocate(result.bytesProduced());
                    outBuffer.put(decryptOut);
                    con.getService().writeDirectly(outBuffer, WriteFlags.PART);
                }
                switch (result.getHandshakeStatus()) {
                    case FINISHED:
                        setHandshakeSuccess();
                        break;
                    case NEED_TASK:
                    case NEED_UNWRAP:
                    case NEED_WRAP:
                    case NOT_HANDSHAKING:
                        break;
                    default:
                        throw new IllegalStateException("Unknown handshake status: " + result.getHandshakeStatus());
                }
                if (result.bytesProduced() == 0) {
                    break;
                }
            }
        } catch (SSLException e) {
            LOGGER.error("during the handshark, wrap data exception: {}", e);
            con.close("during the handshark, wrap data fail");
            throw e;
        }
    }

    /**
     * apply ssl wrap to application data
     */
    public ByteBuffer wrapAppData(ByteBuffer appBuffer) throws SSLException {
        if (!isHandshakeSuccess()) {
            return appBuffer;
        }
        try {
            appBuffer.flip();
            decryptOut.clear();

            SSLEngineResult result = wrap(engine, appBuffer);
            if (result.getStatus() == Status.CLOSED) {
                if (!con.isClosed())
                    con.close("SSL closed");
                return ByteBufferUtil.EMPTY_BYTE_BUFFER;
            }

            if (result.bytesProduced() > 0) {
                decryptOut.flip();
                ByteBuffer outBuffer = con.allocate(result.bytesProduced());
                outBuffer.put(decryptOut);
                return outBuffer;
            }

            return ByteBufferUtil.EMPTY_BYTE_BUFFER;
        } catch (SSLException e) {
            LOGGER.error("during the interaction, wrap data exception: {}", e);
            con.close("during the interaction, wrap data fail");
            throw e;
        }
    }

    private SSLEngineResult wrap(SSLEngine engine0, ByteBuffer in) throws SSLException {
        for (; ; ) {
            SSLEngineResult result = engine0.wrap(in, decryptOut);
            switch (result.getStatus()) {
                case BUFFER_OVERFLOW:
                    decryptOut = ensure(decryptOut, engine0.getSession().getPacketBufferSize());
                    break;
                default:
                    return result;
            }
        }
    }

    private void runDelegatedTasks() {
        for (; ; ) {
            Runnable task = engine.getDelegatedTask();
            if (task == null) {
                break;
            }
            task.run();
        }
    }

    private void setHandshakeSuccess() {
        isHandshakeSuccess = true;
        if (LOGGER.isDebugEnabled()) {
            LOGGER.info("{} SSL handshake complete", con);
        }
    }

    public boolean isHandshakeSuccess() {
        return isHandshakeSuccess;
    }

    public void close() {
        try {
            if (engine != null) {
                if (engine.isInboundDone())
                    engine.closeInbound();
                if (engine.isOutboundDone())
                    engine.closeOutbound();
            }
            if (decryptOut != null)
                con.recycle(decryptOut);

        } catch (SSLException e) {
            LOGGER.warn("SSL close failed, exception：{}", e);
        }
    }

    private ByteBuffer ensure(ByteBuffer recycleBuffer, int size) {
        ByteBuffer newBuffer = con.allocate(size);
        con.recycle(recycleBuffer);
        return newBuffer;
    }

    public static boolean isSSLPackage(byte i) {
        int packageType = i & 0xff;
        switch (packageType) {
            case 20:  // change_cipher_spec
            case 21:  // alert
            case 22:  // handshake
            case 23:  // application_data
                return true;
            default:
                // SSLv2 or bad data
                return false;
        }
    }
}
