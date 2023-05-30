package com.actiontech.dble.net;

import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.util.CompareUtil;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketException;
import java.net.SocketOption;
import java.nio.channels.NetworkChannel;
import java.nio.channels.SocketChannel;
import java.util.Objects;
import java.util.Set;

public final class DbleSocketOptions {
    private static final Logger LOGGER = LoggerFactory.getLogger("DbleSocketOptions");

    private static final boolean KEEP_ALIVE_OPT_SUPPORTED;

    // https://bugs.openjdk.org/browse/JDK-8194298
    public static final String ORACLE_VERSION = "1.8.0_261";
    public static final String OPEN_VERSION = "1.8.0_272";
    public static final String TCP_KEEP_IDLE = "TCP_KEEPIDLE";
    public static final String TCP_KEEP_INTERVAL = "TCP_KEEPINTERVAL";
    public static final String TCP_KEEP_COUNT = "TCP_KEEPCOUNT";
    private static NetworkChannel networkChannel = null;


    private DbleSocketOptions() {
    }

    static {
        KEEP_ALIVE_OPT_SUPPORTED = keepAliveOptSupported();
    }


    /**
     * did not added the note for upper-bound because values are
     * also OS specific.
     * <p>
     * since https://mail.openjdk.org/pipermail/net-dev/2018-May/011430.html
     *
     * @param socketName
     * @param value
     * @throws IOException
     */
    public static boolean check(String socketName, int value) throws IOException {
        if (KEEP_ALIVE_OPT_SUPPORTED) {
            if (Objects.isNull(networkChannel)) {
                networkChannel = SocketChannel.open();
            }
            try {
                Set<SocketOption<?>> socketOptions = networkChannel.supportedOptions();
                SocketOption<Integer> socket;
                socket = (SocketOption<Integer>) socketOptions.stream().filter(socketOption -> StringUtil.equals(socketName, socketOption.name())).findFirst().get();
                networkChannel.setOption(socket, value);
                return true;
            } catch (SocketException e) {
                LOGGER.warn(e.toString(), e);
                return false;
            }
        }
        return true;
    }


    /**
     * In general, connection establishment should be controlled by dble default tcp parameters
     *
     * @param channel
     * @throws IOException
     * @since https://bugs.openjdk.org/browse/JDK-8194298
     */
    public static void setKeepAliveOptions(NetworkChannel channel) throws IOException {
        if (KEEP_ALIVE_OPT_SUPPORTED) {
            SystemConfig instance = SystemConfig.getInstance();
            int tcpKeepIdle = instance.getTcpKeepIdle();
            int tcpKeepInterval = instance.getTcpKeepInterval();
            int tcpKeepCount = instance.getTcpKeepCount();
            Set<SocketOption<?>> socketOptions = channel.supportedOptions();
            //Compile compatibility
            SocketOption<Integer> socket;

            for (SocketOption<?> socketOption : socketOptions) {
                switch (socketOption.name()) {
                    case TCP_KEEP_IDLE:
                        socket = (SocketOption<Integer>) socketOption;
                        channel.setOption(socket, tcpKeepIdle);
                        break;
                    case TCP_KEEP_INTERVAL:
                        socket = (SocketOption<Integer>) socketOption;
                        channel.setOption(socket, tcpKeepInterval);
                        break;
                    case TCP_KEEP_COUNT:
                        socket = (SocketOption<Integer>) socketOption;
                        channel.setOption(socket, tcpKeepCount);
                        break;
                    default:
                        break;
                }
            }
        }
    }

    private static boolean keepAliveOptSupported() {
        String property = System.getProperty("java.vendor");
        String version = System.getProperty("java.version");
        if (osName().contains("Windows")) {
            return false;
        }
        if (property.toLowerCase().contains("oracle")) {
            return CompareUtil.versionCompare(ORACLE_VERSION, version, "\\.") <= 0;
        } else {
            return CompareUtil.versionCompare(OPEN_VERSION, version, "\\.") <= 0;
        }
    }

    public static void clean() {
        if (Objects.nonNull(networkChannel)) {
            try {
                networkChannel.close();
            } catch (IOException e) {
                LOGGER.warn("close channel error {}", e);
            }
        }
    }

    public static String osName() {
        return System.getProperty("os.name");
    }

    public static boolean isKeepAliveOPTSupported() {
        return KEEP_ALIVE_OPT_SUPPORTED;
    }

}

