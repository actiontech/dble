package com.actiontech.dble.net;

import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.util.CompareUtil;

import java.io.IOException;
import java.net.SocketOption;
import java.nio.channels.NetworkChannel;
import java.util.Set;

public final class DbleSocketOptions {
    private static final boolean KEEP_ALIVE_OPT_SUPPORTED;

    // https://bugs.openjdk.org/browse/JDK-8194298
    public static final String ORACLE_VERSION = "1.8.0_261";
    public static final String OPEN_VERSION = "1.8.0_272";


    private DbleSocketOptions() {
    }

    static {
        KEEP_ALIVE_OPT_SUPPORTED = keepAliveOptSupported();
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
                    case "TCP_KEEPIDLE":
                        socket = (SocketOption<Integer>) socketOption;
                        channel.setOption(socket, tcpKeepIdle);
                        break;
                    case "TCP_KEEPINTERVAL":
                        socket = (SocketOption<Integer>) socketOption;
                        channel.setOption(socket, tcpKeepInterval);
                        break;
                    case "TCP_KEEPCOUNT":
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

    public static String osName() {
        return System.getProperty("os.name");
    }

    public static boolean isKeepAliveOPTSupported() {
        return KEEP_ALIVE_OPT_SUPPORTED;
    }

}

