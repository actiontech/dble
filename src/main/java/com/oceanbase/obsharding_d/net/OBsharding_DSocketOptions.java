package com.oceanbase.obsharding_d.net;

import com.oceanbase.obsharding_d.config.ProblemReporter;
import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.config.util.StartProblemReporter;
import com.oceanbase.obsharding_d.util.CompareUtil;
import com.oceanbase.obsharding_d.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketException;
import java.net.SocketOption;
import java.nio.channels.NetworkChannel;
import java.nio.channels.SocketChannel;
import java.util.Objects;
import java.util.Set;

public final class OBsharding_DSocketOptions {
    private static final Logger LOGGER = LoggerFactory.getLogger("OBsharding_DSocketOptions");

    private final ProblemReporter problemReporter = StartProblemReporter.getInstance();
    private static final String WARNING_FORMAT = "Property [ %s ] '%s' in bootstrap.cnf is illegal, you may need use the default value %s replaced";
    private static final OBsharding_DSocketOptions INSTANCE = new OBsharding_DSocketOptions();


    private static final boolean KEEP_ALIVE_OPT_SUPPORTED;

    // https://bugs.openjdk.org/browse/JDK-8194298
    public static final String ORACLE_VERSION = "1.8.0_261";
    public static final String OPEN_VERSION = "1.8.0_272";
    public static final String TCP_KEEP_IDLE = "TCP_KEEPIDLE";
    public static final String TCP_KEEP_INTERVAL = "TCP_KEEPINTERVAL";
    public static final String TCP_KEEP_COUNT = "TCP_KEEPCOUNT";
    private static NetworkChannel networkChannel = null;

    private int tcpKeepIdle = 30;
    private int tcpKeepInterval = 10;
    private int tcpKeepCount = 3;


    private OBsharding_DSocketOptions() {
    }

    static {
        KEEP_ALIVE_OPT_SUPPORTED = keepAliveOptSupported();
    }

    public void check() throws IOException {
        SystemConfig instance = SystemConfig.getInstance();
        if (!checkHelp(OBsharding_DSocketOptions.TCP_KEEP_INTERVAL, instance.getTcpKeepInterval())) {
            problemReporter.warn(String.format(WARNING_FORMAT, "tcpKeepInterval", instance.getTcpKeepInterval(), getTcpKeepInterval()));
        }
        if (!checkHelp(OBsharding_DSocketOptions.TCP_KEEP_IDLE, instance.getTcpKeepIdle())) {
            problemReporter.warn(String.format(WARNING_FORMAT, "tcpKeepIdle", instance.getTcpKeepIdle(), getTcpKeepIdle()));
        }
        if (!checkHelp(OBsharding_DSocketOptions.TCP_KEEP_COUNT, instance.getTcpKeepCount())) {
            problemReporter.warn(String.format(WARNING_FORMAT, "tcpKeepCount", instance.getTcpKeepCount(), getTcpKeepCount()));
        }
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
    private boolean checkHelp(String socketName, int value) throws IOException {
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
     * In general, connection establishment should be controlled by OBsharding-D default tcp parameters
     *
     * @param channel
     * @throws IOException
     * @since https://bugs.openjdk.org/browse/JDK-8194298
     */
    public void setKeepAliveOptions(NetworkChannel channel) throws IOException {
        if (KEEP_ALIVE_OPT_SUPPORTED) {
            SystemConfig instance = SystemConfig.getInstance();
            int curTcpKeepIdle = instance.getTcpKeepIdle();
            int curTcpKeepInterval = instance.getTcpKeepInterval();
            int curTcpKeepCount = instance.getTcpKeepCount();
            Set<SocketOption<?>> socketOptions = channel.supportedOptions();
            //Compile compatibility
            SocketOption<Integer> socket;

            for (SocketOption<?> socketOption : socketOptions) {
                switch (socketOption.name()) {
                    case TCP_KEEP_IDLE:
                        socket = (SocketOption<Integer>) socketOption;
                        channel.setOption(socket, curTcpKeepIdle);
                        break;
                    case TCP_KEEP_INTERVAL:
                        socket = (SocketOption<Integer>) socketOption;
                        channel.setOption(socket, curTcpKeepInterval);
                        break;
                    case TCP_KEEP_COUNT:
                        socket = (SocketOption<Integer>) socketOption;
                        channel.setOption(socket, curTcpKeepCount);
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

    public void clean() {
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

    public int getTcpKeepIdle() {
        return tcpKeepIdle;
    }

    public int getTcpKeepInterval() {
        return tcpKeepInterval;
    }

    public int getTcpKeepCount() {
        return tcpKeepCount;
    }

    public static OBsharding_DSocketOptions getInstance() {
        return INSTANCE;
    }
}

