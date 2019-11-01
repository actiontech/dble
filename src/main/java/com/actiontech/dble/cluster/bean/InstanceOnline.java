package com.actiontech.dble.cluster.bean;

import com.actiontech.dble.DbleServer;
import com.alibaba.fastjson.JSONObject;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

/**
 * Created by szf on 2019/8/28.
 */
public final class InstanceOnline {

    private static final InstanceOnline INSTANCE = new InstanceOnline();
    private static final String SERVER_PORT = "SERVER_PORT";
    private static final String HOST_ADDR = "HOST_ADDR";
    private static final String START_TIME = "START_TIME";
    private final int serverPort;
    private String hostAddr;
    private final long startTime;

    private InstanceOnline() {
        serverPort = DbleServer.getInstance().getConfig().getSystem().getServerPort();
        startTime = System.currentTimeMillis();
        hostAddr = getHostIp();
    }

    public static InstanceOnline getInstance() {
        return INSTANCE;
    }

    public boolean canRemovePath(String value) {
        if (hostAddr == null) {
            return false;
        }
        try {
            JSONObject jsonObj = JSONObject.parseObject(value);
            return serverPort == Long.parseLong(jsonObj.getString(SERVER_PORT)) &&
                    hostAddr.equals(jsonObj.getString(HOST_ADDR));
        } catch (Exception e) {
            //remove the old online timestamp when upgrade from old version
            return true;
        }
    }

    public String toString() {
        JSONObject online = new JSONObject();
        online.put(SERVER_PORT, serverPort);
        online.put(HOST_ADDR, hostAddr);
        online.put(START_TIME, startTime);
        return online.toJSONString();
    }

    private static String getHostIp() {
        try {
            Enumeration<NetworkInterface> allNetInterfaces = NetworkInterface.getNetworkInterfaces();
            while (allNetInterfaces.hasMoreElements()) {
                NetworkInterface netInterface = (NetworkInterface) allNetInterfaces.nextElement();
                Enumeration<InetAddress> addresses = netInterface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress ip = (InetAddress) addresses.nextElement();
                    if (ip != null &&
                            ip instanceof Inet4Address &&
                            !ip.isLoopbackAddress() &&
                            ip.getHostAddress().indexOf(":") == -1) {
                        return ip.getHostAddress();
                    }
                }
            }
        } catch (Exception e) {
            return null;
        }
        return null;
    }
}
