/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.util;


import com.actiontech.dble.config.util.ConfigException;
import com.google.gson.Gson;
import io.netty.handler.ipfilter.IpFilterRuleType;
import io.netty.handler.ipfilter.IpSubnetFilterRule;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class IPAddressUtil {

    private static final String SEPARATOR_1 = "-";

    private static final String SEPARATOR_2 = "%";

    private static final String SEPARATOR_3 = "/";

    private static final String IPV6_LOCAL_PREFIX = "fe80";

    private static final String PATTERN_IP = "(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))";

    private IPAddressUtil() {
    }

    /**
     * Given an IPv6 or IPv4 address, convert it into a BigInteger.
     *
     * @return the integer representation of the InetAddress
     */
    private static BigInteger ipAddressToBigInteger(InetAddress addr) {
        byte[] ip = addr.getAddress();
        if (ip[0] == -1) {
            return new BigInteger(1, ip);
        }
        return new BigInteger(ip);
    }

    /**
     * check ip
     *
     * @param target ip to be check
     */
    public static boolean check(String target) {
        if (target.contains(SEPARATOR_1)) {
            //rule ip1-ip2
            String[] sourceSplit = target.split(SEPARATOR_1);
            if (sourceSplit.length == 2) {
                return Pattern.matches(PATTERN_IP, sourceSplit[0]) && Pattern.matches(PATTERN_IP, sourceSplit[1]);
            }
        } else if (target.contains(SEPARATOR_2) && !target.contains(IPV6_LOCAL_PREFIX)) {
            //rule 172.0.0.%
            String start = target.replace(SEPARATOR_2, "0");
            return Pattern.matches(PATTERN_IP, start);
        } else if (target.contains(SEPARATOR_3)) {
            //rule CIDR
            String[] sourceSplit = target.split(SEPARATOR_3);
            if (sourceSplit.length == 2 && isIPCidr(sourceSplit[0], Integer.parseInt(sourceSplit[1]))) {
                return Pattern.matches(PATTERN_IP, sourceSplit[0]);
            }
        } else {
            //rule ip
            return Pattern.matches(PATTERN_IP, target);
        }
        return false;
    }

    /**
     * ip/cidr
     *
     * @param ipAddress
     * @param cidrPrefix
     * @return
     */
    private static boolean isIPCidr(String ipAddress, int cidrPrefix) {
        if (StringUtil.isEmpty(ipAddress)) {
            return false;
        }
        if (sun.net.util.IPAddressUtil.isIPv4LiteralAddress(ipAddress) && cidrPrefix >= 0 && cidrPrefix <= 32) {
            return true;
        } else if (sun.net.util.IPAddressUtil.isIPv6LiteralAddress(ipAddress) && cidrPrefix >= 0 && cidrPrefix <= 128) {
            return true;
        }
        return false;
    }

    /**
     * Whether the matching ip is in the range
     *
     * @param target ip to be matched
     * @param source ip range
     * @return true:match  false:mismatch
     * @throws UnknownHostException
     */
    public static boolean match(String target, String source) throws UnknownHostException {
        if (StringUtil.isEmpty(target) || StringUtil.isEmpty(source)) {
            return false;
        }
        if (source.contains(SEPARATOR_1)) {
            //rule ip1-ip2
            String[] sourceSplit = source.split(SEPARATOR_1);
            if (sourceSplit.length == 2) {
                return between(target, sourceSplit[0], sourceSplit[1]);
            }
        } else if (source.contains(SEPARATOR_2) && !source.contains(IPV6_LOCAL_PREFIX)) {
            //rule 172.0.0.%
            String start = source.replace(SEPARATOR_2, "0");
            String end = null;
            if (sun.net.util.IPAddressUtil.isIPv4LiteralAddress(start)) {
                end = source.replace(SEPARATOR_2, "255");
            } else if (sun.net.util.IPAddressUtil.isIPv6LiteralAddress(start)) {
                end = source.replace(SEPARATOR_2, "ffff");
            }
            return between(target, start, end);
        } else if (source.contains(SEPARATOR_3)) {
            //rule CIDR
            String[] sourceSplit = source.split(SEPARATOR_3);
            if (sourceSplit.length == 2) {
                IpSubnetFilterRule rule = new IpSubnetFilterRule(sourceSplit[0], Integer.parseInt(sourceSplit[1]), IpFilterRuleType.ACCEPT);
                return rule.matches(newSockAddress(target));
            }
        } else {
            //rule ip
            return between(target, source, source);
        }
        return false;
    }

    private static InetSocketAddress newSockAddress(String ipAddress) {
        return new InetSocketAddress(ipAddress, 1234);
    }

    private static boolean between(String targetStr, String startStr, String endStr) throws UnknownHostException {
        if (StringUtil.isEmpty(targetStr) || StringUtil.isEmpty(startStr) || StringUtil.isEmpty(endStr)) {
            return false;
        }
        InetAddress start = InetAddress.getByName(startStr);
        InetAddress end = InetAddress.getByName(endStr);
        InetAddress target = InetAddress.getByName(targetStr);
        return ipAddressToBigInteger(start).compareTo(ipAddressToBigInteger(target)) <= 0 &&
                ipAddressToBigInteger(target).compareTo(ipAddressToBigInteger(end)) <= 0;
    }

    public static void checkWhiteIPs(String strWhiteIPs) {
        if (!StringUtil.isEmpty(strWhiteIPs)) {
            String[] theWhiteIPs = SplitUtil.split(strWhiteIPs, ',');
            Set<String> incorrectIPs = Arrays.stream(theWhiteIPs).filter(e -> !IPAddressUtil.check(e)).collect(Collectors.toSet());
            if (incorrectIPs.size() > 0) {
                throw new ConfigException("The configuration contains incorrect IP" + new Gson().toJson(incorrectIPs));
            }
        }
    }
}
