package com.actiontech.dble.util;

import org.junit.Assert;
import org.junit.Test;

import java.net.UnknownHostException;


public class IPAddressUtilTest {

    @Test
    public void check() {
        //ipv6-normal
        Assert.assertEquals(true, IPAddressUtil.check("1:2:3:4:5:6:7:8"));
        Assert.assertEquals(true, IPAddressUtil.check("1::"));
        Assert.assertEquals(true, IPAddressUtil.check("1:2:3:4:5:6:7::"));
        Assert.assertEquals(true, IPAddressUtil.check("1::8"));
        Assert.assertEquals(true, IPAddressUtil.check("1:2:3:4:5:6::8"));
        Assert.assertEquals(true, IPAddressUtil.check("1::7:8"));
        Assert.assertEquals(true, IPAddressUtil.check("1:2:3:4:5::7:8"));
        Assert.assertEquals(true, IPAddressUtil.check("1:2:3:4:5::8"));
        Assert.assertEquals(true, IPAddressUtil.check("1::6:7:8"));
        Assert.assertEquals(true, IPAddressUtil.check("1:2:3:4::6:7:8"));
        Assert.assertEquals(true, IPAddressUtil.check("1:2:3:4::8"));
        Assert.assertEquals(true, IPAddressUtil.check("1::5:6:7:8"));
        Assert.assertEquals(true, IPAddressUtil.check("1:2:3::8"));
        Assert.assertEquals(true, IPAddressUtil.check("1:2:3::5:6:7:8"));
        Assert.assertEquals(true, IPAddressUtil.check("1::4:5:6:7:8"));
        Assert.assertEquals(true, IPAddressUtil.check("1:2::4:5:6:7:8"));
        Assert.assertEquals(true, IPAddressUtil.check("1:2::8"));
        Assert.assertEquals(true, IPAddressUtil.check("1::3:4:5:6:7:8"));
        Assert.assertEquals(true, IPAddressUtil.check("::2:3:4:5:6:7:8"));
        Assert.assertEquals(true, IPAddressUtil.check("::8"));
        Assert.assertEquals(true, IPAddressUtil.check("::"));
        Assert.assertEquals(true, IPAddressUtil.check("fe80::7:8%eth0"));
        Assert.assertEquals(true, IPAddressUtil.check("fe80::7:8%1"));
        Assert.assertEquals(true, IPAddressUtil.check("fe80::f492:e475:4894:fe06%20"));
        //ipv6-unusual
        Assert.assertEquals(false, IPAddressUtil.check("1:2:3:4:5:6:7:gggg"));
        Assert.assertEquals(false, IPAddressUtil.check("1:2:3:4:5:6:7:-1"));
        Assert.assertEquals(false, IPAddressUtil.check("1:2:3:4:5:6:7:8:9"));
        Assert.assertEquals(false, IPAddressUtil.check("1:2:3:4:5:6:7"));

        //ipv6-ipv4
        Assert.assertEquals(false, IPAddressUtil.check("::255.255.255.255"));
        Assert.assertEquals(false, IPAddressUtil.check("::ffff:255.255.255.255"));
        Assert.assertEquals(false, IPAddressUtil.check("2001:db8:3:4::192.0.2.33"));
        Assert.assertEquals(false, IPAddressUtil.check("2001:db8:3:4:5:6:192.0.2.33"));
        Assert.assertEquals(false, IPAddressUtil.check("64:ff9b::192.0.2.33"));

        //ipv4-normal
        Assert.assertEquals(true, IPAddressUtil.check("192.168.1.1"));
        Assert.assertEquals(true, IPAddressUtil.check("0.0.0.0"));
        Assert.assertEquals(true, IPAddressUtil.check("255.255.255.255"));
        //ipv4-unusual
        Assert.assertEquals(false, IPAddressUtil.check("-1.0.0.0"));
        Assert.assertEquals(false, IPAddressUtil.check("256.0.0.0"));
        Assert.assertEquals(false, IPAddressUtil.check("0.0.0.256"));
        Assert.assertEquals(false, IPAddressUtil.check("0.0.0.-1"));
        Assert.assertEquals(false, IPAddressUtil.check("255.255.255"));
        Assert.assertEquals(false, IPAddressUtil.check("255.255.255.255.255"));
    }

    @Test
    public void match() throws UnknownHostException {
        String host4 = "192.168.2.1";
        String host6 = "1:0:3:4:5:6:7:8";
        Assert.assertEquals(IPAddressUtil.match(host4, "192.168.2.1"), true);
        Assert.assertEquals(IPAddressUtil.match(host4, "192.168.2.22"), false);
        Assert.assertEquals(IPAddressUtil.match(host4, "192.168.2.1-192.168.2.100"), true);
        Assert.assertEquals(IPAddressUtil.match(host4, "192.168.1.10-192.168.1.100"), false);
        Assert.assertEquals(IPAddressUtil.match(host4, "192.168.2.%"), true);
        Assert.assertEquals(IPAddressUtil.match(host4, "192.168.1.%"), false);
        Assert.assertEquals(IPAddressUtil.match(host4, "192.168.1.1/22"), true);
        Assert.assertEquals(IPAddressUtil.match(host4, "192.168.1.1/23"), false);
        Assert.assertEquals(IPAddressUtil.match(host4, "1::3:4:5:6:7:9"), false);
        Assert.assertEquals(IPAddressUtil.match(host4, "1::3:4:5:6:7:8-1::3:4:5:6:7:ffff"), false);
        Assert.assertEquals(IPAddressUtil.match(host4, "1::3:%:5:6:7:%"), false);
        Assert.assertEquals(IPAddressUtil.match(host4, "1:3:4:5:6:7:127.0.0.1"), false);

        Assert.assertEquals(IPAddressUtil.match(host6, "1::3:4:5:6:7:8"), true);
        Assert.assertEquals(IPAddressUtil.match(host6, "1::3:4:5:6:7:9"), false);
        Assert.assertEquals(IPAddressUtil.match(host6, "1::3:4:5:6:7:8-1::3:4:5:6:7:ffff"), true);
        Assert.assertEquals(IPAddressUtil.match(host6, "1::3:4:5:6:7:9-1::3:4:5:6:7:ffff"), false);
        Assert.assertEquals(IPAddressUtil.match(host6, "1::3:4:5:6:7:9-1::3:4:5:6:7:ffff"), false);
        Assert.assertEquals(IPAddressUtil.match(host6, "1::3:4:%:5:6:7"), true);
        Assert.assertEquals(IPAddressUtil.match(host6, "1::2:%:5:6:7:%"), false);
        Assert.assertEquals(IPAddressUtil.match(host6, "1::3:9:5:6:7:8/60"), true);
        Assert.assertEquals(IPAddressUtil.match(host6, "1::3:9:5:6:7:8/61"), false);
        Assert.assertEquals(IPAddressUtil.match(host6, "192.168.2.22"), false);
        Assert.assertEquals(IPAddressUtil.match(host6, "192.168.1.10-192.168.1.100"), false);
        Assert.assertEquals(IPAddressUtil.match(host6, "192.168.1.%"), false);
    }
}