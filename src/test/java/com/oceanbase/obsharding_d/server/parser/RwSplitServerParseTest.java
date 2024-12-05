package com.oceanbase.obsharding_d.server.parser;

import org.junit.Assert;
import org.junit.Test;


public class RwSplitServerParseTest {


    ServerParse serverParse = ServerParseFactory.getRwSplitParser();

    @Test
    public void testIsGrant() {
        Assert.assertEquals(RwSplitServerParse.GRANT, 0xff & serverParse.parse(" grant  ..."));
        Assert.assertEquals(RwSplitServerParse.GRANT, 0xff & serverParse.parse("GRANT ..."));
        Assert.assertEquals(RwSplitServerParse.GRANT, 0xff & serverParse.parse(" Grant   a"));
    }

    @Test
    public void testIsRevoke() {
        Assert.assertEquals(RwSplitServerParse.REVOKE, 0xff & serverParse.parse(" revoke  ..."));
        Assert.assertEquals(RwSplitServerParse.REVOKE, 0xff & serverParse.parse("REVOKE ..."));
        Assert.assertEquals(RwSplitServerParse.REVOKE, 0xff & serverParse.parse(" Revoke   a"));
    }

    @Test
    public void testIsInstall() {
        Assert.assertEquals(RwSplitServerParse.INSTALL, 0xff & serverParse.parse(" install  ..."));
        Assert.assertEquals(RwSplitServerParse.INSTALL, 0xff & serverParse.parse("INSTALL ..."));
        Assert.assertEquals(RwSplitServerParse.INSTALL, 0xff & serverParse.parse(" Install   a"));
    }

    @Test
    public void testIsRename() {
        Assert.assertEquals(RwSplitServerParse.RENAME, 0xff & serverParse.parse(" rename  ..."));
        Assert.assertEquals(RwSplitServerParse.RENAME, 0xff & serverParse.parse("RENAME ..."));
        Assert.assertEquals(RwSplitServerParse.RENAME, 0xff & serverParse.parse(" Rename   a"));
    }

    @Test
    public void testIsUnInstall() {
        Assert.assertEquals(RwSplitServerParse.UNINSTALL, 0xff & serverParse.parse(" uninstall  ..."));
        Assert.assertEquals(RwSplitServerParse.UNINSTALL, 0xff & serverParse.parse("UNINSTALL ..."));
        Assert.assertEquals(RwSplitServerParse.UNINSTALL, 0xff & serverParse.parse(" Uninstall   a"));
    }

    @Test
    public void testIsStartTransaction() {
        Assert.assertEquals(RwSplitServerParse.START_TRANSACTION, 0xff & serverParse.parse("  START TRANSACTION"));
        Assert.assertEquals(RwSplitServerParse.START_TRANSACTION, 0xff & serverParse.parse(" start transaction"));
        Assert.assertEquals(RwSplitServerParse.START_TRANSACTION, 0xff & serverParse.parse("  Start Transaction"));
    }

    @Test
    public void testIsStartSlave() {
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & serverParse.parse("  START SLAVE  ..."));
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & serverParse.parse(" start slave ..."));
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & serverParse.parse("  Start Slave   a"));
    }

    @Test
    public void testIsAnalyze() {
        Assert.assertEquals(RwSplitServerParse.UNSUPPORT & 0xff, 0xff & serverParse.parse("   ANALYZE TABLE test"));
        Assert.assertEquals(RwSplitServerParse.UNSUPPORT & 0xff, 0xff & serverParse.parse(" analyze table test"));
        Assert.assertEquals(RwSplitServerParse.UNSUPPORT & 0xff, 0xff & serverParse.parse("  Analyze Table test"));
    }

    @Test
    public void testIsCache() {
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & serverParse.parse("    CACHE INDEX test"));
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & serverParse.parse(" cache index test"));
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & serverParse.parse(" Cache Index test"));
    }

    @Test
    public void testIsCheck() {
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & serverParse.parse(" CHECK TABLE test"));
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & serverParse.parse(" check table test"));
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & serverParse.parse(" Check Table test"));
    }

    @Test
    public void testIsFlush() {
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & serverParse.parse(" flush"));
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & serverParse.parse(" FLUSH"));
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & serverParse.parse(" Flush"));
    }

    @Test
    public void testIsLoadIndex() {
        Assert.assertEquals(RwSplitServerParse.UNSUPPORT & 0xff, 0xff & serverParse.parse("  LOAD INDEX INTO CACHE"));
        Assert.assertEquals(RwSplitServerParse.UNSUPPORT & 0xff, 0xff & serverParse.parse(" load index into cache"));
        Assert.assertEquals(RwSplitServerParse.UNSUPPORT & 0xff, 0xff & serverParse.parse(" Load Index Into Cache"));
    }

    @Test
    public void testIsOptimize() {
        Assert.assertEquals(RwSplitServerParse.UNSUPPORT & 0xff, 0xff & serverParse.parse("   OPTIMIZE TABLE test"));
        Assert.assertEquals(RwSplitServerParse.UNSUPPORT & 0xff, 0xff & serverParse.parse(" optimize table test"));
        Assert.assertEquals(RwSplitServerParse.UNSUPPORT & 0xff, 0xff & serverParse.parse(" Optimize Table Test"));
    }

    @Test
    public void testIsRepair() {
        Assert.assertEquals(RwSplitServerParse.UNSUPPORT & 0xff, 0xff & serverParse.parse("  REPAIR TABLE test "));
        Assert.assertEquals(RwSplitServerParse.UNSUPPORT & 0xff, 0xff & serverParse.parse(" repair table test "));
        Assert.assertEquals(RwSplitServerParse.UNSUPPORT & 0xff, 0xff & serverParse.parse(" Repair Table test "));
    }

    @Test
    public void testIsReset() {
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & serverParse.parse("   RESET test"));
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & serverParse.parse(" reset test"));
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & serverParse.parse(" Reset test"));
    }

    @Test
    public void testIsStop() {
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & serverParse.parse("   STOP SLAVE"));
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & serverParse.parse(" stop slave"));
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & serverParse.parse(" Stop Slave"));
    }

    @Test
    public void testIsResetSlave() {
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & serverParse.parse("   RESET SLAVE"));
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & serverParse.parse(" reset slave"));
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & serverParse.parse(" Reset Slave"));
    }

    @Test
    public void testIsChange() {
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & serverParse.parse("   CHANGE MASTER TO"));
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & serverParse.parse(" change master to"));
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & serverParse.parse(" Change Master to"));
    }
}
