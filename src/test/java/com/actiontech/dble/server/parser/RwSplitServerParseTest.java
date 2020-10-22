package com.actiontech.dble.server.parser;

import org.junit.Assert;
import org.junit.Test;


public class RwSplitServerParseTest {


    @Test
    public void testIsGrant() {
        Assert.assertEquals(RwSplitServerParse.GRANT, 0xff & RwSplitServerParse.parse(" grant  ..."));
        Assert.assertEquals(RwSplitServerParse.GRANT, 0xff & RwSplitServerParse.parse("GRANT ..."));
        Assert.assertEquals(RwSplitServerParse.GRANT, 0xff & RwSplitServerParse.parse(" Grant   a"));
    }

    @Test
    public void testIsRevoke() {
        Assert.assertEquals(RwSplitServerParse.REVOKE, 0xff & RwSplitServerParse.parse(" revoke  ..."));
        Assert.assertEquals(RwSplitServerParse.REVOKE, 0xff & RwSplitServerParse.parse("REVOKE ..."));
        Assert.assertEquals(RwSplitServerParse.REVOKE, 0xff & RwSplitServerParse.parse(" Revoke   a"));
    }

    @Test
    public void testIsInstall() {
        Assert.assertEquals(RwSplitServerParse.INSTALL, 0xff & RwSplitServerParse.parse(" install  ..."));
        Assert.assertEquals(RwSplitServerParse.INSTALL, 0xff & RwSplitServerParse.parse("INSTALL ..."));
        Assert.assertEquals(RwSplitServerParse.INSTALL, 0xff & RwSplitServerParse.parse(" Install   a"));
    }

    @Test
    public void testIsRename() {
        Assert.assertEquals(RwSplitServerParse.RENAME, 0xff & RwSplitServerParse.parse(" rename  ..."));
        Assert.assertEquals(RwSplitServerParse.RENAME, 0xff & RwSplitServerParse.parse("RENAME ..."));
        Assert.assertEquals(RwSplitServerParse.RENAME, 0xff & RwSplitServerParse.parse(" Rename   a"));
    }

    @Test
    public void testIsUnInstall() {
        Assert.assertEquals(RwSplitServerParse.UNINSTALL, 0xff & RwSplitServerParse.parse(" uninstall  ..."));
        Assert.assertEquals(RwSplitServerParse.UNINSTALL, 0xff & RwSplitServerParse.parse("UNINSTALL ..."));
        Assert.assertEquals(RwSplitServerParse.UNINSTALL, 0xff & RwSplitServerParse.parse(" Uninstall   a"));
    }

    @Test
    public void testIsStartTransaction() {
        Assert.assertEquals(RwSplitServerParse.START_TRANSACTION, 0xff & RwSplitServerParse.parse("  START TRANSACTION"));
        Assert.assertEquals(RwSplitServerParse.START_TRANSACTION, 0xff & RwSplitServerParse.parse(" start transaction"));
        Assert.assertEquals(RwSplitServerParse.START_TRANSACTION, 0xff & RwSplitServerParse.parse("  Start Transaction"));
    }

    @Test
    public void testIsStartSlave() {
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & RwSplitServerParse.parse("  START SLAVE  ..."));
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & RwSplitServerParse.parse(" start slave ..."));
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & RwSplitServerParse.parse("  Start Slave   a"));
    }

    @Test
    public void testIsAnalyze() {
        Assert.assertEquals(RwSplitServerParse.UNSUPPORT & 0xff, 0xff & RwSplitServerParse.parse("   ANALYZE TABLE test"));
        Assert.assertEquals(RwSplitServerParse.UNSUPPORT & 0xff, 0xff & RwSplitServerParse.parse(" analyze table test"));
        Assert.assertEquals(RwSplitServerParse.UNSUPPORT & 0xff, 0xff & RwSplitServerParse.parse("  Analyze Table test"));
    }

    @Test
    public void testIsCache() {
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & RwSplitServerParse.parse("    CACHE INDEX test"));
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & RwSplitServerParse.parse(" cache index test"));
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & RwSplitServerParse.parse(" Cache Index test"));
    }

    @Test
    public void testIsCheck() {
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & RwSplitServerParse.parse(" CHECK TABLE test"));
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & RwSplitServerParse.parse(" check table test"));
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & RwSplitServerParse.parse(" Check Table test"));
    }

    @Test
    public void testIsFlush() {
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & RwSplitServerParse.parse(" flush"));
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & RwSplitServerParse.parse(" FLUSH"));
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & RwSplitServerParse.parse(" Flush"));
    }

    @Test
    public void testIsLoadIndex() {
        Assert.assertEquals(RwSplitServerParse.UNSUPPORT & 0xff, 0xff & RwSplitServerParse.parse("  LOAD INDEX INTO CACHE"));
        Assert.assertEquals(RwSplitServerParse.UNSUPPORT & 0xff, 0xff & RwSplitServerParse.parse(" load index into cache"));
        Assert.assertEquals(RwSplitServerParse.UNSUPPORT & 0xff, 0xff & RwSplitServerParse.parse(" Load Index Into Cache"));
    }

    @Test
    public void testIsOptimize() {
        Assert.assertEquals(RwSplitServerParse.UNSUPPORT & 0xff, 0xff & RwSplitServerParse.parse("   OPTIMIZE TABLE test"));
        Assert.assertEquals(RwSplitServerParse.UNSUPPORT & 0xff, 0xff & RwSplitServerParse.parse(" optimize table test"));
        Assert.assertEquals(RwSplitServerParse.UNSUPPORT & 0xff, 0xff & RwSplitServerParse.parse(" Optimize Table Test"));
    }

    @Test
    public void testIsRepair() {
        Assert.assertEquals(RwSplitServerParse.UNSUPPORT & 0xff, 0xff & RwSplitServerParse.parse("  REPAIR TABLE test "));
        Assert.assertEquals(RwSplitServerParse.UNSUPPORT & 0xff, 0xff & RwSplitServerParse.parse(" repair table test "));
        Assert.assertEquals(RwSplitServerParse.UNSUPPORT & 0xff, 0xff & RwSplitServerParse.parse(" Repair Table test "));
    }

    @Test
    public void testIsReset() {
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & RwSplitServerParse.parse("   RESET test"));
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & RwSplitServerParse.parse(" reset test"));
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & RwSplitServerParse.parse(" Reset test"));
    }

    @Test
    public void testIsStop() {
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & RwSplitServerParse.parse("   STOP SLAVE"));
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & RwSplitServerParse.parse(" stop slave"));
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & RwSplitServerParse.parse(" Stop Slave"));
    }

    @Test
    public void testIsResetSlave() {
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & RwSplitServerParse.parse("   RESET SLAVE"));
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & RwSplitServerParse.parse(" reset slave"));
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & RwSplitServerParse.parse(" Reset Slave"));
    }

    @Test
    public void testIsChange() {
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & RwSplitServerParse.parse("   CHANGE MASTER TO"));
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & RwSplitServerParse.parse(" change master to"));
        Assert.assertEquals(RwSplitServerParse.OTHER & 0xff, 0xff & RwSplitServerParse.parse(" Change Master to"));
    }
}