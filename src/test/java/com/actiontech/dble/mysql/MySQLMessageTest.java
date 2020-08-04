/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.mysql;

import com.actiontech.dble.backend.mysql.MySQLMessage;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author mycat
 */
public class MySQLMessageTest {

    @Test
    public void testReadBytesWithNull() {
        byte[] bytes = new byte[]{1, 2, 3, 0, 5};
        MySQLMessage message = new MySQLMessage(bytes);
        byte[] ab = message.readBytesWithNull();
        Assert.assertEquals(3, ab.length);
        Assert.assertEquals(4, message.position());
    }

    @Test
    public void testReadBytesWithNull2() {
        byte[] bytes = new byte[]{0, 1, 2, 3, 0, 5};
        MySQLMessage message = new MySQLMessage(bytes);
        byte[] ab = message.readBytesWithNull();
        Assert.assertEquals(0, ab.length);
        Assert.assertEquals(1, message.position());
    }

    @Test
    public void testReadBytesWithNull3() {
        byte[] bytes = new byte[]{};
        MySQLMessage message = new MySQLMessage(bytes);
        byte[] ab = message.readBytesWithNull();
        Assert.assertEquals(0, ab.length);
        Assert.assertEquals(0, message.position());
    }

}