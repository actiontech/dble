/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.handler;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class SetHandlerTest {
    @Test
    public void testConvertCharsetKeyWord() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method convertCharsetKeyWord = SetHandler.class.getDeclaredMethod("convertCharsetKeyWord", String.class);
        convertCharsetKeyWord.setAccessible(true);
        Assert.assertEquals("set character set utf8", convertCharsetKeyWord.invoke(null, "set charset utf8"));
        Assert.assertEquals("SET character set UTF8", convertCharsetKeyWord.invoke(null, "SET CHARSET UTF8"));
        Assert.assertEquals("SET names utf8,character set UTF8,character set gbk,@@tx_readonly=1", convertCharsetKeyWord.invoke(null, "SET names utf8,CHARSET UTF8,CHARSET gbk,@@tx_readonly=1"));
        Assert.assertEquals("SET names utf8,character set UTF8,character set gbk,@@tx_readonly=1", convertCharsetKeyWord.invoke(null, "SET names utf8,CHARSET UTF8,CHARSET gbk,@@tx_readonly=1"));
    }

//    @Test
    //    public void testCheckSetNamesSyntax() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    //        Method checkSetNamesSyntax = SetHandler.class.getDeclaredMethod("checkSetNamesSyntax", String.class);
    //        checkSetNamesSyntax.setAccessible(true);
    //        Assert.assertEquals(true, checkSetNamesSyntax.invoke(null, "SET NAMES utf8"));
    //        Assert.assertEquals(true, checkSetNamesSyntax.invoke(null, "set names utf8"));
    //        Assert.assertEquals(true, checkSetNamesSyntax.invoke(null, "set names 'utf8'"));
    //        Assert.assertEquals(true, checkSetNamesSyntax.invoke(null, "set names `utf8`"));
    //        Assert.assertEquals(true, checkSetNamesSyntax.invoke(null, "set names DEFAULT"));
    //        Assert.assertEquals(true, checkSetNamesSyntax.invoke(null, "set names utf8 COLLATE utf8_bin"));
    //        Assert.assertEquals(true, checkSetNamesSyntax.invoke(null, "set names utf8 COLLATE default"));
    //        Assert.assertEquals(true, checkSetNamesSyntax.invoke(null, "set names 'utf8' COLLATE utf8_bin"));
    //        Assert.assertEquals(true, checkSetNamesSyntax.invoke(null, "set names `utf8` COLLATE default"));
    //        Assert.assertEquals(true, checkSetNamesSyntax.invoke(null, "set names utf8 COLLATE 'utf8_bin'"));
    //        Assert.assertEquals(true, checkSetNamesSyntax.invoke(null, "set names utf8 COLLATE `utf8_bin`"));
    //        Assert.assertEquals(true, checkSetNamesSyntax.invoke(null, "set names 'utf8' COLLATE 'utf8_bin'"));
    //        Assert.assertEquals(true, checkSetNamesSyntax.invoke(null, "set names `utf8` COLLATE `utf8_bin`"));
    //        Assert.assertEquals(false, checkSetNamesSyntax.invoke(null, "set names utf8 2"));
    //        Assert.assertEquals(false, checkSetNamesSyntax.invoke(null, "set names utf8 COLLATION utf8_bin"));
    //    }
}
