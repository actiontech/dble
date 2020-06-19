/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.util;

import junit.framework.TestCase;
import org.junit.Assert;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author mycat
 */
public class SmallSetTest extends TestCase {

    public void assertListEquals(Collection<? extends Object> col, Object... objects) {
        if (objects == null) {
            Assert.assertTrue(col.isEmpty());
        }
        Assert.assertEquals(objects.length, col.size());
        int i = 0;
        for (Object o : col) {
            Assert.assertEquals(objects[i++], o);
        }
    }

    public void testSet() throws Exception {
        SmallSet<Object> sut = new SmallSet<Object>();
        sut.add(1);
        Assert.assertEquals(1, sut.size());
        Iterator<Object> iter = sut.iterator();
        Assert.assertTrue(iter.hasNext());
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(1, iter.next());
        Assert.assertFalse(iter.hasNext());
        assertListEquals(sut, 1);
        try {
            iter.next();
            Assert.assertTrue(false);
        } catch (Exception e) {
        }

        sut = new SmallSet<Object>();
        sut.add(1);
        Assert.assertEquals(1, sut.size());
        iter = sut.iterator();
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(1, iter.next());
        Assert.assertFalse(iter.hasNext());
        assertListEquals(sut, 1);
        iter.remove();
        Assert.assertEquals(0, sut.size());
        Assert.assertFalse(iter.hasNext());
        iter = sut.iterator();
        Assert.assertFalse(iter.hasNext());

        sut = new SmallSet<Object>();
        sut.add(1);
        sut.add(2);
        Assert.assertEquals(2, sut.size());
        iter = sut.iterator();
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(1, iter.next());
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(2, iter.next());
        Assert.assertFalse(iter.hasNext());
        assertListEquals(sut, 1, 2);
        iter.remove();
        assertListEquals(sut, 1);
        Assert.assertEquals(1, sut.size());
        Assert.assertFalse(iter.hasNext());

        sut = new SmallSet<Object>();
        sut.add(1);
        sut.add(2);
        assertListEquals(sut, 1, 2);
        Assert.assertEquals(2, sut.size());
        iter = sut.iterator();
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(1, iter.next());
        Assert.assertTrue(iter.hasNext());
        iter.remove();
        assertListEquals(sut, 2);
        Assert.assertEquals(1, sut.size());
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(2, iter.next());
        Assert.assertFalse(iter.hasNext());

        sut = new SmallSet<Object>();
        sut.add(1);
        sut.add(2);
        assertListEquals(sut, 1, 2);
        Assert.assertEquals(2, sut.size());
        iter = sut.iterator();
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(1, iter.next());
        Assert.assertTrue(iter.hasNext());
        iter.remove();
        assertListEquals(sut, 2);
        Assert.assertEquals(1, sut.size());
        iter = sut.iterator();
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(2, iter.next());
        Assert.assertFalse(iter.hasNext());

        sut = new SmallSet<Object>();
        sut.add(1);
        sut.add(2);
        assertListEquals(sut, 1, 2);
        Assert.assertEquals(2, sut.size());
        iter = sut.iterator();
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(1, iter.next());
        Assert.assertTrue(iter.hasNext());
        iter.remove();
        assertListEquals(sut, 2);
        Assert.assertEquals(1, sut.size());
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(2, iter.next());
        iter.remove();
        assertListEquals(sut);
        iter = sut.iterator();
        Assert.assertFalse(iter.hasNext());

        sut = new SmallSet<Object>();
        sut.add(1);
        sut.add(2);
        sut.add(3);
        assertListEquals(sut, 1, 2, 3);
        iter = sut.iterator();
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(1, iter.next());
        assertListEquals(sut, 1, 2, 3);
        iter.remove();
        assertListEquals(sut, 2, 3);
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(2, iter.next());
        iter.remove();
        assertListEquals(sut, 3);
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(3, iter.next());
        Assert.assertFalse(iter.hasNext());
        iter.remove();
        assertListEquals(sut);
        Assert.assertFalse(iter.hasNext());
        iter = sut.iterator();
        Assert.assertFalse(iter.hasNext());

        sut = new SmallSet<Object>();
        sut.add(1);
        sut.add(2);
        sut.add(3);
        assertListEquals(sut, 1, 2, 3);
        iter = sut.iterator();
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(1, iter.next());
        assertListEquals(sut, 1, 2, 3);
        iter.remove();
        assertListEquals(sut, 2, 3);
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(2, iter.next());
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(3, iter.next());
        Assert.assertFalse(iter.hasNext());
        iter.remove();
        assertListEquals(sut, 2);
        Assert.assertFalse(iter.hasNext());
        iter = sut.iterator();
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(2, iter.next());
    }

}