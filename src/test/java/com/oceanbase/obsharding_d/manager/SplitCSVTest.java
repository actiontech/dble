package com.oceanbase.obsharding_d.manager;

import com.oceanbase.obsharding_d.route.parser.util.Pair;
import com.oceanbase.obsharding_d.services.manager.dump.parse.InsertParser;
import com.oceanbase.obsharding_d.services.manager.dump.parse.InsertQueryPos;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class SplitCSVTest {
    @Test
    public void test1() {
        String sql = "replace  into table1(col1,column2) values(1,'3'),(2,'5'),(3,';,');";
        char[] sqlArray = sql.toCharArray();
        InsertParser obj = new InsertParser(sql);
        InsertQueryPos pos = obj.parseStatement();
        Assert.assertEquals(0, pos.getQueryRange().getKey().intValue());
        Assert.assertEquals(sql.length(), pos.getQueryRange().getValue().intValue());
        Assert.assertTrue(pos.isReplace());
        Assert.assertFalse(pos.isIgnore());
        Assert.assertEquals("table1", pos.getTableName());
        Assert.assertEquals("col1,column2", getSubString(sqlArray, pos.getColumnRange()));
        List<Pair<Integer, Integer>> valueRange = pos.getValuesRange();
        // first value
        Assert.assertEquals("1,'3'", getSubString(sqlArray, valueRange.get(0)));
        // 3nd value
        Assert.assertEquals("3,';,'", getSubString(sqlArray, valueRange.get(2)));
        // 3nd value ,2th col
        Pair<Integer, Integer> valueItemRange = pos.getValueItemsRange().get(2).get(1);
        Assert.assertEquals("';,'", getSubString(sqlArray, valueItemRange));

    }

    @Test
    public void test2() {
        String sql = "insert ignore into table1(col1,column2) values(1,'3'),(2,'5'),(3,';,');";
        char[] sqlArray = sql.toCharArray();
        InsertParser obj = new InsertParser(sql);
        InsertQueryPos pos = obj.parseStatement();
        Assert.assertEquals(0, pos.getQueryRange().getKey().intValue());
        Assert.assertEquals(sql.length(), pos.getQueryRange().getValue().intValue());
        Assert.assertFalse(pos.isReplace());
        Assert.assertTrue(pos.isIgnore());
        Assert.assertEquals("table1", pos.getTableName());
        Assert.assertEquals("col1,column2", getSubString(sqlArray, pos.getColumnRange()));
        List<Pair<Integer, Integer>> valueRange = pos.getValuesRange();
        // first value
        Assert.assertEquals("1,'3'", getSubString(sqlArray, valueRange.get(0)));
        // 3nd value
        Assert.assertEquals("3,';,'", getSubString(sqlArray, valueRange.get(2)));
        // 3nd value ,2th col
        Pair<Integer, Integer> valueItemRange = pos.getValueItemsRange().get(2).get(1);
        Assert.assertEquals("';,'", getSubString(sqlArray, valueItemRange));

    }

    @Test
    public void test3() {
        String sql = "insert  into table1  values(1,'3'),(2,'5'),(3,';,');";
        char[] sqlArray = sql.toCharArray();
        InsertParser obj = new InsertParser(sql);
        InsertQueryPos pos = obj.parseStatement();
        Assert.assertEquals(0, pos.getQueryRange().getKey().intValue());
        Assert.assertEquals(sql.length(), pos.getQueryRange().getValue().intValue());
        Assert.assertFalse(pos.isReplace());
        Assert.assertFalse(pos.isIgnore());
        Assert.assertEquals("table1", pos.getTableName());
        List<Pair<Integer, Integer>> valueRange = pos.getValuesRange();
        // first value
        Assert.assertEquals("1,'3'", getSubString(sqlArray, valueRange.get(0)));
        // 3nd value
        Assert.assertEquals("3,';,'", getSubString(sqlArray, valueRange.get(2)));
        // 3nd value ,2th col
        Pair<Integer, Integer> valueItemRange = pos.getValueItemsRange().get(2).get(1);
        Assert.assertEquals("';,'", getSubString(sqlArray, valueItemRange));

        Assert.assertEquals(";,", getValueString(sqlArray, valueItemRange));

        Pair<Integer, Integer> valueItemRange1 = pos.getValueItemsRange().get(2).get(0);
        Assert.assertEquals("3", getValueString(sqlArray, valueItemRange1));
    }

    @Test
    public void test4() {
        Assert.assertNotNull(getInsertQueryPos("insert ignore into table1(col1,column2) values(1,'3'),(2,'5'),(3,';,');").getQueryRange());
        Assert.assertNull(getInsertQueryPos("insert ignore into table1(col1,column2) values(1,'3'),(2,'5'),(3,';,')").getQueryRange());
        Assert.assertNull(getInsertQueryPos("insert ignore into table1(col1,column2) values(1,'3'),(2,'5'),(3,';,'").getQueryRange());
        Assert.assertNull(getInsertQueryPos("insert ignore into table1(col1,column2) values(1,'3'),(2,'5'),(3,';").getQueryRange());
        Assert.assertNull(getInsertQueryPos("insert ignore into table1(col1,column2) values(1,").getQueryRange());
        Assert.assertNull(getInsertQueryPos("insert ignore into table1(col1,column2) values").getQueryRange());
        Assert.assertNull(getInsertQueryPos("insert ignore into table1(col1,column2) val").getQueryRange());
        Assert.assertNull(getInsertQueryPos("insert ignore into table1(col1,column2)").getQueryRange());
        Assert.assertNull(getInsertQueryPos("insert ignore into table1(col1,").getQueryRange());
        Assert.assertNull(getInsertQueryPos("insert ignore into table1(col").getQueryRange());
        Assert.assertNull(getInsertQueryPos("insert ignore into table").getQueryRange());
        Assert.assertNull(getInsertQueryPos("insert ignore into ").getQueryRange());
        Assert.assertNull(getInsertQueryPos("insert ignore in ").getQueryRange());
        Assert.assertNull(getInsertQueryPos("insert ignore  ").getQueryRange());
        Assert.assertNull(getInsertQueryPos("insert ign").getQueryRange());
        Assert.assertNull(getInsertQueryPos("insert").getQueryRange());
        Assert.assertNull(getInsertQueryPos("inser"));
    }

    @Test
    public void test5() {
        String s1 = "insert  into table1  values(4,'z'),(5,'y'),(6,'x');\n";
        String s2 = "insert  into table1  values(1,'3'),(2,'5'),(3,';,');";
        String sql = s1 + s2;
        char[] sqlArray = sql.toCharArray();
        InsertParser obj = new InsertParser(sql);
        InsertQueryPos pos1 = obj.parseStatement(); // first time
        InsertQueryPos pos = obj.parseStatement(); //second time
        Assert.assertEquals(s1.length(), pos.getQueryRange().getKey().intValue());
        Assert.assertEquals(sql.length(), pos.getQueryRange().getValue().intValue());
        Assert.assertFalse(pos.isReplace());
        Assert.assertFalse(pos.isIgnore());
        Assert.assertEquals("table1", pos.getTableName());
        List<Pair<Integer, Integer>> valueRange = pos.getValuesRange();
        // first value
        Assert.assertEquals("1,'3'", getSubString(sqlArray, valueRange.get(0)));
        // 3nd value
        Assert.assertEquals("3,';,'", getSubString(sqlArray, valueRange.get(2)));
        // 3nd value ,2th col
        Pair<Integer, Integer> valueItemRange = pos.getValueItemsRange().get(2).get(1);
        Assert.assertEquals("';,'", getSubString(sqlArray, valueItemRange));

        Assert.assertEquals(";,", getValueString(sqlArray, valueItemRange));

        Pair<Integer, Integer> valueItemRange1 = pos.getValueItemsRange().get(2).get(0);
        Assert.assertEquals("3", getValueString(sqlArray, valueItemRange1));
    }

    @Test
    public void test6() {
        String s11 = ");\ninsert  into table1  values(4,'z'),(5,'y'),(6,'x');\n";
        String s12 = "insert  into table1  values(7,'z7'),(8,'8'),(9,'9');\n";
        String s13 = "insert  into table1  value";

        String s21 = "s(1,'3'),(2,'5'),(3,';,');\n";
        String s22 = "insert  into table1  values(10,'10'),(11,'11'),(12,'12');\n";
        String s23 = "insert  into table1  values(13,'13'),(14";
        String s1 = s11 + s12 + s13;
        String s2 = s21 + s22 + s23;

        char[] sqlArray1 = s1.toCharArray();
        char[] sqlArray2 = s2.toCharArray();
        InsertParser obj1 = new InsertParser(s1);
        obj1.findInsert();
        InsertQueryPos tmpPos = obj1.parseStatement();
        int lastEndPos = 0;
        // not insert query or insert not finish
        while (tmpPos != null && tmpPos.getQueryRange() != null) {
            //do something about tmpPos
            lastEndPos = tmpPos.getQueryRange().getValue();
            tmpPos = obj1.parseStatement();
        }

        InsertParser obj2 = new InsertParser(s2);
        // may need loop
        int nextStartPos = obj2.findInsert();

        // cat last end and next start
        char[] sqlArray = genCatChars(sqlArray1, sqlArray2, lastEndPos, nextStartPos);
        String catQuery = new String(sqlArray);

        InsertParser obj = new InsertParser(catQuery);
        InsertQueryPos pos = obj.parseStatement();

        // then do obj2 ... obj2.parseStatement();

        Assert.assertFalse(pos.isReplace());
        Assert.assertFalse(pos.isIgnore());
        Assert.assertEquals("table1", pos.getTableName());
        List<Pair<Integer, Integer>> valueRange = pos.getValuesRange();
        // first value
        Assert.assertEquals("1,'3'", getSubString(sqlArray, valueRange.get(0)));
        // 3nd value
        Assert.assertEquals("3,';,'", getSubString(sqlArray, valueRange.get(2)));
        // 3nd value ,2th col
        Pair<Integer, Integer> valueItemRange = pos.getValueItemsRange().get(2).get(1);
        Assert.assertEquals("';,'", getSubString(sqlArray, valueItemRange));

        Assert.assertEquals(";,", getValueString(sqlArray, valueItemRange));

        Pair<Integer, Integer> valueItemRange1 = pos.getValueItemsRange().get(2).get(0);
        Assert.assertEquals("3", getValueString(sqlArray, valueItemRange1));
    }


    private InsertQueryPos getInsertQueryPos(String sql) {
        InsertParser obj = new InsertParser(sql);
        return obj.parseStatement();
    }

    private String getSubString(char[] src, Pair<Integer, Integer> range) {
        StringBuilder target = new StringBuilder();
        addSubString(src, range, target);
        return target.toString();
    }


    //utils

    private char[] genCatChars(char[] lastChar, char[] nextChar, int lastEndPos, int nextStartPos) {
        char[] sqlChars = new char[lastChar.length - lastEndPos + nextStartPos];
        System.arraycopy(lastChar, lastEndPos, sqlChars, 0, lastChar.length - lastEndPos);
        System.arraycopy(nextChar, 0, sqlChars, lastChar.length - lastEndPos, nextStartPos);
        return sqlChars;
    }

    private String getValueString(char[] src, Pair<Integer, Integer> range) {
        StringBuilder target = new StringBuilder();
        int start = range.getKey();
        int end = range.getValue();
        if (src[start] == '\'' && src[end - 1] == '\'') {
            start++;
            end--;
        }
        for (int i = start; i < end; i++) {
            target.append(src[i]);
        }
        return target.toString();
    }

    private void addSubString(char[] src, Pair<Integer, Integer> range, StringBuilder target) {
        for (int i = range.getKey(); i < range.getValue(); i++) {
            target.append(src[i]);
        }
    }
}
