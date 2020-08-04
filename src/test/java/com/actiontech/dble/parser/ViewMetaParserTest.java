package com.actiontech.dble.parser;

import com.actiontech.dble.meta.ViewMetaParser;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Created by szf on 2017/10/10.
 */
public class ViewMetaParserTest {

    @Test
    public void testGetViewName() {
        ViewMetaParser x = new ViewMetaParser("create view dddddd as select * from suntest");
        Assert.assertEquals("dddddd", x.getViewName());
        x = new ViewMetaParser("create or replace  view xxxxx as select * from suntest");
        Assert.assertEquals("xxxxx", x.getViewName());
        x = new ViewMetaParser("create or replace  view x_xx__xx as select * from suntest");
        Assert.assertEquals("x_xx__xx", x.getViewName());
        x = new ViewMetaParser("create      or        replace         view       x_xx__xx      as select * from suntest");
        Assert.assertEquals("x_xx__xx", x.getViewName());
        x = new ViewMetaParser("       create      or        replace         view        x_xx__xx      as select * from suntest");
        Assert.assertEquals("x_xx__xx", x.getViewName());
        x = new ViewMetaParser("       create      or        replace         view        x_xx__xx(id,name)as select * from suntest");
        Assert.assertEquals("x_xx__xx", x.getViewName());
    }


    @Test
    public void getViewColumnTest() {
        ViewMetaParser x = new ViewMetaParser("       create      or        replace         view        x_xx__xx(id,name)as select * from suntest");
        x.getViewName();
        List<String> testlist = x.getViewColumn();
        Assert.assertEquals("id", testlist.get(0));
        Assert.assertEquals("name", testlist.get(1));
    }


    @Test
    public void getViewNodeSupportTest() {
        ViewMetaParser x = new ViewMetaParser("create      or replace  ALGORITHM = MERGE        view        x_xx__xx(id,name)as select * from suntest");
        Assert.assertEquals("", x.getViewName());
        x = new ViewMetaParser("create      or replace  DEFINER  = CURRENT_USER         view        x_xx__xx(id,name)as select * from suntest");
        Assert.assertEquals("", x.getViewName());
        x = new ViewMetaParser("create      or replace  SQL SECURITY DEFINER       view        x_xx__xx(id,name)as select * from suntest");
        Assert.assertEquals("", x.getViewName());

    }

    @Test
    public void parseTotalTest() {
        ViewMetaParser x = new ViewMetaParser("       create      or        replace         view        x_xx__xx(id,name)as select * from suntest");
        x.getViewName();
        List<String> testlist = x.getViewColumn();
        Assert.assertEquals("id", testlist.get(0));
        Assert.assertEquals("name", testlist.get(1));
        Assert.assertEquals(" select * from suntest", x.parseSelectSQL());

    }


}
