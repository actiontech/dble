package com.actiontech.dble.parser;

import com.actiontech.dble.util.FormatUtil;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by szf on 2018/4/3.
 */
public class MetaParseTest {

    @Test
    public void createTableCommentTest() {
        String sql = "";
        String result = "";

        sql = "CREATE TABLE `suntest` (\n" +
                "\t`id` INT (11) DEFAULT NULL,\n" +
                "\t`name` VARCHAR (50) DEFAULT NULL\n" +
                ") ENGINE = INNODB DEFAULT CHARSET = latin1";
        result = "CREATE TABLE `suntest` (\n" +
                "\t`id` INT (11) DEFAULT NULL,\n" +
                "\t`name` VARCHAR (50) DEFAULT NULL\n" +
                ") ENGINE = INNODB DEFAULT CHARSET = latin1";
        Assert.assertEquals(FormatUtil.deleteComment(sql).trim(), result.trim());

        sql = "CREATE TABLE `suntest` (\n" +
                "\t`id` INT (11) NOT NULL,\n" +
                "\t`name` VARCHAR (50) DEFAULT NULL,\n" +
                "\tPRIMARY KEY (`id`)\n" +
                ") ENGINE = INNODB DEFAULT CHARSET = latin1";
        result = "CREATE TABLE `suntest` (\n" +
                "\t`id` INT (11) NOT NULL,\n" +
                "\t`name` VARCHAR (50) DEFAULT NULL,\n" +
                "\tPRIMARY KEY (`id`)\n" +
                ") ENGINE = INNODB DEFAULT CHARSET = latin1";
        Assert.assertEquals(FormatUtil.deleteComment(sql).trim(), result.trim());


        sql = "CREATE TABLE `suntest` (\n" +
                "\t`id` INT (11) NOT NULL,\n" +
                "\t`name` VARCHAR (50) DEFAULT NULL,\n" +
                "\tPRIMARY KEY (`id`),\n" +
                "\tUNIQUE KEY `name` (`name`)\n" +
                ") ENGINE = INNODB DEFAULT CHARSET = latin1";
        result = "CREATE TABLE `suntest` (\n" +
                "\t`id` INT (11) NOT NULL,\n" +
                "\t`name` VARCHAR (50) DEFAULT NULL,\n" +
                "\tPRIMARY KEY (`id`),\n" +
                "\tUNIQUE KEY `name` (`name`)\n" +
                ") ENGINE = INNODB DEFAULT CHARSET = latin1";
        Assert.assertEquals(FormatUtil.deleteComment(sql).trim(), result.trim());


        sql = "CREATE TABLE `suntest` (\n" +
                "\t`id` INT (11) NOT NULL,\n" +
                "\t`name` VARCHAR (50) DEFAULT NULL,\n" +
                "\tPRIMARY KEY (`id`),\n" +
                "\tUNIQUE KEY `name` (`name`)\n" +
                ") ENGINE = INNODB DEFAULT CHARSET = latin1 COMMENT = 'suntest_comment'";
        result = "CREATE TABLE `suntest` (\n" +
                "\t`id` INT (11) NOT NULL,\n" +
                "\t`name` VARCHAR (50) DEFAULT NULL,\n" +
                "\tPRIMARY KEY (`id`),\n" +
                "\tUNIQUE KEY `name` (`name`)\n" +
                ") ENGINE = INNODB DEFAULT CHARSET = latin1 ";
        Assert.assertEquals(FormatUtil.deleteComment(sql).trim(), result.trim());


        sql = "CREATE TABLE `suntest` (\n" +
                "  `id` int(11) NOT NULL COMMENT 'test,test)',\n" +
                "  `name` varchar(50) DEFAULT NULL,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  UNIQUE KEY `name` (`name`)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8 MIN_ROWS=10 MAX_ROWS=1000 AVG_ROW_LENGTH=20 CHECKSUM=1 COMMENT='suntest_comment'";
        result = "CREATE TABLE `suntest` (\n" +
                "  `id` int(11) NOT NULL,\n" +
                "  `name` varchar(50) DEFAULT NULL,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  UNIQUE KEY `name` (`name`)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8 MIN_ROWS=10 MAX_ROWS=1000 AVG_ROW_LENGTH=20 CHECKSUM=1 COMMENT='suntest_comment'";
        Assert.assertEquals(FormatUtil.deleteComment(sql).trim(), result.trim());

        sql = "CREATE TABLE `suntest` (\n" +
                "  `id` int(11) NOT NULL COMMENT 'test''test',\n" +
                "  `name` varchar(50) DEFAULT NULL,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  UNIQUE KEY `name` (`name`) COMMENT 'test_comment'\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8 MIN_ROWS=10 MAX_ROWS=1000 AVG_ROW_LENGTH=20 CHECKSUM=1 COMMENT='suntest_comment'";
        result = "CREATE TABLE `suntest` (\n" +
                "  `id` int(11) NOT NULL,\n" +
                "  `name` varchar(50) DEFAULT NULL,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  UNIQUE KEY `name` (`name`)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8 MIN_ROWS=10 MAX_ROWS=1000 AVG_ROW_LENGTH=20 CHECKSUM=1 COMMENT='suntest_comment'";
        Assert.assertEquals(FormatUtil.deleteComment(sql).trim(), result.trim());

        sql = "CREATE TABLE `suntest` (\n" +
                "  `id` int(11) NOT NULL COMMENT 'test''test',\n" +
                "  `name` varchar(50) DEFAULT NULL,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  UNIQUE KEY `name` (`name`) COMMENT 'test_''''''comment'\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8 MIN_ROWS=10 MAX_ROWS=1000 AVG_ROW_LENGTH=20 CHECKSUM=1 COMMENT='suntest_comment'";
        result = "CREATE TABLE `suntest` (\n" +
                "  `id` int(11) NOT NULL,\n" +
                "  `name` varchar(50) DEFAULT NULL,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  UNIQUE KEY `name` (`name`)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8 MIN_ROWS=10 MAX_ROWS=1000 AVG_ROW_LENGTH=20 CHECKSUM=1 COMMENT='suntest_comment'";
        Assert.assertEquals(FormatUtil.deleteComment(sql).trim(), result.trim());

    }
}
