package com.actiontech.dble.parser;

import com.alibaba.druid.sql.dialect.mysql.parser.MySqlCreateTableParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import org.junit.Test;

/**
 * Created by szf on 2018/4/3.
 */
public class MetaParseTest {

    @Test
    public void createTableCommentTest() {
        String sql = "CREATE TABLE `suntest` (\n" +
                "\t`id` INT (11) DEFAULT NULL,\n" +
                "\t`name` VARCHAR (50) DEFAULT NULL\n" +
                ") ENGINE = INNODB DEFAULT CHARSET = latin1";
        SQLStatementParser parser = new MySqlCreateTableParser(sql);
        parser.parseCreateTable();

        sql = "CREATE TABLE `suntest` (\n" +
                "\t`id` INT (11) NOT NULL,\n" +
                "\t`name` VARCHAR (50) DEFAULT NULL,\n" +
                "\tPRIMARY KEY (`id`)\n" +
                ") ENGINE = INNODB DEFAULT CHARSET = latin1";
        parser = new MySqlCreateTableParser (sql);
        parser.parseCreateTable();


        sql = "CREATE TABLE `suntest` (\n" +
                "\t`id` INT (11) NOT NULL,\n" +
                "\t`name` VARCHAR (50) DEFAULT NULL,\n" +
                "\tPRIMARY KEY (`id`),\n" +
                "\tUNIQUE KEY `name` (`name`)\n" +
                ") ENGINE = INNODB DEFAULT CHARSET = latin1";
        parser = new MySqlCreateTableParser (sql);
        parser.parseCreateTable();


        sql = "CREATE TABLE `suntest` (\n" +
                "\t`id` INT (11) NOT NULL,\n" +
                "\t`name` VARCHAR (50) DEFAULT NULL,\n" +
                "\tPRIMARY KEY (`id`),\n" +
                "\tUNIQUE KEY `name` (`name`)\n" +
                ") ENGINE = INNODB DEFAULT CHARSET = latin1 COMMENT = 'suntest_comment'";
        parser = new MySqlCreateTableParser (sql);
        parser.parseCreateTable();


        sql = "CREATE TABLE `suntest` (\n" +
                "  `id` int(11) NOT NULL COMMENT 'test,test)',\n" +
                "  `name` varchar(50) DEFAULT NULL,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  UNIQUE KEY `name` (`name`)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8 MIN_ROWS=10 MAX_ROWS=1000 AVG_ROW_LENGTH=20 CHECKSUM=1 COMMENT='suntest_comment'";
        parser = new MySqlCreateTableParser (sql);
        parser.parseCreateTable();


        sql = "CREATE TABLE `suntest` (\n" +
                "  `id` int(11) NOT NULL COMMENT 'test''test',\n" +
                "  `name` varchar(50) DEFAULT NULL,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  KEY `name_index` (`name`) COMMENT 'ffff'\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8 MIN_ROWS=10 MAX_ROWS=1000 AVG_ROW_LENGTH=20 CHECKSUM=1 COMMENT='suntest_comment'";
        parser = new MySqlCreateTableParser (sql);
        parser.parseCreateTable();

        sql = "CREATE TABLE `suntest` (\n" +
                "  `id` int(11) NOT NULL COMMENT 'test''test',\n" +
                "  `name` varchar(50) DEFAULT NULL,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  UNIQUE KEY `name` (`name`) COMMENT 'test_comment'\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8 MIN_ROWS=10 MAX_ROWS=1000 AVG_ROW_LENGTH=20 CHECKSUM=1 COMMENT='suntest_comment'";
        parser = new MySqlCreateTableParser (sql);
        parser.parseCreateTable();


        sql = "CREATE TABLE `suntest` (\n" +
                "  `id` int(11) NOT NULL COMMENT 'test''test',\n" +
                "  `name` varchar(20) DEFAULT NULL,\n" +
                "  `rid` int(11) DEFAULT NULL,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  KEY `rid` (`rid`),\n" +
                "  CONSTRAINT `suntest_ibfk_1` FOREIGN KEY (`rid`) REFERENCES `dbletest` (`id`) COMMENT 'XXXX' " +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8 MIN_ROWS=10 MAX_ROWS=1000 AVG_ROW_LENGTH=20 CHECKSUM=1 COMMENT='suntest_comment'";
        parser = new MySqlCreateTableParser (sql);
        parser.parseCreateTable();


        sql = "CREATE TABLE `suntest` (\n" +
                "  `id` int(11) NOT NULL COMMENT 'test''test',\n" +
                "  `name` varchar(50) DEFAULT NULL,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  FULLTEXT KEY `name_index` (`name`) COMMENT 'sdfasdf'\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8 MIN_ROWS=10 MAX_ROWS=1000 AVG_ROW_LENGTH=20 CHECKSUM=1 COMMENT='suntest_comment'";
        parser = new MySqlCreateTableParser (sql);
        parser.parseCreateTable();


        sql = "CREATE TABLE `suntest` (\n" +
                "  `id` int(11) NOT NULL COMMENT 'test''test',\n" +
                "  `name` varchar(20) DEFAULT NULL,\n" +
                "  `rid` int(11) DEFAULT NULL,\n" +
                "  PRIMARY KEY (`id`) COMMENT 'ffff',\n" +
                "  KEY `rid` (`rid`) COMMENT 'xxxxx',\n" +
                "  CONSTRAINT `suntest_ibfk_1` FOREIGN KEY (`rid`) REFERENCES `dbletest` (`id`)\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8 MIN_ROWS=10 MAX_ROWS=1000 AVG_ROW_LENGTH=20 CHECKSUM=1 COMMENT='suntest_comment'";
        parser = new MySqlCreateTableParser (sql);
        parser.parseCreateTable();

    }
}
