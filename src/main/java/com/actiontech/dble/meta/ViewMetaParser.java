/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.route.parser.util.ParseUtil;
import com.actiontech.dble.util.StringUtil;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by szf on 2017/10/9.
 */
public class ViewMetaParser {

    public static final int TYPE_CREATE_VIEW = 1;
    public static final int TYPE_REPLACE_VIEW = 2;
    public static final int TYPE_ALTER_VIEW = 3;

    private int offset = -1;
    private String originalSql;
    private int type = TYPE_CREATE_VIEW;

    public ViewMetaParser(String originalSql) {
        this.originalSql = originalSql;
    }

    public void parseCreateView(ViewMeta viewMeta) throws SQLException {
        String viewName = getViewName();
        if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
            viewName = viewName.toLowerCase();
        }

        String vSchema = null;
        //delete the schema if exists
        if (viewName.indexOf('.') != -1) {
            String[] viewNameInfo = viewName.split("\\.");
            vSchema = StringUtil.removeBackQuote(viewNameInfo[0]);
            if (DbleServer.getInstance().getConfig().getSchemas().get(vSchema) == null) {
                throw new SQLException("Unknown database '" + vSchema + "'", "42000", ErrorCode.ER_BAD_DB_ERROR);
            }

            viewName = viewNameInfo[1];
        }
        viewName = StringUtil.removeBackQuote(viewName);

        if (!StringUtil.isEmpty(vSchema))
            viewMeta.setSchema(vSchema);
        //get the name of view
        viewMeta.setViewName(viewName);
        //get the list of column name
        viewMeta.setViewColumnMeta(getViewColumn());
        //get select sql
        viewMeta.setSelectSql(parseSelectSQL());

    }

    public String getViewName() {
        //parse the sql
        while (true) {
            switch (originalSql.charAt(++offset)) {
                case ' ':
                case '\t':
                case '\r':
                case '\n':
                    continue;
                case '/':
                    offset = ParseUtil.comment(originalSql, offset);
                    offset++;
                    break;
                case 'a':
                    offset = offset + 5;
                    type = TYPE_ALTER_VIEW;
                    return parseCreateOrReplace();
                default:
                    //skip the create because in ServerParse is already know
                    offset = offset + 6;
                    return parseCreateOrReplace();
            }
        }
    }

    public String parseCreateOrReplace() {

        while (true) {
            char next = originalSql.charAt(++offset);
            if (next != ' ' && next != '\t' && next != '\r' && next != '\n') {
                if (next == 'o' || next == 'O') {
                    offset += 2;
                    while (true) {
                        next = originalSql.charAt(++offset);
                        if (next == 'r' || next == 'R') {
                            offset += 7;
                            this.type = TYPE_REPLACE_VIEW;
                            return parseCreateOrReplace();
                        }
                    }
                } else if (next == 'v' || next == 'V') {
                    //skip the word view because in ServerParse is already know
                    offset += 4;
                    int viewNameStat = 0;
                    while (true) {
                        next = originalSql.charAt(++offset);
                        if (next != ' ' && next != '\t' &&
                                next != '\r' &&
                                next != '\n' &&
                                viewNameStat == 0) {
                            viewNameStat = offset;
                        } else if ((next == ' ' || next == '\t' ||
                                next == '\r' ||
                                next == '\n' ||
                                next == '(') && viewNameStat != 0) {
                            return originalSql.substring(viewNameStat, offset);
                        }
                    }
                } else {
                    return "";
                }
            }
        }
    }


    public String parseSelectSQL() {
        while (true) {
            char next = originalSql.charAt(offset++);
            if ((next == 'a' || next == 'A') &&
                    (originalSql.charAt(offset) == 's' ||
                            originalSql.charAt(offset) == 'S')) {
                offset++;
                break;
            } else if (offset == originalSql.length() - 1) {
                throw new RuntimeException("You have an error in your SQL syntax;");
            }

        }
        return originalSql.substring(offset);
    }


    public List<String> getViewColumn() {
        int start = 0;
        String columnList = "";
        while (true) {
            char next = originalSql.charAt(offset++);
            if (next != ' ' && next != '\t' && next != '\n' && next != '\r' && next != '(' && start == 0) {
                offset--;
                return null;
            } else if (next == '(' && start == 0) {
                start = offset;
            } else if (next == ')') {
                columnList = originalSql.substring(start, offset - 1);
                break;
            }
        }
        return new ArrayList<>(Arrays.asList(columnList.split(",")));
    }


    public int getType() {
        return type;
    }
}
