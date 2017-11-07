package com.actiontech.dble.meta;

import com.actiontech.dble.DbleServer;

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

    public void parseCreateView(ViewMeta viewMeta) {
        String viewName = getViewName();
        if(DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()){
            viewName = viewName.toLowerCase();
        }
        //get the name of view
        viewMeta.setViewName(viewName);
        //get the list of column name
        viewMeta.setViewColumnMeta(getViewColumn(DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()));
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
            }

        }
        return originalSql.substring(offset, originalSql.length());
    }


    public List<String> getViewColumn(boolean isLowerCaseTableNames) {
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
        if(isLowerCaseTableNames){
            columnList = columnList.toLowerCase();
        }
        return new ArrayList<String>(Arrays.asList(columnList.split(",")));
    }


    public int getType() {
        return type;
    }
}
