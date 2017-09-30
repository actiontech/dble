package com.actiontech.dble.meta;

import java.util.*;

/**
 * Created by szf on 2017/10/9.
 */
public class ViewMetaParser {

    private int offset = -1;
    private String originalSql;

    public ViewMetaParser(String originalSql) {
        this.originalSql = originalSql;
    }

    public void parseCreateView(ViewMeta viewMeta) {
        //get the name of view
        viewMeta.setViewName(getViewName());
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
        return new ArrayList<String>(Arrays.asList(columnList.split(",")));
    }
}
