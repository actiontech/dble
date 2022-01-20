/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.dump.parse;

import com.alibaba.druid.sql.parser.Lexer;

class InsertLexer extends Lexer {

    InsertLexer(String input) {
        super(input);
    }

    public int getStartPos() {
        return startPos;
    }

    public int getPos() {
        return pos;
    }
}
