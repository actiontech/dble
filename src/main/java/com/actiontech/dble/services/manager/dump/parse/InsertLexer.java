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
