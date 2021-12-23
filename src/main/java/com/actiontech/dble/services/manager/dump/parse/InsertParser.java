package com.actiontech.dble.services.manager.dump.parse;

import com.actiontech.dble.route.parser.util.Pair;
import com.alibaba.druid.sql.parser.Token;

import java.util.ArrayList;
import java.util.List;

public class InsertParser {

    protected final InsertLexer lexer;

    public InsertParser(String sql) {
        lexer = new InsertLexer(sql);
        lexer.nextToken();
    }

    public int findInsert() {
        while (true) {
            if (lexer.token() == Token.SEMI) {
                lexer.nextToken();
                if (lexer.token() == Token.INSERT || lexer.token() == Token.REPLACE) {
                    return lexer.getStartPos();
                }
                lexer.nextToken();
            } else if (lexer.token() == Token.EOF) {
                return -1;
            } else {
                lexer.nextToken();
            }
        }
    }

    // return null means not start with insert/replace
    // QueryRang  null means insert not finish
    public InsertQueryPos parseStatement() {
        InsertQueryPos insertQueryPos = null;
        if (lexer.token() == Token.INSERT || lexer.token() == Token.REPLACE) {
            int start = lexer.getStartPos();
            insertQueryPos = new InsertQueryPos();

            if (lexer.token() == Token.REPLACE) {
                insertQueryPos.setReplace(true);
            }
            lexer.nextToken();
            if (lexer.stringVal().equalsIgnoreCase("IGNORE")) {
                insertQueryPos.setIgnore(true);
                lexer.nextToken();
            }
            if (lexer.token() == Token.INTO) {
                lexer.nextToken();
                String tableName = lexer.stringVal();
                insertQueryPos.setTableName(tableName);
                lexer.nextToken();
            }

            if (lexer.token() == Token.LPAREN) {
                lexer.nextToken();
                int colStart = lexer.getStartPos();
                this.parseColumnNameList(insertQueryPos);
                int colEnd = lexer.getStartPos();
                Pair<Integer, Integer> columnRange = new Pair<>(colStart, colEnd);
                insertQueryPos.setColumnRange(columnRange);
                this.accept(Token.RPAREN);
            }

            if (lexer.token() == Token.VALUES) {
                lexer.nextToken();

                while (true) {
                    if (lexer.token() == Token.LPAREN) {
                        lexer.nextToken();
                        int valueStart = lexer.getStartPos();
                        List<Pair<Integer, Integer>> valuerItemList = new ArrayList<>();
                        this.parseValueList(valuerItemList);
                        insertQueryPos.getValueItemsRange().add(valuerItemList);
                        int valueEnd = lexer.getStartPos();
                        Pair<Integer, Integer> valueRange = new Pair<>(valueStart, valueEnd);
                        insertQueryPos.getValuesRange().add(valueRange);
                        this.accept(Token.RPAREN);
                    }

                    if (lexer.token() != Token.COMMA) {
                        break;
                    }

                    lexer.nextToken();
                }
            }
            if (accept(Token.SEMI)) {
                int end = lexer.getStartPos();
                insertQueryPos.setQueryRange(new Pair<>(start, end));
            }
        }
        return insertQueryPos;
    }

    private boolean accept(Token token) {
        if (lexer.token() == token) {
            lexer.nextToken();
            return true;
        }
        return false;
    }

    private void parseColumnNameList(InsertQueryPos insertQueryPos) {
        int i = 0;
        while (true) {
            String colName = lexer.stringVal();
            insertQueryPos.getColNameIndexMap().put(colName, i);
            insertQueryPos.getColumns().add(colName);
            lexer.nextToken();
            if (lexer.token() != Token.COMMA) {
                return;
            }
            lexer.nextToken();
            i++;
        }
    }

    private void parseValueList(List<Pair<Integer, Integer>> valuerItemList) {
        while (true) {
            int valueItemStart = lexer.getStartPos();
            int valueItemEnd = lexer.getPos();
            Pair<Integer, Integer> valueRange = new Pair<>(valueItemStart, valueItemEnd);
            valuerItemList.add(valueRange);
            lexer.nextToken();
            if (lexer.token() != Token.COMMA) {
                return;
            }
            lexer.nextTokenValue();
        }
    }

}
