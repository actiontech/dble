package com.actiontech.dble.meta.table;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLOrderingSpecification;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlUnique;
import com.alibaba.druid.sql.dialect.mysql.ast.MysqlForeignKey;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlOrderingExpr;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlExprParser;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.Token;

/**
 * Created by szf on 2018/4/4.
 */
public class CreateSqlExprParser extends MySqlExprParser {

    public CreateSqlExprParser(String sql) {
        super(sql);
    }


    @Override
    public MySqlPrimaryKey parsePrimaryKey() {
        accept(Token.PRIMARY);
        accept(Token.KEY);

        MySqlPrimaryKey primaryKey = new MySqlPrimaryKey();

        if (identifierEquals("USING")) {
            lexer.nextToken();
            primaryKey.setIndexType(lexer.stringVal());
            lexer.nextToken();
        }

        accept(Token.LPAREN);
        for (; ; ) {
            primaryKey.addColumn(this.expr());
            if (!(lexer.token() == (Token.COMMA))) {
                break;
            } else {
                lexer.nextToken();
            }
        }
        accept(Token.RPAREN);

        if (lexer.token() == Token.COMMENT) {
            lexer.nextToken();
            lexer.nextToken();
        }

        return primaryKey;
    }

    public MySqlUnique parseUnique() {
        accept(Token.UNIQUE);

        if (lexer.token() == Token.KEY) {
            lexer.nextToken();
        }

        if (lexer.token() == Token.INDEX) {
            lexer.nextToken();
        }

        MySqlUnique unique = new MySqlUnique();

        if (lexer.token() != Token.LPAREN) {
            SQLName indexName = name();
            unique.setIndexName(indexName);
        }

        if (identifierEquals("USING")) {
            lexer.nextToken();
            unique.setIndexType(lexer.stringVal());
            lexer.nextToken();
        }

        accept(Token.LPAREN);
        for (; ; ) {
            SQLExpr column = this.expr();
            if (lexer.token() == Token.ASC) {
                column = new MySqlOrderingExpr(column, SQLOrderingSpecification.ASC);
                lexer.nextToken();
            } else if (lexer.token() == Token.DESC) {
                column = new MySqlOrderingExpr(column, SQLOrderingSpecification.DESC);
                lexer.nextToken();
            }
            unique.addColumn(column);
            if (!(lexer.token() == (Token.COMMA))) {
                break;
            } else {
                lexer.nextToken();
            }
        }
        accept(Token.RPAREN);

        if (identifierEquals("USING")) {
            lexer.nextToken();
            unique.setIndexType(lexer.stringVal());
            lexer.nextToken();
        }

        if (lexer.token() == Token.COMMENT) {
            lexer.nextToken();
            lexer.nextToken();
        }

        return unique;
    }


    public MysqlForeignKey parseForeignKey() {
        accept(Token.FOREIGN);
        accept(Token.KEY);

        MysqlForeignKey fk = new MysqlForeignKey();

        if (lexer.token() != Token.LPAREN) {
            SQLName indexName = name();
            fk.setIndexName(indexName);
        }

        accept(Token.LPAREN);
        this.names(fk.getReferencingColumns());
        accept(Token.RPAREN);

        accept(Token.REFERENCES);

        fk.setReferencedTableName(this.name());

        accept(Token.LPAREN);
        this.names(fk.getReferencedColumns());
        accept(Token.RPAREN);

        if (identifierEquals("MATCH")) {
            if (identifierEquals("FULL")) {
                fk.setReferenceMatch(MysqlForeignKey.Match.FULL);
            } else if (identifierEquals("PARTIAL")) {
                fk.setReferenceMatch(MysqlForeignKey.Match.PARTIAL);
            } else if (identifierEquals("SIMPLE")) {
                fk.setReferenceMatch(MysqlForeignKey.Match.SIMPLE);
            }
        }

        while (lexer.token() == Token.ON) {
            lexer.nextToken();

            if (lexer.token() == Token.DELETE) {
                lexer.nextToken();

                MysqlForeignKey.Option option = parseReferenceOption();
                fk.setOnDelete(option);
            } else if (lexer.token() == Token.UPDATE) {
                lexer.nextToken();

                MysqlForeignKey.Option option = parseReferenceOption();
                fk.setOnUpdate(option);
            } else {
                throw new ParserException("syntax error, expect DELETE or UPDATE, actual " + lexer.token() + " " +
                        lexer.stringVal());
            }
        }

        if (lexer.token() == Token.COMMENT) {
            lexer.nextToken();
            lexer.nextToken();
        }

        return fk;
    }
}
