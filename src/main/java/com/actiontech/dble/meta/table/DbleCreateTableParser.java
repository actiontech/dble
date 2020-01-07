/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.meta.table;

import com.alibaba.druid.sql.ast.*;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlKey;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlPartitionByKey;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlCreateTableParser;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlSelectParser;
import com.alibaba.druid.sql.parser.Lexer;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.Token;
import com.alibaba.druid.util.FnvHash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by szf on 2018/12/4.
 * extends druid MySqlCreateTableParser
 * exempts code check because the source code is copy from druid
 */
public class DbleCreateTableParser extends MySqlCreateTableParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbleCreateTableParser.class);

    public DbleCreateTableParser(String sql) {
        super(sql);
    }

    public MySqlCreateTableStatement parseCreateTable(boolean acceptCreate) {
        MySqlCreateTableStatement stmt = new MySqlCreateTableStatement();
        if (acceptCreate) {
            if (lexer.hasComment() && lexer.isKeepComments()) {
                stmt.addBeforeComment(lexer.readAndResetComments());
            }
            accept(Token.CREATE);
        }

        if (lexer.identifierEquals("TEMPORARY")) {
            lexer.nextToken();
            stmt.setType(SQLCreateTableStatement.Type.GLOBAL_TEMPORARY);
        }

        accept(Token.TABLE);

        if (lexer.token() == Token.IF || lexer.identifierEquals("IF")) {
            lexer.nextToken();
            accept(Token.NOT);
            accept(Token.EXISTS);

            stmt.setIfNotExiists(true);
        }

        stmt.setName(this.exprParser.name());

        if (lexer.token() == Token.LIKE) {
            lexer.nextTokenValue();
            SQLName name = this.exprParser.name();
            stmt.setLike(name);
        }

        if (lexer.token() == (Token.LPAREN)) {
            lexer.nextToken();

            if (lexer.token() == Token.LIKE) {
                lexer.nextTokenValue();
                SQLName name = this.exprParser.name();
                stmt.setLike(name);
            } else if (lexer.token() == Token.SELECT) {
                SQLSelect query = new MySqlSelectParser(this.exprParser).select();
                stmt.setSelect(query);
            } else {
                for (; ; ) {
                    if (lexer.token() == Token.FULLTEXT) {
                        Lexer.SavePoint mark = lexer.mark();
                        lexer.nextToken();
                        if (lexer.token() == Token.KEY) {
                            MySqlKey fulltextKey = (MySqlKey) parseConstraint();
                            fulltextKey.setIndexType("FULLTEXT");
                            stmt.getTableElementList().add(fulltextKey);
                            if (lexer.token() == Token.RPAREN) {
                                break;
                            } else if (lexer.token() == Token.COMMA) {
                                lexer.nextToken();
                                continue;
                            }
                        } else if (lexer.token() == Token.INDEX) {
                            lexer.nextToken();
                            MySqlTableIndex idx = new MySqlTableIndex();
                            idx.setIndexType("FULLTEXT");
                            idx.setName(this.exprParser.name());

                            accept(Token.LPAREN);
                            for (; ; ) {
                                idx.addColumn(this.exprParser.parseSelectOrderByItem());
                                if (!(lexer.token() == (Token.COMMA))) {
                                    break;
                                } else {
                                    lexer.nextToken();
                                }
                            }
                            stmt.getTableElementList().add(idx);
                            accept(Token.RPAREN);
                            if (lexer.token() == Token.RPAREN) {
                                break;
                            } else if (lexer.token() == Token.COMMA) {
                                lexer.nextToken();
                                continue;
                            }
                        } else {
                            MySqlTableIndex idx = new MySqlTableIndex();
                            idx.setIndexType("FULLTEXT");
                            idx.setName(this.exprParser.name());

                            accept(Token.LPAREN);
                            for (; ; ) {
                                idx.addColumn(this.exprParser.parseSelectOrderByItem());
                                if (!(lexer.token() == (Token.COMMA))) {
                                    break;
                                } else {
                                    lexer.nextToken();
                                }
                            }
                            stmt.getTableElementList().add(idx);
                            accept(Token.RPAREN);
                            if (lexer.token() == Token.RPAREN) {
                                break;
                            } else if (lexer.token() == Token.COMMA) {
                                lexer.nextToken();
                                continue;
                            }
                        }
                    } else if (lexer.identifierEquals(FnvHash.Constants.SPATIAL)) {
                        Lexer.SavePoint mark = lexer.mark();
                        lexer.nextToken();
                        if (lexer.token() == Token.INDEX) {
                            lexer.nextToken();
                            MySqlTableIndex idx = new MySqlTableIndex();
                            idx.setIndexType("SPATIAL");

                            if (lexer.token() == Token.IDENTIFIER) {
                                if (!"USING".equalsIgnoreCase(lexer.stringVal())) {
                                    idx.setName(this.exprParser.name());
                                }
                            }

                            if (lexer.identifierEquals("USING")) {
                                lexer.nextToken();
                                idx.setIndexType(lexer.stringVal());
                                lexer.nextToken();
                            }

                            accept(Token.LPAREN);
                            for (; ; ) {
                                idx.addColumn(this.exprParser.parseSelectOrderByItem());
                                if (!(lexer.token() == (Token.COMMA))) {
                                    break;
                                } else {
                                    lexer.nextToken();
                                }
                            }
                            accept(Token.RPAREN);

                            if (lexer.identifierEquals("USING")) {
                                lexer.nextToken();
                                idx.setIndexType(lexer.stringVal());
                                lexer.nextToken();
                            }

                            stmt.getTableElementList().add(idx);

                            if (lexer.token() == Token.RPAREN) {
                                break;
                            } else if (lexer.token() == Token.COMMA) {
                                lexer.nextToken();
                                continue;
                            }
                        } else {
                            lexer.reset(mark);
                        }
                    }

                    SQLColumnDefinition column = null;
                    if (lexer.token() == Token.IDENTIFIER ||
                            lexer.token() == Token.LITERAL_CHARS) {
                        column = this.exprParser.parseColumn();
                        stmt.getTableElementList().add(column);

                        if (lexer.isKeepComments() && lexer.hasComment()) {
                            column.addAfterComment(lexer.readAndResetComments());
                        }
                    } else if (lexer.token() == Token.CONSTRAINT ||
                            lexer.token() == Token.PRIMARY ||
                            lexer.token() == Token.UNIQUE) {
                        SQLTableConstraint constraint = this.parseConstraint();
                        constraint.setParent(stmt);
                        stmt.getTableElementList().add(constraint);
                    } else if (lexer.token() == (Token.INDEX)) {
                        lexer.nextToken();

                        MySqlTableIndex idx = new MySqlTableIndex();

                        if (lexer.token() == Token.IDENTIFIER) {
                            if (!"USING".equalsIgnoreCase(lexer.stringVal())) {
                                idx.setName(this.exprParser.name());
                            }
                        }

                        if (lexer.identifierEquals("USING")) {
                            lexer.nextToken();
                            idx.setIndexType(lexer.stringVal());
                            lexer.nextToken();
                        }

                        accept(Token.LPAREN);
                        for (; ; ) {
                            idx.addColumn(this.exprParser.parseSelectOrderByItem());
                            if (!(lexer.token() == (Token.COMMA))) {
                                break;
                            } else {
                                lexer.nextToken();
                            }
                        }
                        accept(Token.RPAREN);

                        if (lexer.identifierEquals("USING")) {
                            lexer.nextToken();
                            idx.setIndexType(lexer.stringVal());
                            lexer.nextToken();
                        }

                        stmt.getTableElementList().add(idx);
                    } else if (lexer.token() == (Token.KEY)) {
                        Lexer.SavePoint savePoint = lexer.mark();
                        lexer.nextToken();

                        boolean isColumn = false;
                        if (lexer.identifierEquals(FnvHash.Constants.VARCHAR)) {
                            isColumn = true;
                        }
                        lexer.reset(savePoint);

                        if (isColumn) {
                            column = this.exprParser.parseColumn();
                            stmt.getTableElementList().add(column);
                        } else {
                            stmt.getTableElementList().add(parseConstraint());
                        }
                    } else if (lexer.token() == (Token.PRIMARY)) {
                        SQLTableConstraint pk = parseConstraint();
                        pk.setParent(stmt);
                        stmt.getTableElementList().add(pk);
                    } else if (lexer.token() == (Token.FOREIGN)) {
                        SQLForeignKeyConstraint fk = this.getExprParser().parseForeignKey();
                        fk.setParent(stmt);
                        stmt.getTableElementList().add(fk);
                    } else if (lexer.token() == Token.CHECK) {
                        SQLCheck check = this.exprParser.parseCheck();
                        stmt.getTableElementList().add(check);
                    } else {
                        column = this.exprParser.parseColumn();
                        stmt.getTableElementList().add(column);
                    }

                    if (lexer.token() != Token.COMMA) {
                        break;
                    } else {
                        lexer.nextToken();
                        if (lexer.isKeepComments() && lexer.hasComment() && column != null) {
                            column.addAfterComment(lexer.readAndResetComments());
                        }
                    }
                }
            }

            accept(Token.RPAREN);
        }

        for (; ; ) {
            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
            }

            if (lexer.identifierEquals("ENGINE")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }

                SQLExpr expr = null;
                if (lexer.token() == Token.MERGE) {
                    expr = new SQLIdentifierExpr(lexer.stringVal());
                    lexer.nextToken();
                } else {
                    expr = this.exprParser.expr();
                }
                stmt.getTableOptions().put("ENGINE", expr);
                continue;
            }

            if (lexer.identifierEquals("AUTO_INCREMENT")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                LOGGER.warn("parse sql drop AUTO_INCREMENT detail in " + stmt.getTableSource().getName());
                this.exprParser.expr();
                continue;
            }

            if (lexer.identifierEquals("AVG_ROW_LENGTH")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.getTableOptions().put("AVG_ROW_LENGTH", this.exprParser.expr());
                continue;
            }

            if (lexer.token() == Token.DEFAULT) {
                lexer.nextToken();
                parseTableOptionCharsetOrCollate(stmt);
                continue;
            }

            if (parseTableOptionCharsetOrCollate(stmt)) {
                continue;
            }

            if (lexer.identifierEquals("CHECKSUM")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.getTableOptions().put("CHECKSUM", this.exprParser.expr());
                continue;
            }

            if (lexer.token() == Token.COMMENT) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.setComment(this.exprParser.expr());
                continue;
            }

            if (lexer.identifierEquals("CONNECTION")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.getTableOptions().put("CONNECTION", this.exprParser.expr());
                continue;
            }

            if (lexer.identifierEquals("DATA")) {
                lexer.nextToken();
                acceptIdentifier("DIRECTORY");
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.getTableOptions().put("DATA DIRECTORY", this.exprParser.expr());
                continue;
            }

            if (lexer.identifierEquals("DELAY_KEY_WRITE")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.getTableOptions().put("DELAY_KEY_WRITE", this.exprParser.expr());
                continue;
            }

            if (lexer.identifierEquals("INDEX")) {
                lexer.nextToken();
                acceptIdentifier("DIRECTORY");
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.getTableOptions().put("INDEX DIRECTORY", this.exprParser.expr());
                continue;
            }

            if (lexer.identifierEquals("INSERT_METHOD")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.getTableOptions().put("INSERT_METHOD", this.exprParser.expr());
                continue;
            }

            if (lexer.identifierEquals("KEY_BLOCK_SIZE")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.getTableOptions().put("KEY_BLOCK_SIZE", this.exprParser.expr());
                continue;
            }

            if (lexer.identifierEquals("MAX_ROWS")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.getTableOptions().put("MAX_ROWS", this.exprParser.expr());
                continue;
            }

            if (lexer.identifierEquals("MIN_ROWS")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.getTableOptions().put("MIN_ROWS", this.exprParser.expr());
                continue;
            }

            if (lexer.identifierEquals("PACK_KEYS")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.getTableOptions().put("PACK_KEYS", this.exprParser.expr());
                continue;
            }

            if (lexer.identifierEquals("PASSWORD")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.getTableOptions().put("PASSWORD", this.exprParser.expr());
                continue;
            }

            if (lexer.identifierEquals("ROW_FORMAT")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.getTableOptions().put("ROW_FORMAT", this.exprParser.expr());
                continue;
            }

            if (lexer.identifierEquals("STATS_AUTO_RECALC")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }

                stmt.getTableOptions().put("STATS_AUTO_RECALC", this.exprParser.expr());
                continue;
            }

            if (lexer.identifierEquals("STATS_PERSISTENT")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }

                stmt.getTableOptions().put("STATS_PERSISTENT", this.exprParser.expr());
                continue;
            }

            if (lexer.identifierEquals("STATS_SAMPLE_PAGES")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }

                stmt.getTableOptions().put("STATS_SAMPLE_PAGES", this.exprParser.expr());
                continue;
            }

            if (lexer.token() == Token.UNION) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }

                accept(Token.LPAREN);
                SQLTableSource tableSrc = this.createSQLSelectParser().parseTableSource();
                stmt.getTableOptions().put("UNION", tableSrc);
                accept(Token.RPAREN);
                continue;
            }

            if (lexer.token() == Token.TABLESPACE) {
                lexer.nextToken();

                MySqlCreateTableStatement.TableSpaceOption option = new MySqlCreateTableStatement.TableSpaceOption();
                option.setName(this.exprParser.name());

                if (lexer.identifierEquals("STORAGE")) {
                    lexer.nextToken();
                    option.setStorage(this.exprParser.name());
                }

                stmt.getTableOptions().put("TABLESPACE", option);
                continue;
            }

            if (lexer.identifierEquals("TABLEGROUP")) {
                lexer.nextToken();

                SQLName tableGroup = this.exprParser.name();
                stmt.setTableGroup(tableGroup);
                continue;
            }

            if (lexer.identifierEquals("TYPE")) {
                lexer.nextToken();
                accept(Token.EQ);
                stmt.getTableOptions().put("TYPE", this.exprParser.expr());
                continue;
            }

            if (lexer.identifierEquals("ENCRYPTION")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.getTableOptions().put("ENCRYPTION", this.exprParser.expr());
                continue;
            }

            if (lexer.identifierEquals(FnvHash.Constants.COMPRESSION)) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.getTableOptions().put("COMPRESSION", this.exprParser.expr());
                continue;
            }

            if (lexer.token() == Token.PARTITION) {
                SQLPartitionBy partitionClause = parsePartitionBy();
                stmt.setPartitioning(partitionClause);

                continue;
            }

            if (lexer.identifierEquals(FnvHash.Constants.DBPARTITION)) {
                SQLPartitionBy partitionClause = parsePartitionBy();
                stmt.setDbPartitionBy(partitionClause);
                continue;
            }

            if (lexer.identifierEquals(FnvHash.Constants.TBPARTITION)) {
                SQLPartitionBy partitionClause = parsePartitionBy();
                stmt.setTablePartitionBy(partitionClause);
                continue;
            }

            if (lexer.identifierEquals(FnvHash.Constants.TBPARTITIONS)) {
                lexer.nextToken();
                SQLExpr tbpartitions = this.exprParser.expr();
                stmt.setTbpartitions(tbpartitions);
                continue;
            }

            break;
        }

        if (lexer.token() == (Token.ON)) {
            throw new ParserException("TODO. " + lexer.info());
        }

        if (lexer.token() == (Token.AS)) {
            lexer.nextToken();
        }

        if (lexer.token() == (Token.SELECT)) {
            SQLSelect query = new MySqlSelectParser(this.exprParser).select();
            stmt.setSelect(query);
        }

        while (lexer.token() == (Token.HINT)) {
            this.exprParser.parseHints(stmt.getOptionHints());
        }
        return stmt;
    }

    private SQLPartitionBy parsePartitionBy() {
        lexer.nextToken();
        accept(Token.BY);

        SQLPartitionBy partitionClause;

        boolean linera = false;
        if (lexer.identifierEquals("LINEAR")) {
            lexer.nextToken();
            linera = true;
        }

        if (lexer.token() == Token.KEY) {
            MySqlPartitionByKey clause = new MySqlPartitionByKey();
            lexer.nextToken();

            if (linera) {
                clause.setLinear(true);
            }

            if (lexer.identifierEquals(FnvHash.Constants.ALGORITHM)) {
                lexer.nextToken();
                accept(Token.EQ);
                clause.setAlgorithm(lexer.integerValue().shortValue());
                lexer.nextToken();
            }

            accept(Token.LPAREN);
            if (lexer.token() != Token.RPAREN) {
                for (; ; ) {
                    clause.addColumn(this.exprParser.name());
                    if (lexer.token() == Token.COMMA) {
                        lexer.nextToken();
                        continue;
                    }
                    break;
                }
            }
            accept(Token.RPAREN);

            partitionClause = clause;

            partitionClauseRest(clause);
        } else if (lexer.identifierEquals("HASH") || lexer.identifierEquals("UNI_HASH")) {
            SQLPartitionByHash clause = new SQLPartitionByHash();

            if (lexer.identifierEquals("UNI_HASH")) {
                clause.setUnique(true);
            }

            lexer.nextToken();

            if (linera) {
                clause.setLinear(true);
            }

            if (lexer.token() == Token.KEY) {
                lexer.nextToken();
                clause.setKey(true);
            }

            accept(Token.LPAREN);
            this.exprParser.exprList(clause.getColumns(), clause);
            accept(Token.RPAREN);
            partitionClause = clause;

            partitionClauseRest(clause);

        } else if (lexer.identifierEquals("RANGE")) {
            SQLPartitionByRange clause = partitionByRange();
            partitionClause = clause;

            partitionClauseRest(clause);

        } else if (lexer.identifierEquals("LIST")) {
            lexer.nextToken();
            SQLPartitionByList clause = new SQLPartitionByList();

            if (lexer.token() == Token.LPAREN) {
                lexer.nextToken();
                clause.addColumn(this.exprParser.expr());
                accept(Token.RPAREN);
            } else {
                acceptIdentifier("COLUMNS");
                accept(Token.LPAREN);
                for (; ; ) {
                    clause.addColumn(this.exprParser.name());
                    if (lexer.token() == Token.COMMA) {
                        lexer.nextToken();
                        continue;
                    }
                    break;
                }
                accept(Token.RPAREN);
            }
            partitionClause = clause;

            partitionClauseRest(clause);
        } else {
            throw new ParserException("TODO. " + lexer.info());
        }

        if (lexer.token() == Token.LPAREN) {
            lexer.nextToken();
            for (; ; ) {
                SQLPartition partitionDef = this.getExprParser().parsePartition();

                partitionClause.addPartition(partitionDef);

                if (lexer.token() == Token.COMMA) {
                    lexer.nextToken();
                    continue;
                } else {
                    break;
                }
            }
            accept(Token.RPAREN);
        }
        return partitionClause;
    }

    private boolean parseTableOptionCharsetOrCollate(MySqlCreateTableStatement stmt) {
        if (lexer.identifierEquals("CHARACTER")) {
            lexer.nextToken();
            accept(Token.SET);
            if (lexer.token() == Token.EQ) {
                lexer.nextToken();
            }
            stmt.getTableOptions().put("CHARACTER SET", this.exprParser.expr());
            return true;
        }

        if (lexer.identifierEquals("CHARSET")) {
            lexer.nextToken();
            if (lexer.token() == Token.EQ) {
                lexer.nextToken();
            }
            stmt.getTableOptions().put("CHARSET", this.exprParser.expr());
            return true;
        }

        if (lexer.identifierEquals("COLLATE")) {
            lexer.nextToken();
            if (lexer.token() == Token.EQ) {
                lexer.nextToken();
            }
            stmt.getTableOptions().put("COLLATE", this.exprParser.expr());
            return true;
        }

        return false;
    }

}
