package com.actiontech.dble.meta.table;

import com.alibaba.druid.sql.ast.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlKey;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlUnique;
import com.alibaba.druid.sql.dialect.mysql.ast.MysqlForeignKey;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlOrderingExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlPartitionByKey;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlCreateTableParser;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlSelectParser;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.Token;


/**
 * Created by szf on 2018/4/4.
 */
public class CreateTableParserImp extends MySqlCreateTableParser {


    public CreateTableParserImp(String sql) {
        super(new CreateSqlExprParser(sql));
    }


    public SQLCreateTableStatement parseCreateTable() {
        return parseCrateTable(true);
    }


    public MySqlCreateTableStatement parseCrateTable(boolean acceptCreate) {
        if (acceptCreate) {
            accept(Token.CREATE);
        }
        MySqlCreateTableStatement stmt = new MySqlCreateTableStatement();

        if (identifierEquals("TEMPORARY")) {
            lexer.nextToken();
            stmt.setType(SQLCreateTableStatement.Type.GLOBAL_TEMPORARY);
        }

        accept(Token.TABLE);

        if (lexer.token() == Token.IF || identifierEquals("IF")) {
            lexer.nextToken();
            accept(Token.NOT);
            accept(Token.EXISTS);

            stmt.setIfNotExiists(true);
        }

        stmt.setName(this.exprParser.name());

        if (lexer.token() == Token.LIKE) {
            lexer.nextToken();
            SQLName name = this.exprParser.name();
            stmt.setLike(name);
        }

        if (lexer.token() == (Token.LPAREN)) {
            lexer.nextToken();

            if (lexer.token() == Token.LIKE) {
                lexer.nextToken();
                SQLName name = this.exprParser.name();
                stmt.setLike(name);
            } else {
                parseCreateDefinition(stmt);
            }

            accept(Token.RPAREN);
        }


        parseTableOptions(stmt);


        if (lexer.token() == (Token.ON)) {
            throw new ParserException("TODO");
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

    private boolean parseTableOptionCharsetOrCollate(MySqlCreateTableStatement stmt) {
        if (identifierEquals("CHARACTER")) {
            lexer.nextToken();
            accept(Token.SET);
            if (lexer.token() == Token.EQ) {
                lexer.nextToken();
            }
            stmt.getTableOptions().put("CHARACTER SET", this.exprParser.expr());
            return true;
        }

        if (identifierEquals("CHARSET")) {
            lexer.nextToken();
            if (lexer.token() == Token.EQ) {
                lexer.nextToken();
            }
            stmt.getTableOptions().put("CHARSET", this.exprParser.expr());
            return true;
        }

        if (identifierEquals("COLLATE")) {
            lexer.nextToken();
            if (lexer.token() == Token.EQ) {
                lexer.nextToken();
            }
            stmt.getTableOptions().put("COLLATE", this.exprParser.expr());
            return true;
        }

        return false;
    }


    protected SQLTableConstraint parseConstraint() {
        SQLName name = null;
        boolean hasConstaint = false;
        if (lexer.token() == (Token.CONSTRAINT)) {
            hasConstaint = true;
            lexer.nextToken();
        }

        if (lexer.token() == Token.IDENTIFIER) {
            name = this.exprParser.name();
        }

        if (lexer.token() == (Token.KEY)) {
            lexer.nextToken();

            MySqlKey key = new MySqlKey();
            key.setHasConstaint(hasConstaint);


            if (lexer.token() == Token.IDENTIFIER || lexer.token() == Token.LITERAL_ALIAS) {
                SQLName indexName = this.exprParser.name();
                if (indexName != null) {
                    key.setIndexName(indexName);
                }
            }

            if (identifierEquals("USING")) {
                lexer.nextToken();
                key.setIndexType(lexer.stringVal());
                lexer.nextToken();
            }

            accept(Token.LPAREN);
            for (; ; ) {
                SQLExpr expr = this.exprParser.expr();
                if (lexer.token() == Token.ASC) {
                    lexer.nextToken();
                    expr = new MySqlOrderingExpr(expr, SQLOrderingSpecification.ASC);
                } else if (lexer.token() == Token.DESC) {
                    lexer.nextToken();
                    expr = new MySqlOrderingExpr(expr, SQLOrderingSpecification.DESC);
                }

                key.addColumn(expr);
                if (!(lexer.token() == (Token.COMMA))) {
                    break;
                } else {
                    lexer.nextToken();
                }
            }
            accept(Token.RPAREN);

            if (name != null) {
                key.setName(name);
            }

            if (identifierEquals("USING")) {
                lexer.nextToken();
                key.setIndexType(lexer.stringVal());
                lexer.nextToken();
            }

            if (lexer.token() == Token.COMMENT) {
                lexer.nextToken();
                lexer.nextToken();
            }

            return key;
        }

        if (lexer.token() == Token.PRIMARY) {
            MySqlPrimaryKey pk = this.getExprParser().parsePrimaryKey();
            pk.setName(name);
            pk.setHasConstaint(hasConstaint);
            return (SQLTableConstraint) pk;
        }

        if (lexer.token() == Token.UNIQUE) {
            MySqlUnique uk = this.getExprParser().parseUnique();
            uk.setName(name);
            uk.setHasConstaint(hasConstaint);
            return (SQLTableConstraint) uk;
        }

        if (lexer.token() == Token.FOREIGN) {
            MysqlForeignKey fk = this.getExprParser().parseForeignKey();
            fk.setName(name);
            fk.setHasConstraint(hasConstaint);
            return (SQLTableConstraint) fk;
        }

        throw new ParserException("TODO :" + lexer.token());
    }


    private void parseCreateDefinition(MySqlCreateTableStatement stmt) {
        for (; ; ) {
            if (lexer.token() == Token.IDENTIFIER && lexer.stringVal().equalsIgnoreCase("FULLTEXT")) {
                lexer.nextToken();
                stmt.getTableElementList().add(parseConstraint());
            } else if (lexer.token() == Token.IDENTIFIER ||
                    lexer.token() == Token.LITERAL_CHARS) {
                SQLColumnDefinition column = this.exprParser.parseColumn();
                stmt.getTableElementList().add(column);
            } else if (lexer.token() == Token.CONSTRAINT ||
                    lexer.token() == Token.PRIMARY ||
                    lexer.token() == Token.UNIQUE) {
                SQLTableConstraint constraint = this.parseConstraint();
                stmt.getTableElementList().add(constraint);
            } else if (lexer.token() == (Token.INDEX)) {
                lexer.nextToken();

                MySqlTableIndex idx = new MySqlTableIndex();

                if (lexer.token() == Token.IDENTIFIER) {
                    if (!"USING".equalsIgnoreCase(lexer.stringVal())) {
                        idx.setName(this.exprParser.name());
                    }
                }

                if (identifierEquals("USING")) {
                    lexer.nextToken();
                    idx.setIndexType(lexer.stringVal());
                    lexer.nextToken();
                }

                accept(Token.LPAREN);
                for (; ; ) {
                    idx.addColumn(this.exprParser.expr());
                    if (!(lexer.token() == (Token.COMMA))) {
                        break;
                    } else {
                        lexer.nextToken();
                    }
                }
                accept(Token.RPAREN);

                if (identifierEquals("USING")) {
                    lexer.nextToken();
                    idx.setIndexType(lexer.stringVal());
                    lexer.nextToken();
                }

                stmt.getTableElementList().add(idx);
            } else if (lexer.token() == (Token.KEY)) {
                stmt.getTableElementList().add(parseConstraint());
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
                SQLColumnDefinition column = this.exprParser.parseColumn();
                stmt.getTableElementList().add(column);
            }

            if (!(lexer.token() == (Token.COMMA))) {
                break;
            } else {
                lexer.nextToken();
            }
        }
    }


    private void parseTableOptions(MySqlCreateTableStatement stmt) {
        for (; ; ) {
            if (identifierEquals("ENGINE")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.getTableOptions().put("ENGINE", this.exprParser.expr());
            } else if (identifierEquals("AUTO_INCREMENT")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.getTableOptions().put("AUTO_INCREMENT", this.exprParser.expr());
            } else if (identifierEquals("AVG_ROW_LENGTH")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.getTableOptions().put("AVG_ROW_LENGTH", this.exprParser.expr());
            } else if (lexer.token() == Token.DEFAULT) {
                lexer.nextToken();
                parseTableOptionCharsetOrCollate(stmt);
            } else if (parseTableOptionCharsetOrCollate(stmt)) {
                continue;
            } else if (identifierEquals("CHECKSUM")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.getTableOptions().put("CHECKSUM", this.exprParser.expr());
            } else if (lexer.token() == Token.COMMENT) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.getTableOptions().put("COMMENT", this.exprParser.expr());
            } else if (identifierEquals("CONNECTION")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.getTableOptions().put("CONNECTION", this.exprParser.expr());
            } else if (identifierEquals("DATA")) {
                lexer.nextToken();
                acceptIdentifier("DIRECTORY");
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.getTableOptions().put("DATA DIRECTORY", this.exprParser.expr());
            } else if (identifierEquals("DELAY_KEY_WRITE")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.getTableOptions().put("DELAY_KEY_WRITE", this.exprParser.expr());
            } else if (identifierEquals("INDEX")) {
                lexer.nextToken();
                acceptIdentifier("DIRECTORY");
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.getTableOptions().put("INDEX DIRECTORY", this.exprParser.expr());
            } else if (identifierEquals("INSERT_METHOD")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.getTableOptions().put("INSERT_METHOD", this.exprParser.expr());
            } else if (identifierEquals("KEY_BLOCK_SIZE")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.getTableOptions().put("KEY_BLOCK_SIZE", this.exprParser.expr());
            } else if (identifierEquals("MAX_ROWS")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.getTableOptions().put("MAX_ROWS", this.exprParser.expr());
            } else if (identifierEquals("MIN_ROWS")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.getTableOptions().put("MIN_ROWS", this.exprParser.expr());
            } else if (identifierEquals("PACK_KEYS")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.getTableOptions().put("PACK_KEYS", this.exprParser.expr());
            } else if (identifierEquals("PASSWORD")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.getTableOptions().put("PASSWORD", this.exprParser.expr());
            } else if (identifierEquals("ROW_FORMAT")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }
                stmt.getTableOptions().put("ROW_FORMAT", this.exprParser.expr());
            } else if (identifierEquals("STATS_AUTO_RECALC")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }

                stmt.getTableOptions().put("STATS_AUTO_RECALC", this.exprParser.expr());
            } else if (identifierEquals("STATS_PERSISTENT")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }

                stmt.getTableOptions().put("STATS_PERSISTENT", this.exprParser.expr());
            } else if (identifierEquals("STATS_SAMPLE_PAGES")) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }

                stmt.getTableOptions().put("STATS_SAMPLE_PAGES", this.exprParser.expr());
            } else if (lexer.token() == Token.UNION) {
                lexer.nextToken();
                if (lexer.token() == Token.EQ) {
                    lexer.nextToken();
                }

                accept(Token.LPAREN);
                SQLTableSource tableSrc = this.createSQLSelectParser().parseTableSource();
                stmt.getTableOptions().put("UNION", tableSrc);
                accept(Token.RPAREN);
            } else if (lexer.token() == Token.TABLESPACE) {
                lexer.nextToken();

                MySqlCreateTableStatement.TableSpaceOption option = new MySqlCreateTableStatement.TableSpaceOption();
                option.setName(this.exprParser.name());

                if (identifierEquals("STORAGE")) {
                    lexer.nextToken();
                    option.setStorage(this.exprParser.name());
                }

                stmt.getTableOptions().put("TABLESPACE", option);
            } else if (identifierEquals("TABLEGROUP")) {
                lexer.nextToken();

                SQLName tableGroup = this.exprParser.name();
                stmt.setTableGroup(tableGroup);
            } else if (identifierEquals("TYPE")) {
                lexer.nextToken();
                accept(Token.EQ);
                stmt.getTableOptions().put("TYPE", this.exprParser.expr());
                lexer.nextToken();
            } else if (lexer.token() == Token.PARTITION) {
                lexer.nextToken();
                accept(Token.BY);

                SQLPartitionBy partitionClause = null;

                parsePartitionOptions(partitionClause);

                stmt.setPartitioning(partitionClause);

            } else {
                break;
            }
        }
    }


    private void parsePartitionOptions(SQLPartitionBy partitionClause) {

        boolean linera = false;
        if (identifierEquals("LINEAR")) {
            lexer.nextToken();
            linera = true;
        }

        if (lexer.token() == Token.KEY) {
            MySqlPartitionByKey clause = new MySqlPartitionByKey();
            lexer.nextToken();

            if (linera) {
                clause.setLinear(true);
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
        } else if (identifierEquals("HASH")) {
            lexer.nextToken();
            SQLPartitionByHash clause = new SQLPartitionByHash();

            if (linera) {
                clause.setLinear(true);
            }

            if (lexer.token() == Token.KEY) {
                lexer.nextToken();
                clause.setKey(true);
            }

            accept(Token.LPAREN);
            clause.setExpr(this.exprParser.expr());
            accept(Token.RPAREN);
            partitionClause = clause;

            partitionClauseRest(clause);

        } else if (identifierEquals("RANGE")) {
            SQLPartitionByRange clause = partitionByRange();
            partitionClause = clause;

            partitionClauseRest(clause);

        } else if (identifierEquals("LIST")) {
            lexer.nextToken();
            SQLPartitionByList clause = new SQLPartitionByList();

            if (lexer.token() == Token.LPAREN) {
                lexer.nextToken();
                clause.setExpr(this.exprParser.expr());
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
            throw new ParserException("TODO " + lexer.token() + " " + lexer.stringVal());
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
    }

}
