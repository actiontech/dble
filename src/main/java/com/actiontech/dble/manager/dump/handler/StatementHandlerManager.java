package com.actiontech.dble.manager.dump.handler;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateDatabaseStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLUseStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class StatementHandlerManager {

    public enum Type {
        INSERT, DATABASE, CREATE_TABLE, DROP, DEFAULT
    }

    private static Map<Type, StatementHandler> handlers = new ConcurrentHashMap<>(8);

    static {
        handlers.put(Type.DATABASE, new SchemaHandler());
        handlers.put(Type.INSERT, new InsertHandler());
        handlers.put(Type.CREATE_TABLE, new CreateTableHandler());
        handlers.put(Type.DROP, new DropHandler());
        handlers.put(Type.DEFAULT, new DefaultHandler());
    }

    private StatementHandlerManager() {
    }

    public static StatementHandler getHandler(SQLStatement statement) {
        if (statement instanceof SQLDropTableStatement) {
            return handlers.get(Type.DROP);
        } else if (statement instanceof SQLCreateDatabaseStatement || statement instanceof SQLUseStatement) {
            return handlers.get(Type.DATABASE);
        } else if (statement instanceof MySqlCreateTableStatement) {
            return handlers.get(Type.CREATE_TABLE);
        } else if (statement instanceof MySqlInsertStatement) {
            return handlers.get(Type.INSERT);
        }
        return handlers.get(Type.DEFAULT);
    }
}
