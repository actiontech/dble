package com.actiontech.dble.manager.handler.dump.type.handler;

import com.actiontech.dble.manager.handler.dump.type.DumpContent;

import java.sql.SQLSyntaxErrorException;

public interface DumpHandler {

    void handle(DumpContent content) throws SQLSyntaxErrorException;

}
