/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.backend.mysql.nio.handler;

/**
 * @author mycat
 */
public interface ExecutableHandler {

    /**
     * execute the handler
     */
    void execute() throws Exception;


    /**
     * execute the handler
     */
    void clearAfterFailExecute();


    void writeRemainBuffer();
}
