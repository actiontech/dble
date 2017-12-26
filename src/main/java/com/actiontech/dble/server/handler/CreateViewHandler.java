package com.actiontech.dble.server.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.meta.ViewMeta;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.server.ServerConnection;

/**
 * Created by szf on 2017/10/9.
 */
public final class CreateViewHandler {

    private CreateViewHandler() {
    }

    public static void handle(String stmt, ServerConnection c, boolean isReplace) {
        //create a new object of the view
        ViewMeta vm = new ViewMeta(stmt, c.getSchema());
        ErrorPacket error = vm.initAndSet(isReplace);
        if (error != null) {
            //if any error occurs when parse sql into view object
            c.writeErrMessage(error.getErrNo(), new String(error.getMessage()));
            return;
        }

        //or just save the create sql into file
        saveCreateSqlToReposoitory(stmt, vm.getViewName(), c.getSchema());

        //if the create success with no error send back OK
        c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
    }

    /**
     * save the view create sql into local file
     *
     * @param stmt
     * @param name
     * @param schema
     */
    public static void saveCreateSqlToReposoitory(String stmt, String name, String schema) {
        DbleServer.getInstance().getTmManager().getRepository().put(schema, name, stmt);
    }


}
