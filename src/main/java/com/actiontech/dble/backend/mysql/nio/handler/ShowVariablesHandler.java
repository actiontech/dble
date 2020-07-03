/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler;

import com.actiontech.dble.net.mysql.RowDataPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.route.RouteResultset;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.util.StringUtil;

import java.util.Map;

public class ShowVariablesHandler extends SingleNodeHandler {
    private Map<String, String> shadowVars;

    public ShowVariablesHandler(RouteResultset rrs, NonBlockingSession session) {
        super(rrs, session);
        shadowVars = session.getShardingService().getSysVariables();
    }

    @Override
    public boolean rowResponse(byte[] row, RowDataPacket rowPacket, boolean isLeft, AbstractService service) {
        String charset = session.getShardingService().getCharset().getResults();
        RowDataPacket rowDataPacket = new RowDataPacket(2);
        /* Fixme: the accurate statistics of netOutBytes.
         *
         * We read net packet, but don't do Stat here. So the statistical magnitude -- netOutBytes is not exact.
         * Of course, we can do that.
         * But it is tiresome: re-implement the function of method rowResponse that have been implemented in super class.
         */
        rowDataPacket.read(row);
        String varName = StringUtil.decode(rowDataPacket.fieldValues.get(0), charset);
        if (shadowVars.containsKey(varName)) {
            rowDataPacket.setValue(1, StringUtil.encode(shadowVars.get(varName), charset));
            super.rowResponse(rowDataPacket.toBytes(), rowPacket, isLeft, service);
        } else {
            super.rowResponse(row, rowPacket, isLeft, service);
        }
        return false;
    }
}
