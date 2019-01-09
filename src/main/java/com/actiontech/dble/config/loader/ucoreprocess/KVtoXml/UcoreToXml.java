/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.ucoreprocess.KVtoXml;

import com.actiontech.dble.config.loader.ucoreprocess.UcorePathUtil;
import com.actiontech.dble.config.loader.ucoreprocess.listen.UOffLineListener;
import com.actiontech.dble.config.loader.ucoreprocess.listen.UcoreClearKeyListener;
import com.actiontech.dble.config.loader.ucoreprocess.listen.UcoreNodesListener;
import com.actiontech.dble.config.loader.ucoreprocess.listen.UcoreSingleKeyListener;
import com.actiontech.dble.config.loader.ucoreprocess.loader.*;
import com.actiontech.dble.config.loader.zkprocess.parse.XmlProcessBase;

import java.util.Map;

import static com.actiontech.dble.config.loader.ucoreprocess.UcorePathUtil.SEPARATOR;

/**
 * Created by szf on 2018/1/24.
 */
public final class UcoreToXml {

    private static UcoreClearKeyListener listener = null;

    private static UcoreSingleKeyListener ddlListener = null;

    private static UcoreSingleKeyListener viewListener = null;

    private static UOffLineListener onlineListener = null;

    private static UcoreNodesListener ucoreNodesListener = null;

    private UcoreToXml() {

    }

    public static void loadKVtoFile() {
        try {
            //create a new listener to the ucore config change
            listener = new UcoreClearKeyListener();
            XmlProcessBase xmlProcess = new XmlProcessBase();
            //add all loader into listener map list
            new UXmlRuleLoader(xmlProcess, listener);
            new UXmlSchemaLoader(xmlProcess, listener);
            new UXmlServerLoader(xmlProcess, listener);
            new UXmlEhcachesLoader(xmlProcess, listener);
            new UCacheserviceResponse(listener);
            new UPropertySequenceLoader(listener);
            xmlProcess.initJaxbClass();

            //add listener to watch the Prefix of the keys
            new UConfigStatusResponse(listener);
            new UBinlogPauseStatusResponse(listener);
            new UPauseDataNodeResponse(listener);


            ddlListener = new UcoreSingleKeyListener(UcorePathUtil.getDDLPath() + SEPARATOR, new UDdlChildResponse());

            viewListener = new UcoreSingleKeyListener(UcorePathUtil.getViewChangePath() + SEPARATOR, new UViewChildResponse());

            onlineListener = new UOffLineListener();

            ucoreNodesListener = new UcoreNodesListener();

            listener.initForXml();
            Thread thread = new Thread(listener);
            thread.setName("UCORE_KEY_LISTENER");
            thread.start();

            Thread thread2 = new Thread(ddlListener);
            thread2.setName("DDL_UCORE_LISTENER");
            thread2.start();

            Thread thread3 = new Thread(viewListener);
            thread3.setName("VIEW_UCORE_LISTENER");
            thread3.start();

            Thread thread4 = new Thread(onlineListener);
            thread4.setName("ONLINE_UCORE_LISTENER");
            thread4.start();

            Thread thread5 = new Thread(ucoreNodesListener);
            thread5.setName("NODES_UCORE_LISTENER");
            thread5.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static UcoreClearKeyListener getListener() {
        return listener;
    }

    public static Map<String, String> getOnlineMap() {
        return onlineListener.copyOnlineMap();
    }
}
