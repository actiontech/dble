package com.actiontech.dble.config.loader.ucoreprocess.KVtoXml;

import com.actiontech.dble.config.loader.ucoreprocess.UcorePathUtil;
import com.actiontech.dble.config.loader.ucoreprocess.listen.UOffLineListener;
import com.actiontech.dble.config.loader.ucoreprocess.listen.UcoreClearKeyListener;
import com.actiontech.dble.config.loader.ucoreprocess.listen.UcoreSingleKeyListener;
import com.actiontech.dble.config.loader.ucoreprocess.loader.*;
import com.actiontech.dble.config.loader.zkprocess.parse.XmlProcessBase;

import static com.actiontech.dble.config.loader.ucoreprocess.UcorePathUtil.SEPARATOR;

/**
 * Created by szf on 2018/1/24.
 */
public final class UcoreToXml {

    private static UcoreClearKeyListener listener = null;

    private static UcoreSingleKeyListener ddlListener = null;

    private static UcoreSingleKeyListener viewListener = null;

    private static UOffLineListener onlineListener = null;

    private UcoreToXml() {

    }

    public static void loadKVtoFile() {
        try {
            //create a new listener to the ucore config change
            listener = new UcoreClearKeyListener();
            XmlProcessBase xmlProcess = new XmlProcessBase();
            listener.init();
            //add all loader into listener map list
            new UXmlRuleLoader(xmlProcess, listener);
            new UXmlSchemaLoader(xmlProcess, listener);
            new UXmlServerLoader(xmlProcess, listener);
            new UXmlEhcachesLoader(xmlProcess, listener);
            new UPropertySequenceLoader(listener);
            xmlProcess.initJaxbClass();

            //add listener to watch the Prefix of the keys
            new UConfigStatusResponse(listener);
            new UBinlogPauseStatusResponse(listener);
            new UPauseDataNodeResponse(listener);


            ddlListener = new UcoreSingleKeyListener(UcorePathUtil.getDDLPath() + SEPARATOR, new UDdlChildResponse());

            viewListener = new UcoreSingleKeyListener(UcorePathUtil.getViewChangePath() + SEPARATOR, new UViewChildResponse());

            onlineListener = new UOffLineListener();

            listener.initForXml();
            Thread thread = new Thread(listener);
            thread.start();

            ddlListener.init();
            Thread thread2 = new Thread(ddlListener);
            thread2.start();

            viewListener.init();
            Thread thread3 = new Thread(viewListener);
            thread3.start();

            onlineListener.init();
            Thread thread4 = new Thread(onlineListener);
            thread4.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static UcoreClearKeyListener getListener() {
        return listener;
    }
}
