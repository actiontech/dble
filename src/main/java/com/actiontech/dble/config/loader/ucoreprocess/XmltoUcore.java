/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.ucoreprocess;

import com.actiontech.dble.cluster.ClusterController;
import com.actiontech.dble.config.loader.ucoreprocess.listen.UcoreClearKeyListener;
import com.actiontech.dble.config.loader.ucoreprocess.loader.*;
import com.actiontech.dble.config.loader.zkprocess.parse.XmlProcessBase;


/**
 * Created by szf on 2018/1/29.
 */
public final class XmltoUcore {


    private XmltoUcore() {

    }

    public static void main(String[] args) throws Exception {
        ClusterController.initFromShellUcore();
        initFileToUcore();
        System.out.println("XmltoZkMain Finished");
    }

    public static void initFileToUcore() throws Exception {
        UcoreClearKeyListener ucoreListen = new UcoreClearKeyListener();

        XmlProcessBase xmlProcess = new XmlProcessBase();

        new UXmlRuleLoader(xmlProcess, ucoreListen);

        new UXmlServerLoader(xmlProcess, ucoreListen);

        new UXmlSchemaLoader(xmlProcess, ucoreListen);

        new UXmlEhcachesLoader(xmlProcess, ucoreListen);

        new UCacheserviceResponse(ucoreListen);

        new UPropertySequenceLoader(ucoreListen);

        xmlProcess.initJaxbClass();
        ucoreListen.initAllNode();
    }

}
