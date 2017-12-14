package com.actiontech.dble.config.loader.xml;


import com.actiontech.dble.config.model.AlarmConfig;
import com.actiontech.dble.config.util.ConfigUtil;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.lang.reflect.InvocationTargetException;

/**
 * Created by szf on 2017/12/12.
 */
public class AlarmConfigLoader implements Loader<AlarmConfig, XMLServerLoader> {
    @Override
    public void load(Element root, XMLServerLoader xsl, boolean isLowerCaseTableNames) throws IllegalAccessException, InvocationTargetException {
        AlarmConfig alarmConfig = xsl.getAlarm();
        NodeList list = root.getElementsByTagName("alarm");

        if (list != null && list.getLength() > 0) {
            Node node = list.item(0);
            if (node instanceof Element) {
                Element e = (Element) node;
                alarmConfig.setUrl(ConfigUtil.loadElement(e, "url").getTextContent());
                alarmConfig.setPort(ConfigUtil.loadElement(e, "port").getTextContent());
                alarmConfig.setLevel(ConfigUtil.loadElement(e, "level").getTextContent());
                alarmConfig.setServerId(ConfigUtil.loadElement(e, "serverId").getTextContent());
                alarmConfig.setComponentId(ConfigUtil.loadElement(e, "componentId").getTextContent());
                alarmConfig.setComponentType(ConfigUtil.loadElement(e, "componentType").getTextContent());
            }
        }
    }
}
