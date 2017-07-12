package io.mycat.route.util;

import io.mycat.util.ResourceUtil;

import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.Properties;

import static io.mycat.route.sequence.handler.IncrSequenceHandler.KEY_CUR_NAME;
import static io.mycat.route.sequence.handler.IncrSequenceHandler.KEY_MAX_NAME;
import static io.mycat.route.sequence.handler.IncrSequenceHandler.KEY_MIN_NAME;

/**
 * Property文件加载器
 *
 * @author Hash Zhang
 * @time 00:08:03 2016/5/3
 * @version 1.0
 */
public class PropertiesUtil {
    public static Properties loadProps(String propsFile){
        Properties props = new Properties();
        InputStream inp = ResourceUtil.getResourceAsStreamForCurrentThread(propsFile);

        if (inp == null) {
            throw new java.lang.RuntimeException("time sequnce properties not found " + propsFile);
        }
        try {
            props.load(inp);
        } catch (IOException e) {
            throw new java.lang.RuntimeException(e);
        }
        return props;
    }
    public static Properties loadProps(String propsFile, boolean isLowerCaseTableNames){
        Properties props = loadProps(propsFile);
        if(isLowerCaseTableNames){
            Properties newProps = new Properties();
            Enumeration<?> enu = props.propertyNames();
            while (enu.hasMoreElements()) {
                String key = (String) enu.nextElement();
                if (key.endsWith(KEY_MIN_NAME) || key.endsWith(KEY_MAX_NAME) || key.endsWith(KEY_CUR_NAME)) {
                    int index = key.lastIndexOf('.');
                    newProps.setProperty(key.substring(0,index).toLowerCase()+key.substring(index), props.getProperty(key));
                }else {
                    newProps.setProperty(key.toLowerCase(), props.getProperty(key));
                }
            }
            props.clear();
            return newProps;
        }else{
            return props;
        }
    }
}
