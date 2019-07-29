package com.actiontech.dble.alarm;

import com.actiontech.dble.util.ResourceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by szf on 2019/5/17.
 */
public final class AlertGeneralConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(AlertGeneralConfig.class);
    private static final AlertGeneralConfig INSTANCE = new AlertGeneralConfig();
    private static final String CONFIG_ALERT = "alert";
    private static final String CONFIG_FILE_NAME = "/dble_alert.properties";
    public static final Alert DEFAULT_ALERT = new NoAlert();
    private Properties properties;

    private AlertGeneralConfig() {

    }


    public void initAlertConfig() {
        //read the config from alert.properties
        properties = readConfigFile();
    }

    public Alert customizedAlert() {
        if (properties != null && properties.getProperty(CONFIG_ALERT) != null) {
            LOGGER.info("Use customized Alert to send the Alert Message" + properties.getProperty(CONFIG_ALERT));
            try {
                Class<?> clz = Class.forName(properties.getProperty(CONFIG_ALERT));
                if (!Alert.class.isAssignableFrom(clz)) {
                    throw new IllegalArgumentException("No Alert AS " + properties.getProperty(CONFIG_ALERT));
                }
                Alert alertInstance = (Alert) clz.newInstance();
                alertInstance.alertConfigCheck();
                return alertInstance;
            } catch (Exception e) {
                LOGGER.info("User customized Alert error,dble will use default Alert", e);
            }
        } else {
            LOGGER.info("No alert config in conf dir ,start with dble self alert");
        }
        return DEFAULT_ALERT;
    }

    public Properties getProperties() {
        return properties;
    }

    public static AlertGeneralConfig getInstance() {
        return INSTANCE;
    }

    private Properties readConfigFile() {
        Properties pros = new Properties();

        try (InputStream configIS = ResourceUtil.getResourceAsStream(CONFIG_FILE_NAME)) {
            if (configIS == null) {
                return pros;
            }
            pros.load(configIS);
        } catch (IOException e) {
            LOGGER.info("read" + e.getMessage());
        }
        return pros;
    }

}
