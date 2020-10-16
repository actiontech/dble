package com.actiontech.dble.net.service;

import com.actiontech.dble.backend.mysql.proto.handler.Impl.MySQLProtoHandlerImpl;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.services.mysqlauthenticate.PluginName;

import java.io.IOException;

import static com.actiontech.dble.services.mysqlauthenticate.PluginName.caching_sha2_password;
import static com.actiontech.dble.services.mysqlauthenticate.PluginName.mysql_native_password;

/**
 * Created by collapsar on 2020/10/15.
 */
public abstract class AuthService extends AbstractService {

    public static final PluginName[] MYSQL_DEFAULT_PLUGIN = {mysql_native_password, mysql_native_password, mysql_native_password, caching_sha2_password};

    protected volatile byte[] seed;
    protected volatile boolean needAuthSwitched;
    protected volatile boolean authSwitchMore;
    protected volatile PluginName pluginName;

    public AuthService(AbstractConnection connection) {
        super(connection);
        this.proto = new MySQLProtoHandlerImpl();
    }

    protected PluginName getDefaultPluginName() {
        String majorMySQLVersion = SystemConfig.getInstance().getFakeMySQLVersion();
        if (majorMySQLVersion != null) {
            String[] versions = majorMySQLVersion.split("\\.");
            if (versions.length == 3) {
                majorMySQLVersion = versions[0] + "." + versions[1];
                for (int i = 0; i < SystemConfig.MYSQL_VERSIONS.length; i++) {
                    // version is x.y.z ,just compare the x.y
                    if (majorMySQLVersion.equals(SystemConfig.MYSQL_VERSIONS[i])) {
                        return MYSQL_DEFAULT_PLUGIN[i];
                    }
                }
            }
        } else {
            return mysql_native_password;
        }
        return null;
    }

    public void register() throws IOException {
    }

    public void onConnectFailed(Throwable e) {
    }
}
