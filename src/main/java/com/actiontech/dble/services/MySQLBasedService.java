package com.actiontech.dble.services;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.mysql.CharsetNames;
import com.actiontech.dble.net.mysql.ErrorPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.net.service.ServiceTask;
import com.actiontech.dble.util.CompressUtil;
import com.actiontech.dble.util.StringUtil;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by szf on 2020/6/28.
 */
public abstract class MySQLBasedService extends AbstractService {

    protected UserConfig userConfig;

    protected volatile Map<String, String> usrVariables = new LinkedHashMap<>();
    protected volatile Map<String, String> sysVariables = new LinkedHashMap<>();

    public MySQLBasedService(AbstractConnection connection) {
        super(connection);
    }


    protected void taskToPriorityQueue(ServiceTask task) {
        DbleServer.getInstance().getFrontPriorityQueue().offer(task);
        DbleServer.getInstance().getFrontHandlerQueue().offer(new ServiceTask(null, null));
    }

    protected void taskToTotalQueue(ServiceTask task) {
        DbleServer.getInstance().getFrontHandlerQueue().offer(task);
    }


    @Override
    public void handleData(ServiceTask task) {
        ServiceTask executeTask = null;
        if (connection.isClosed()) {
            return;
        }

        synchronized (this) {
            if (currentTask == null) {
                executeTask = taskQueue.poll();
                if (executeTask != null) {
                    currentTask = executeTask;
                }
            }
            if (currentTask != task) {
                taskToPriorityQueue(task);
            }
        }

        if (executeTask != null) {
            byte[] data = executeTask.getOrgData();
            if (data != null && !executeTask.isReuse()) {
                this.setPacketId(data[3]);
            }
            if (isSupportCompress()) {
                List<byte[]> packs = CompressUtil.decompressMysqlPacket(data, new ConcurrentLinkedQueue<>());
                for (byte[] pack : packs) {
                    if (pack.length != 0) {
                        handleInnerData(pack);
                    }
                }
            } else {
                this.handleInnerData(data);
                synchronized (this) {
                    currentTask = null;
                }
            }
        }
    }

    protected abstract void handleInnerData(byte[] data);

    public UserConfig getUserConfig() {
        return userConfig;
    }

    public CharsetNames getCharset() {
        return connection.getCharsetName();
    }

    public void writeErrMessage(String code, String msg, int vendorCode) {
        writeErrMessage((byte) this.nextPacketId(), vendorCode, code, msg);
    }

    public void writeErrMessage(int vendorCode, String msg) {
        writeErrMessage((byte) this.nextPacketId(), vendorCode, msg);
    }

    public void writeErrMessage(byte id, int vendorCode, String msg) {
        writeErrMessage(id, vendorCode, "HY000", msg);
    }


    protected void writeErrMessage(byte id, int vendorCode, String sqlState, String msg) {
        ErrorPacket err = new ErrorPacket();
        err.setPacketId(id);
        err.setErrNo(vendorCode);
        err.setSqlState(StringUtil.encode(sqlState, connection.getCharsetName().getResults()));
        err.setMessage(StringUtil.encode(msg, connection.getCharsetName().getResults()));
        err.write(connection);
    }

    public String getStringOfSysVariables() {
        StringBuilder sbSysVariables = new StringBuilder();
        int cnt = 0;
        if (sysVariables != null) {
            for (Map.Entry sysVariable : sysVariables.entrySet()) {
                if (cnt > 0) {
                    sbSysVariables.append(",");
                }
                sbSysVariables.append(sysVariable.getKey());
                sbSysVariables.append("=");
                sbSysVariables.append(sysVariable.getValue());
                cnt++;
            }
        }
        return sbSysVariables.toString();
    }

    public String getStringOfUsrVariables() {
        StringBuilder sbUsrVariables = new StringBuilder();
        int cnt = 0;
        if (usrVariables != null) {
            for (Map.Entry usrVariable : usrVariables.entrySet()) {
                if (cnt > 0) {
                    sbUsrVariables.append(",");
                }
                sbUsrVariables.append(usrVariable.getKey());
                sbUsrVariables.append("=");
                sbUsrVariables.append(usrVariable.getValue());
                cnt++;
            }
        }
        return sbUsrVariables.toString();
    }


}
