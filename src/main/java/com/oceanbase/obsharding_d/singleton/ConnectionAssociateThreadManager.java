/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.singleton;

import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.net.connection.AbstractConnection;
import com.oceanbase.obsharding_d.net.connection.BackendConnection;
import com.oceanbase.obsharding_d.net.connection.FrontendConnection;
import com.oceanbase.obsharding_d.net.service.AbstractService;
import com.oceanbase.obsharding_d.net.service.Service;
import com.oceanbase.obsharding_d.util.StringUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ConnectionAssociateThreadManager {
    private static final ConnectionAssociateThreadManager INSTANCE = new ConnectionAssociateThreadManager();

    private Set<AssociateVector> associate;
    private boolean enable = false;

    public ConnectionAssociateThreadManager() {
        if (SystemConfig.getInstance().getUsePerformanceMode() == 1)
            return;
        enable = (SystemConfig.getInstance().getEnableConnectionAssociateThread() == 1);
        if (enable) {
            associate = Sets.newConcurrentHashSet();
        }
    }

    public static ConnectionAssociateThreadManager getInstance() {
        return INSTANCE;
    }

    public boolean isEnable() {
        return enable;
    }

    public void put(Service service) {
        if (!enable || service == null) return;
        if (service instanceof AbstractService) {
            put(((AbstractService) service).getConnection());
        }
    }

    public void put(AbstractConnection conn) {
        if (!enable || associate == null) return;
        if (conn instanceof FrontendConnection || conn instanceof BackendConnection) {
            associate.add(buildVector(conn));
        }
    }

    public void remove(Service service) {
        if (!enable || service == null) return;
        if (service instanceof AbstractService) {
            remove(((AbstractService) service).getConnection());
        }
    }

    public void remove(AbstractConnection conn) {
        if (!enable || associate == null) return;
        if (conn instanceof FrontendConnection || conn instanceof BackendConnection) {
            associate.remove(buildVector(conn));
        }
    }

    private AssociateVector buildVector(AbstractConnection conn) {
        return new AssociateVector((conn instanceof FrontendConnection ? ConnectType.Frontend : ConnectType.Backend), conn.getId(), Thread.currentThread().getName());
    }

    public List<AssociateVector> getVector(ConnectType type0) {
        if (!enable || associate == null) return Lists.newArrayList();
        List<AssociateVector> list = associate.stream().filter(c -> c.getType() == type0).collect(Collectors.toList());
        return list;
    }

    public static class AssociateVector {
        private ConnectType type;
        private long connId;
        private String threadName;

        public AssociateVector(ConnectType type, long connId, String threadName) {
            this.type = type;
            this.connId = connId;
            this.threadName = threadName;
        }

        public ConnectType getType() {
            return type;
        }

        public long getConnId() {
            return connId;
        }

        public String getThreadName() {
            return threadName;
        }

        @Override
        public int hashCode() {
            int hash = this.type.hashCode();
            hash = hash * 31 + (int) this.connId;
            hash = hash * 31 + this.threadName.hashCode();
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            AssociateVector vector = (AssociateVector) obj;
            if (this.type == vector.getType() &&
                    this.connId == vector.getConnId() &&
                    StringUtil.equals(this.threadName, vector.getThreadName())) {
                return true;
            }
            return false;
        }


        @Override
        public String toString() {
            return type + "," + connId + "," + threadName;
        }
    }

    public enum ConnectType {
        Frontend, Backend
    }
}
