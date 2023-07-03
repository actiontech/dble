package com.actiontech.dble.cluster.zkprocess.entity.user;

import javax.xml.bind.annotation.*;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "hybridTAUser")
@XmlRootElement
public class HybridTAUser extends ShardingUser {

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("HybridTAUser{").append(super.toString());
        sb.append(", schemas=").append(schemas);
        sb.append(", tenant=").append(tenant);
        sb.append(", readOnly=").append(readOnly);
        sb.append(", blacklist=").append(blacklist);

        if (privileges != null) {
            sb.append(", privileges=").append(privileges);
        }
        sb.append('}');
        return sb.toString();
    }
}
