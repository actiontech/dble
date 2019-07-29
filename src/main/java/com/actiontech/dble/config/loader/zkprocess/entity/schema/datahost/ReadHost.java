/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.entity.schema.datahost;

import javax.xml.bind.annotation.*;
import java.util.List;

/**
 * <readHost host="" url="" password="" user=""></readHost>
 * <p>
 * <p>
 * author:liujun
 * Created:2016/9/15
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "readHost")
public class ReadHost extends WriteHost {
    @XmlTransient
    @Override
    public List<ReadHost> getReadHost() {
        return super.getReadHost();
    }

    @Override
    public String toString() {
        return "ReadHost [host=" +
                host +
                ", url=" +
                url +
                ", password=" +
                password +
                ", user=" +
                user +
                ", disabled=" +
                disabled +
                ", id=" +
                id +
                ", usingDecrypt=" +
                usingDecrypt +
                ", weight=" +
                weight +
                "]";
    }

}
