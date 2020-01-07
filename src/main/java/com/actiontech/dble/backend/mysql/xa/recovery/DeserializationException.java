/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.xa.recovery;

/**
 * Created by zhangchao on 2016/10/17.
 */
public class DeserializationException extends Exception {
    private static final long serialVersionUID = -3835526236269555460L;

    public DeserializationException(String content) {
        super(content);
    }
}
