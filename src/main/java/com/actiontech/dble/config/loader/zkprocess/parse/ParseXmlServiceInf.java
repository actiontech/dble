/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.parse;

/**
 * ParseXmlServiceInf
 * <p>
 * <p>
 * author:liujun
 * Created:2016/9/16
 */
public interface ParseXmlServiceInf<T> {

    /**
     * parseToXmlWrite
     *
     * @param data
     * @return
     * @Created 2016/9/16
     */
    void parseToXmlWrite(T data, String outputPath, String dataName);

    /**
     * parseXmlToBean
     *
     * @param path xml
     * @return
     * @Created 2016/9/16
     */
    T parseXmlToBean(String path);

}
