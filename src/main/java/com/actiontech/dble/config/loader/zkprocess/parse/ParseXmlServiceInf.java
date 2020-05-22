/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.parse;

public interface ParseXmlServiceInf<T> {

    void parseToXmlWrite(T data, String outputPath, String dataName);

    T parseXmlToBean(String path);

}
