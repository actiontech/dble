/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.config.loader.xml;

import org.w3c.dom.Element;

import java.lang.reflect.InvocationTargetException;

public interface Loader<P, T> {
    void load(Element root, T t) throws IllegalAccessException, InvocationTargetException;
}
