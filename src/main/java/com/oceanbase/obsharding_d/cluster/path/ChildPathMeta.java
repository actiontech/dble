/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.path;

import java.util.Objects;

/**
 * @author dcy
 * Create Date: 2021-04-02
 */
public final class ChildPathMeta<T> {
    String path;
    Class<T> childClass;

    private ChildPathMeta(String path, Class<T> childClass) {
        this.path = path;
        this.childClass = childClass;
    }

    public String getPath() {
        return path;
    }

    public Class<T> getChildClass() {
        return childClass;
    }

    public static <T> ChildPathMeta<T> of(String path, Class<T> tClass) {
        return new ChildPathMeta<>(path, tClass);
    }

    @Override
    public String toString() {
        return path;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ChildPathMeta)) return false;
        ChildPathMeta<?> pathMeta = (ChildPathMeta<?>) o;
        return Objects.equals(path, pathMeta.path) && Objects.equals(childClass, pathMeta.childClass);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, childClass);
    }
}
