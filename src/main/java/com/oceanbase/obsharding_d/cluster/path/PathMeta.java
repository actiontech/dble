/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.path;

import java.util.Objects;

/**
 * @author dcy
 * Create Date: 2021-04-02
 */
public final class PathMeta<T> {
    String path;
    Class<T> tClass;

    private PathMeta(String path, Class<T> tClass) {
        this.path = path;
        this.tClass = tClass;
    }

    public String getPath() {
        return path;
    }

    public Class<T> getTClass() {
        return tClass;
    }

    public static <T> PathMeta<T> of(String path, Class<T> tClass) {
        return new PathMeta<>(path, tClass);
    }

    @Override
    public String toString() {
        return path;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PathMeta)) return false;
        PathMeta<?> pathMeta = (PathMeta<?>) o;
        return Objects.equals(path, pathMeta.path) && Objects.equals(tClass, pathMeta.tClass);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, tClass);
    }
}
