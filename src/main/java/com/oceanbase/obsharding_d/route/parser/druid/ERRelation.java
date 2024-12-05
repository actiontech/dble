/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.route.parser.druid;

import com.oceanbase.obsharding_d.config.model.sharding.table.ERTable;
import org.jetbrains.annotations.NotNull;

import java.util.Comparator;
import java.util.Objects;

public class ERRelation implements Comparable<ERRelation> {
    private ERTable left;
    private ERTable right;

    public ERRelation(ERTable left, ERTable right) {
        this.left = left;
        this.right = right;
    }

    public ERTable getLeft() {
        return left;
    }

    public void setLeft(ERTable left) {
        this.left = left;
    }

    public ERTable getRight() {
        return right;
    }

    public void setRight(ERTable right) {
        this.right = right;
    }


    @Override
    public int compareTo(@NotNull ERRelation o) {
        return Comparator.comparing(ERRelation::getLeft)
                .thenComparing(ERRelation::getRight)
                .compare(this, o);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ERRelation that = (ERRelation) o;
        return Objects.equals(left, that.left) &&
                Objects.equals(right, that.right);
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right);
    }
}
