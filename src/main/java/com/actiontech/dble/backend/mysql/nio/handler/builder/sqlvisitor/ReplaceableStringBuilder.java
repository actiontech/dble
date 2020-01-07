/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.builder.sqlvisitor;

import com.actiontech.dble.plan.common.ptr.StringPtr;

import java.util.ArrayList;
import java.util.List;

/**
 * @author ActionTech
 * @CreateTime 2015/12/15
 */
public class ReplaceableStringBuilder {
    private List<Element> elements;

    public ReplaceableStringBuilder() {
        elements = new ArrayList<>();
    }

    public Element getCurrentElement() {
        Element curEle = null;
        if (elements.isEmpty()) {
            curEle = new Element();
            elements.add(curEle);
        } else {
            curEle = elements.get(elements.size() - 1);
            if (curEle.getRepString() != null) {
                curEle = new Element();
                elements.add(curEle);
            }
        }
        return curEle;
    }

    public ReplaceableStringBuilder append(ReplaceableStringBuilder other) {
        if (other != null)
            this.elements.addAll(other.elements);
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Element ele : elements) {
            sb.append(ele.getSb());
            StringPtr rep = ele.getRepString();
            if (rep != null)
                sb.append(rep.get());
        }
        return sb.toString();
    }

    /**
     * like stringbuilder.setlength(0)
     */
    public void clear() {
        elements.clear();
    }

    public static final class Element {
        private final StringBuilder sb;
        private StringPtr repString;

        public Element() {
            sb = new StringBuilder();
        }

        /**
         * @return the sb
         */
        public StringBuilder getSb() {
            return sb;
        }

        /**
         * @return the repString
         */
        public StringPtr getRepString() {
            return repString;
        }

        /**
         * @param repString the repString to set
         */
        public void setRepString(StringPtr repString) {
            if (this.repString != null)
                throw new RuntimeException("error use");
            this.repString = repString;
        }

    }

}
