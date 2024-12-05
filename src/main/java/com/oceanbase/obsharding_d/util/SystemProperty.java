/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.util;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class SystemProperty {
    private SystemProperty() {

    }

    public static Set<String> getInnerProperties() {
        return innerProperties;
    }

    private static Set<String> innerProperties = new HashSet<>();
    private static final Pattern JAVA_INNER_PROPERTIES_PATTERN = Pattern.compile("^java.[a-zA-Z_0-9]+(.)*$");

    static {
        innerProperties.add("awt.toolkit");
        innerProperties.add("file.encoding");
        innerProperties.add("file.encoding.pkg");
        innerProperties.add("file.separator");
        innerProperties.add("java.awt.graphicsenv");
        innerProperties.add("java.awt.printerjob");
        innerProperties.add("java.class.path");
        innerProperties.add("java.class.version");
        innerProperties.add("java.endorsed.dirs");
        innerProperties.add("java.ext.dirs");
        innerProperties.add("java.home");
        innerProperties.add("java.io.tmpdir");
        innerProperties.add("java.library.path");
        innerProperties.add("java.runtime.name");
        innerProperties.add("java.runtime.version");
        innerProperties.add("java.specification.name");
        innerProperties.add("java.specification.vendor");
        innerProperties.add("java.specification.version");
        innerProperties.add("java.specification.maintenance.version"); // see https://github.com.oceanbase.obsharding_d/issues/3450
        innerProperties.add("java.vendor");
        innerProperties.add("java.vendor.url");
        innerProperties.add("java.vendor.url.bug");
        innerProperties.add("java.version");
        innerProperties.add("java.vm.info");
        innerProperties.add("java.vm.name");
        innerProperties.add("java.vm.specification.name");
        innerProperties.add("java.vm.specification.vendor");
        innerProperties.add("java.vm.specification.version");
        innerProperties.add("java.vm.vendor");
        innerProperties.add("java.vm.version");
        innerProperties.add("line.separator");
        innerProperties.add("os.arch");
        innerProperties.add("os.name");
        innerProperties.add("os.version");
        innerProperties.add("path.separator");
        innerProperties.add("sun.arch.data.model");
        innerProperties.add("sun.boot.class.path");
        innerProperties.add("sun.boot.library.path");
        innerProperties.add("sun.cpu.endian");
        innerProperties.add("sun.cpu.isalist");
        innerProperties.add("sun.desktop");
        innerProperties.add("sun.io.unicode.encoding");
        innerProperties.add("sun.java.command");
        innerProperties.add("sun.java.launcher");
        innerProperties.add("sun.jnu.encoding");
        innerProperties.add("sun.management.compiler");
        innerProperties.add("sun.os.patch.level");
        innerProperties.add("user.country");
        innerProperties.add("user.dir");
        innerProperties.add("user.home");
        innerProperties.add("user.language");
        innerProperties.add("user.name");
        innerProperties.add("user.script");
        innerProperties.add("user.timezone");
        innerProperties.add("user.variant");
        //for jdk 11
        innerProperties.add("java.vm.compressedOopsMode");
        innerProperties.add("jdk.debug");
        innerProperties.add("java.vendor.version");
        innerProperties.add("java.version.date");
        innerProperties.add("user.language.format");
        innerProperties.add("user.country.format");
        innerProperties.add("javax.net.debug");
    }

    public static boolean isJavaInnerProperties(String key) {
        Matcher m = JAVA_INNER_PROPERTIES_PATTERN.matcher(key);
        if (m.matches())
            return true;
        return false;
    }
}
