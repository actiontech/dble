/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.oceanbase.obsharding_d.memory.unsafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Platform {
    private Platform() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(Platform.class);
    private static final Pattern MAX_DIRECT_MEMORY_SIZE_ARG_PATTERN =
            Pattern.compile("\\s*-XX:MaxDirectMemorySize\\s*=\\s*([0-9]+)\\s*([kKmMgG]?)\\s*$");

    private static final long MAX_DIRECT_MEMORY;

    static {
        MAX_DIRECT_MEMORY = maxDirectMemory();
    }


    private static ClassLoader getSystemClassLoader() {
        return System.getSecurityManager() == null ? ClassLoader.getSystemClassLoader() : (ClassLoader) AccessController.doPrivileged(new PrivilegedAction() {
            public ClassLoader run() {
                return ClassLoader.getSystemClassLoader();
            }
        });
    }

    /**
     * GET  MaxDirectMemory Size,from Netty Project!
     */
    private static long maxDirectMemory() {
        long maxDirectMemory = 0L;
        Class t;
        try {
            t = Class.forName("sun.misc.VM", true, getSystemClassLoader());
            Method runtimeClass = t.getDeclaredMethod("maxDirectMemory");
            maxDirectMemory = ((Number) runtimeClass.invoke(null, new Object[0])).longValue();
        } catch (Throwable var8) {
            //ignore error
        }

        if (maxDirectMemory > 0L) {
            return maxDirectMemory;
        } else {
            try {
                t = Class.forName("java.lang.management.ManagementFactory", true, getSystemClassLoader());
                Class var10 = Class.forName("java.lang.management.RuntimeMXBean", true, getSystemClassLoader());
                Object runtime = t.getDeclaredMethod("getRuntimeMXBean", new Class[0]).invoke(null);
                List vmArgs = (List) var10.getDeclaredMethod("getInputArguments", new Class[0]).invoke(runtime, new Object[0]);

                label41:
                for (int i = vmArgs.size() - 1; i >= 0; --i) {
                    Matcher m = MAX_DIRECT_MEMORY_SIZE_ARG_PATTERN.matcher((CharSequence) vmArgs.get(i));
                    if (m.matches()) {
                        maxDirectMemory = Long.parseLong(m.group(1));
                        switch (m.group(2).charAt(0)) {
                            case 'G':
                            case 'g':
                                maxDirectMemory *= 1073741824L;
                                break label41;
                            case 'K':
                            case 'k':
                                maxDirectMemory *= 1024L;
                                break label41;
                            case 'M':
                            case 'm':
                                maxDirectMemory *= 1048576L;
                                break label41;
                            default:
                                break label41;
                        }
                    }
                }
            } catch (Throwable var9) {
                LOGGER.warn(var9.getMessage());
            }

            if (maxDirectMemory <= 0L) {
                maxDirectMemory = Runtime.getRuntime().maxMemory();
                //System.out.println("maxDirectMemory: {} bytes (maybe)" + Long.valueOf(maxDirectMemory));
            } else {
                //System.out.println("maxDirectMemory: {} bytes" + Long.valueOf(maxDirectMemory));
            }
            return maxDirectMemory;
        }
    }

    public static long getMaxDirectMemory() {
        return MAX_DIRECT_MEMORY;
    }


}
