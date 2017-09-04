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

package com.actiontech.dble.memory.unsafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Cleaner;
import sun.misc.Unsafe;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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
    private static final Unsafe UNSAFE;

    public static final int BYTE_ARRAY_OFFSET;

    public static final int SHORT_ARRAY_OFFSET;

    public static final int INT_ARRAY_OFFSET;

    public static final int LONG_ARRAY_OFFSET;

    public static final int FLOAT_ARRAY_OFFSET;

    public static final int DOUBLE_ARRAY_OFFSET;

    private static final long MAX_DIRECT_MEMORY;

    private static final boolean UNALIGNED;

    public static final boolean LITTLE_ENDIAN = ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN);

    static {
        boolean unaligned;
        // use reflection to access unaligned field
        try {
            Class<?> bitsClass =
                    Class.forName("java.nio.Bits", false, ClassLoader.getSystemClassLoader());
            Method unalignedMethod = bitsClass.getDeclaredMethod("unaligned");
            unalignedMethod.setAccessible(true);
            unaligned = Boolean.TRUE.equals(unalignedMethod.invoke(null));
        } catch (Throwable t) {
            // We at least know x86 and x64 support unaligned access.
            String arch = System.getProperty("os.arch", "");
            //noinspection DynamicRegexReplaceableByCompiledPattern
            unaligned = arch.matches("^(i[3-6]86|x86(_64)?|x64|amd64)$");
        }
        UNALIGNED = unaligned;
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
                LOGGER.error(var9.getMessage());
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

    public static long getMaxHeapMemory() {
        return Runtime.getRuntime().maxMemory();
    }

    /**
     * @return true when running JVM is having sun's Unsafe package available in it and underlying
     * system having unaligned-access capability.
     */
    public static boolean unaligned() {
        return UNALIGNED;
    }

    public static int getInt(Object object, long offset) {
        return UNSAFE.getInt(object, offset);
    }

    public static void putInt(Object object, long offset, int value) {
        UNSAFE.putInt(object, offset, value);
    }

    public static boolean getBoolean(Object object, long offset) {
        return UNSAFE.getBoolean(object, offset);
    }

    public static void putBoolean(Object object, long offset, boolean value) {
        UNSAFE.putBoolean(object, offset, value);
    }

    public static byte getByte(Object object, long offset) {
        return UNSAFE.getByte(object, offset);
    }

    public static void putByte(Object object, long offset, byte value) {
        UNSAFE.putByte(object, offset, value);
    }

    public static short getShort(Object object, long offset) {
        return UNSAFE.getShort(object, offset);
    }

    public static void putShort(Object object, long offset, short value) {
        UNSAFE.putShort(object, offset, value);
    }

    public static long getLong(Object object, long offset) {
        return UNSAFE.getLong(object, offset);
    }

    public static void putLong(Object object, long offset, long value) {
        UNSAFE.putLong(object, offset, value);
    }

    public static float getFloat(Object object, long offset) {
        return UNSAFE.getFloat(object, offset);
    }

    public static void putFloat(Object object, long offset, float value) {
        UNSAFE.putFloat(object, offset, value);
    }

    public static double getDouble(Object object, long offset) {
        return UNSAFE.getDouble(object, offset);
    }

    public static void putDouble(Object object, long offset, double value) {
        UNSAFE.putDouble(object, offset, value);
    }


    public static Object getObjectVolatile(Object object, long offset) {
        return UNSAFE.getObjectVolatile(object, offset);
    }

    public static void putObjectVolatile(Object object, long offset, Object value) {
        UNSAFE.putObjectVolatile(object, offset, value);
    }

    public static long allocateMemory(long size) {
        return UNSAFE.allocateMemory(size);
    }

    public static void freeMemory(long address) {
        UNSAFE.freeMemory(address);
    }

    public static long reallocateMemory(long address, long oldSize, long newSize) {
        long newMemory = UNSAFE.allocateMemory(newSize);
        copyMemory(null, address, null, newMemory, oldSize);
        freeMemory(address);
        return newMemory;
    }

    /**
     * Uses internal JDK APIs to allocate a DirectByteBuffer while ignoring the JVM's
     * MaxDirectMemorySize limit (the default limit is too low and we do not want to require users
     * to increase it).
     */
    @SuppressWarnings("unchecked")
    public static ByteBuffer allocateDirectBuffer(int size) {
        try {
            Class cls = Class.forName("java.nio.DirectByteBuffer");
            Constructor constructor = cls.getDeclaredConstructor(Long.TYPE, Integer.TYPE);
            constructor.setAccessible(true);
            Field cleanerField = cls.getDeclaredField("cleaner");
            cleanerField.setAccessible(true);
            final long memory = allocateMemory(size);
            ByteBuffer buffer = (ByteBuffer) constructor.newInstance(memory, size);
            Cleaner cleaner = Cleaner.create(buffer, new Runnable() {
                @Override
                public void run() {
                    freeMemory(memory);
                }
            });
            cleanerField.set(buffer, cleaner);
            return buffer;
        } catch (Exception e) {
            throwException(e);
        }
        throw new IllegalStateException("unreachable");
    }

    public static void setMemory(long address, byte value, long size) {
        UNSAFE.setMemory(address, size, value);
    }

    public static void copyMemory(
            Object src, long srcOffset, Object dst, long dstOffset, long length) {
        // Check if dstOffset is before or after srcOffset to determine if we should copy
        // forward or backwards. This is necessary in case src and dst overlap.
        if (dstOffset < srcOffset) {
            while (length > 0) {
                long size = Math.min(length, UNSAFE_COPY_THRESHOLD);
                UNSAFE.copyMemory(src, srcOffset, dst, dstOffset, size);
                length -= size;
                srcOffset += size;
                dstOffset += size;
            }
        } else {
            srcOffset += length;
            dstOffset += length;
            while (length > 0) {
                long size = Math.min(length, UNSAFE_COPY_THRESHOLD);
                srcOffset -= size;
                dstOffset -= size;
                UNSAFE.copyMemory(src, srcOffset, dst, dstOffset, size);
                length -= size;
            }

        }
    }

    /**
     * Raises an exception bypassing compiler checks for checked exceptions.
     */
    public static void throwException(Throwable t) {
        UNSAFE.throwException(t);
    }

    /**
     * Limits the number of bytes to copy per {@link Unsafe#copyMemory(long, long, long)} to
     * allow safepoint polling during a large copy.
     */
    private static final long UNSAFE_COPY_THRESHOLD = 1024L * 1024L;

    static {
        Unsafe unsafe;
        try {
            Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            unsafe = (Unsafe) unsafeField.get(null);
        } catch (Throwable cause) {
            unsafe = null;
        }
        UNSAFE = unsafe;

        if (UNSAFE != null) {
            BYTE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
            SHORT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(short[].class);
            INT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(int[].class);
            LONG_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(long[].class);
            FLOAT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(float[].class);
            DOUBLE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(double[].class);
        } else {
            BYTE_ARRAY_OFFSET = 0;
            SHORT_ARRAY_OFFSET = 0;
            INT_ARRAY_OFFSET = 0;
            LONG_ARRAY_OFFSET = 0;
            FLOAT_ARRAY_OFFSET = 0;
            DOUBLE_ARRAY_OFFSET = 0;
        }
    }

    public static long objectFieldOffset(Field field) {
        return UNSAFE.objectFieldOffset(field);
    }

    public static void putOrderedLong(Object object, long valueOffset, long initialValue) {
        UNSAFE.putOrderedLong(object, valueOffset, initialValue);
    }

    public static void putLongVolatile(Object object, long valueOffset, long value) {
        UNSAFE.putLongVolatile(object, valueOffset, value);
    }

    public static boolean compareAndSwapLong(Object object, long valueOffset, long expectedValue, long newValue) {
        return UNSAFE.compareAndSwapLong(object, valueOffset, expectedValue, newValue);
    }

    public static int arrayBaseOffset(Class aClass) {
        return UNSAFE.arrayBaseOffset(aClass);
    }

    public static int arrayIndexScale(Class aClass) {
        return UNSAFE.arrayIndexScale(aClass);
    }

    public static void putOrderedInt(Object availableBuffer, long bufferAddress, int flag) {
        UNSAFE.putOrderedInt(availableBuffer, bufferAddress, flag);
    }

    public static int getIntVolatile(Object availableBuffer, long bufferAddress) {
        return UNSAFE.getIntVolatile(availableBuffer, bufferAddress);
    }

    public static Object getObject(Object entries, long l) {
        return UNSAFE.getObject(entries, l);
    }

    public static char getChar(Object baseObj, long l) {
        return UNSAFE.getChar(baseObj, l);
    }

    public static void putChar(Object baseObj, long l, char value) {
        UNSAFE.putChar(baseObj, l, value);
    }
}
