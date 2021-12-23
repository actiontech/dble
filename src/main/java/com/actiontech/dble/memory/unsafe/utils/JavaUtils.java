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

package com.actiontech.dble.memory.unsafe.utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class,
 */

public final class JavaUtils {
    private JavaUtils() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(JavaUtils.class);

    /**
     * Define a default value for driver memory here since this value is referenced across the code
     * base and nearly all files already use Utils.scala
     */
    public static final long DEFAULT_DRIVER_MEM_MB = 1024;

    private static final int MAX_DIR_CREATION_ATTEMPTS = 10;


    /*
     * Delete a file or directory and its contents recursively.
     * Don't follow directories if they are symlinks.
     * Throws an exception if deletion is unsuccessful.
     */
    public static void deleteRecursively(File file) throws IOException {
        if (file == null) {
            return;
        }

        if (file.isDirectory() && !isSymlink(file)) {
            IOException savedIOException = null;
            for (File child : listFilesSafely(file)) {
                try {
                    deleteRecursively(child);
                } catch (IOException e) {
                    // In case of multiple exceptions, only last one will be thrown
                    savedIOException = e;
                }
            }
            if (savedIOException != null) {
                throw savedIOException;
            }
        }

        boolean deleted = file.delete();
        // Delete can also fail if the file simply did not exist.
        if (!deleted && file.exists()) {
            throw new IOException("Failed to delete: " + file.getAbsolutePath());
        }
    }

    private static File[] listFilesSafely(File file) throws IOException {
        if (file.exists()) {
            File[] files = file.listFiles();
            if (files == null) {
                throw new IOException("Failed to list files for dir: " + file);
            }
            return files;
        } else {
            return new File[0];
        }
    }

    private static boolean isSymlink(File file) throws IOException {
        Preconditions.checkNotNull(file);
        File fileInCanonicalDir = null;
        if (file.getParent() == null) {
            fileInCanonicalDir = file;
        } else {
            fileInCanonicalDir = new File(file.getParentFile().getCanonicalFile(), file.getName());
        }
        return !fileInCanonicalDir.getCanonicalFile().equals(fileInCanonicalDir.getAbsoluteFile());
    }

    private static final ImmutableMap<String, TimeUnit> TIME_SUFFIXES =
            ImmutableMap.<String, TimeUnit>builder().
                    put("us", TimeUnit.MICROSECONDS).
                    put("ms", TimeUnit.MILLISECONDS).
                    put("s", TimeUnit.SECONDS).
                    put("m", TimeUnit.MINUTES).
                    put("min", TimeUnit.MINUTES).
                    put("h", TimeUnit.HOURS).
                    put("d", TimeUnit.DAYS).
                    build();

    private static final ImmutableMap<String, ByteUnit> BYTE_SUFFIXES =
            ImmutableMap.<String, ByteUnit>builder().
                    put("b", ByteUnit.BYTE).
                    put("k", ByteUnit.KiB).
                    put("kb", ByteUnit.KiB).
                    put("m", ByteUnit.MiB).
                    put("mb", ByteUnit.MiB).
                    put("g", ByteUnit.GiB).
                    put("gb", ByteUnit.GiB).
                    put("t", ByteUnit.TiB).
                    put("tb", ByteUnit.TiB).
                    put("p", ByteUnit.PiB).
                    put("pb", ByteUnit.PiB).
                    build();

    /**
     * Convert a passed time string (e.g. 50s, 100ms, or 250us) to a time count in the given unit.
     * The unit is also considered the default if the given string does not specify a unit.
     */
    public static long timeStringAs(String str, TimeUnit unit) {
        String lower = str.toLowerCase().trim();

        try {
            Matcher m = Pattern.compile("(-?[0-9]+)([a-z]+)?").matcher(lower);
            if (!m.matches()) {
                throw new NumberFormatException("Failed to parse time string: " + str);
            }

            long val = Long.parseLong(m.group(1));
            String suffix = m.group(2);

            // Check for invalid suffixes
            if (suffix != null && !TIME_SUFFIXES.containsKey(suffix)) {
                throw new NumberFormatException("Invalid suffix: \"" + suffix + "\"");
            }

            // If suffix is valid use that, otherwise none was provided and use the default passed
            return unit.convert(val, suffix != null ? TIME_SUFFIXES.get(suffix) : unit);
        } catch (NumberFormatException e) {
            String timeError = "Time must be specified as seconds (s), " +
                    "milliseconds (ms), microseconds (us), minutes (m or min), hour (h), or day (d). " +
                    "E.g. 50s, 100ms, or 250us.";

            throw new NumberFormatException(timeError + "\n" + e.getMessage());
        }
    }

    public static long timeStringAsMs(String str, ImmutableMap<String, TimeUnit> map) {
        String lower = str.toLowerCase().trim();

        try {
            Matcher m = Pattern.compile("([0-9]+)([a-z]+)?").matcher(lower);
            if (!m.matches()) {
                throw new NumberFormatException("Failed to parse time string: " + str);
            }

            long val = Long.parseLong(m.group(1));
            if (val == 0) {
                throw new NumberFormatException("Cannot be zero");
            }
            String suffix = m.group(2);

            // Check for invalid suffixes
            if (suffix != null && !map.containsKey(suffix)) {
                throw new NumberFormatException("Invalid suffix: \"" + suffix + "\"");
            }
            TimeUnit unit = TimeUnit.MILLISECONDS;
            // If suffix is valid use that, otherwise none was provided and use the default passed
            return unit.convert(val, suffix != null ? TIME_SUFFIXES.get(suffix) : TimeUnit.SECONDS);
        } catch (NumberFormatException e) {
            String timeError = "Time must be specified as seconds (s), " +
                    "milliseconds (ms), microseconds (us), minutes (m or min), hour (h), or day (d). " +
                    "E.g. 50s, 100ms, or 250us.";

            throw new NumberFormatException(timeError + "\n" + e.getMessage());
        }
    }

    /**
     * Convert a time parameter such as (50s, 100ms, or 250us) to milliseconds for internal use. If
     * no suffix is provided, the passed number is assumed to be in ms.
     */
    public static long timeStringAsMs(String str) {
        return timeStringAs(str, TimeUnit.MILLISECONDS);
    }

    /**
     * Convert a time parameter such as (50s, 100ms, or 250us) to seconds for internal use. If
     * no suffix is provided, the passed number is assumed to be in seconds.
     */
    public static long timeStringAsSec(String str) {
        return timeStringAs(str, TimeUnit.SECONDS);
    }

    /**
     * Convert a passed byte string (e.g. 50b, 100kb, or 250mb) to the given. If no suffix is
     * provided, a direct conversion to the provided unit is attempted.
     */
    public static long byteStringAs(String str, ByteUnit unit) {
        String lower = str.toLowerCase().trim();

        try {
            Matcher m = Pattern.compile("([0-9]+)([a-z]+)?").matcher(lower);
            Matcher fractionMatcher = Pattern.compile("([0-9]+\\.[0-9]+)([a-z]+)?").matcher(lower);

            if (m.matches()) {
                long val = Long.parseLong(m.group(1));
                String suffix = m.group(2);

                // Check for invalid suffixes
                if (suffix != null && !BYTE_SUFFIXES.containsKey(suffix)) {
                    throw new NumberFormatException("Invalid suffix: \"" + suffix + "\"");
                }

                // If suffix is valid use that, otherwise none was provided and use the default passed
                return unit.convertFrom(val, suffix != null ? BYTE_SUFFIXES.get(suffix) : unit);
            } else if (fractionMatcher.matches()) {
                throw new NumberFormatException("Fractional values are not supported. Input was: " +
                        fractionMatcher.group(1));
            } else {
                throw new NumberFormatException("Failed to parse byte string: " + str);
            }

        } catch (NumberFormatException e) {
            String byteError = "Size must be specified as bytes (b), " +
                    "kibibytes (k), mebibytes (m), gibibytes (g), tebibytes (t), or pebibytes(p). " +
                    "E.g. 50b, 100k, or 250m.";

            throw new NumberFormatException(byteError + "\n" + e.getMessage());
        }
    }

    /**
     * Convert a passed byte string (e.g. 50b, 100k, or 250m) to bytes for
     * internal use.
     * <p>
     * If no suffix is provided, the passed number is assumed to be in bytes.
     */
    public static long byteStringAsBytes(String str) {
        return byteStringAs(str, ByteUnit.BYTE);
    }

    /**
     * Convert a passed byte string (e.g. 50b, 100k, or 250m) to kibibytes for
     * internal use.
     * <p>
     * If no suffix is provided, the passed number is assumed to be in kibibytes.
     */
    public static long byteStringAsKb(String str) {
        return byteStringAs(str, ByteUnit.KiB);
    }

    /**
     * Convert a passed byte string (e.g. 50b, 100k, or 250m) to mebibytes for
     * internal use.
     * <p>
     * If no suffix is provided, the passed number is assumed to be in mebibytes.
     */
    public static long byteStringAsMb(String str) {
        return byteStringAs(str, ByteUnit.MiB);
    }

    /**
     * Convert a passed byte string (e.g. 50b, 100k, or 250m) to gibibytes for
     * internal use.
     * <p>
     * If no suffix is provided, the passed number is assumed to be in gibibytes.
     */
    public static long byteStringAsGb(String str) {
        return byteStringAs(str, ByteUnit.GiB);
    }


    public static String bytesToString(long size) {
        long cTB = 1L << 40;
        long cGB = 1L << 30;
        long cMB = 1L << 20;
        long cKB = 1L << 10;
        double value = 0;
        String unit = null;

        if (size >= 2 * cTB) {
            value = (double) size / cTB;
            unit = "TB";
        } else if (size >= 2 * cGB) {
            value = (double) size / cGB;
            unit = "GB";
        } else if (size >= 2 * cMB) {
            value = (double) size / cMB;
            unit = "MB";
        } else if (size >= 2 * cKB) {
            value = (double) size / cKB;
            unit = "KB";
        } else {
            value = (double) size;
            unit = "B";
        }

        return value + " " + unit;
    }



    /* Calculates 'x' modulo 'mod', takes to consideration sign of x,
     * i.e. if 'x' is negative, than 'x' % 'mod' is negative too
     * so function return (x % mod) + mod in that case.
     */
    public static int nonNegativeMod(int x, int mod) {
        int rawMod = x % mod;
        int temp;
        if (rawMod < 0)
            temp = mod;
        else
            temp = 0;
        return (rawMod + temp);
    }


    public static int nonNegativeHash(Object obj) {
        // Required ?
        if (obj == null) return 0;

        int hash = obj.hashCode();
        // math.abs fails for Int.MinValue
        int hashAbs = 0;

        if (Integer.MAX_VALUE != hash && Integer.MIN_VALUE != hash)
            hashAbs = Math.abs(hash);

        // Nothing else to guard against ?
        return hashAbs;
    }

}
