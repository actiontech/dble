/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.store.fs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.util.*;


/**
 * A path to a file. It similar to the Java 7 <code>java.nio.file.Path</code>,
 * but simpler, and works with older versions of Java. It also implements the
 * relevant methods found in <code>java.nio.file.FileSystem</code> and
 * <code>FileSystems</code>
 *
 * @author ActionTech
 * @createTime 2013-11-11
 */
public abstract class FilePath {

    private static volatile FilePath defaultProvider;

    private static Map<String, FilePath> providers;

    /**
     * The prefix for temporary files.
     */
    private static String tempRandom;
    private static long tempSequence;

    /**
     * The complete path (which may be absolute or relative, depending on the
     * file system).
     */
    protected String name;

    /**
     * Get the file path object for the given path. Windows-style '\' is
     * replaced with '/'.
     *
     * @param path the path
     * @return the file path object
     */
    public static FilePath get(String path) {
        path = path.replace('\\', '/');
        int index = path.indexOf(':');
        registerDefaultProviders();
        if (index < 2) {
            // use the default provider if no prefix or
            // only a single character (drive name)
            return defaultProvider.getPath(path);
        }
        String scheme = path.substring(0, index);
        FilePath p = providers.get(scheme);
        if (p == null) {
            // provider not found - use the default
            p = defaultProvider;
        }
        return p.getPath(path);
        // return p;
    }

    private static void registerDefaultProviders() {
        if (defaultProvider == null) {
            synchronized (FilePath.class) {
                if (defaultProvider == null) {
                    Map<String, FilePath> map = Collections.synchronizedMap(new HashMap<String, FilePath>());
                    FilePathDisk p = new FilePathDisk();
                    map.put(p.getScheme(), p);
                    defaultProvider = p;
                    FilePathNio p2 = new FilePathNio();
                    map.put(p2.getScheme(), p2);
                    FilePathNioMapped p3 = new FilePathNioMapped();
                    map.put(p3.getScheme(), p3);
                    providers = map;
                }
            }
        }
    }

    /**
     * Register a file provider.
     *
     * @param provider the file provider
     */
    public static void register(FilePath provider) {
        registerDefaultProviders();
        providers.put(provider.getScheme(), provider);
    }

    /**
     * Unregister a file provider.
     *
     * @param provider the file provider
     */
    public static void unregister(FilePath provider) {
        registerDefaultProviders();
        providers.remove(provider.getScheme());
    }

    /**
     * Get the size of a file in bytes
     *
     * @return the size in bytes
     */
    public abstract long size();

    /**
     * Rename a file if this is allowed.
     *
     * @param newName the new fully qualified file name
     */
    public abstract void moveTo(FilePath newName);

    /**
     * Create a new file.
     *
     * @return true if creating was successful
     */
    public abstract boolean createFile();

    /**
     * Checks if a file exists.
     *
     * @return true if it exists
     */
    public abstract boolean exists();

    /**
     * Delete a file or directory if it exists. Directories may only be deleted
     * if they are empty.
     */
    public abstract void delete();

    /**
     * List the files and directories in the given directory.
     *
     * @return the list of fully qualified file names
     */
    public abstract List<FilePath> newDirectoryStream();

    /**
     * Normalize a file name.
     *
     * @return the normalized file name
     */
    public abstract FilePath toRealPath();

    /**
     * Get the parent directory of a file or directory.
     *
     * @return the parent directory name
     */
    public abstract FilePath getParent();

    /**
     * Check if it is a file or a directory.
     *
     * @return true if it is a directory
     */
    public abstract boolean isDirectory();

    /**
     * Check if the file name includes a path.
     *
     * @return if the file name is absolute
     */
    public abstract boolean isAbsolute();

    /**
     * Get the last modified date of a file
     *
     * @return the last modified date
     */
    public abstract long lastModified();

    /**
     * Check if the file is writable.
     *
     * @return if the file is writable
     */
    public abstract boolean canWrite();

    /**
     * Create a directory (all required parent directories already exist).
     */
    public abstract void createDirectory();

    /**
     * Get the file or directory name (the last element of the path).
     *
     * @return the last element of the path
     */
    public String getName() {
        int idx = Math.max(name.indexOf(':'), name.lastIndexOf('/'));
        return idx < 0 ? name : name.substring(idx + 1);
    }

    /**
     * Create an output stream to writeDirectly into the file.
     *
     * @param append if true, the file will grow, if false, the file will be
     *               truncated first
     * @return the output stream
     */
    public abstract OutputStream newOutputStream(boolean append) throws IOException;

    /**
     * Open a random access file object.
     *
     * @param mode the access mode. Supported are r, rw, rws, rwd
     * @return the file object
     */
    public abstract FileChannel open(String mode) throws IOException;

    /**
     * Create an input stream to read from the file.
     *
     * @return the input stream
     */
    public abstract InputStream newInputStream() throws IOException;

    /**
     * Disable the ability to writeDirectly.
     *
     * @return true if the call was successful
     */
    public abstract boolean setReadOnly();

    /**
     * Create a new temporary file.
     *
     * @param suffix       the suffix
     * @param deleteOnExit if the file should be deleted when the virtual machine exists
     * @return the name of the created file
     */
    public FilePath createTempFile(String suffix, boolean deleteOnExit) throws IOException {
        while (true) {
            FilePath p = getPath(name + getNextTempFileNamePart(false) + suffix);
            if (p.exists()) {
                // in theory, the random number could collide
                getNextTempFileNamePart(true);
                continue;
            }
            return p;
        }
    }

    /**
     * Get the next temporary file name part (the part in the middle).
     *
     * @param newRandom if the random part of the filename should change
     * @return the file name part
     */
    protected static synchronized String getNextTempFileNamePart(boolean newRandom) {
        if (newRandom || tempRandom == null) {
            tempRandom = new Random().nextInt(Integer.MAX_VALUE) + ".";
        }
        return tempRandom + tempSequence++;
    }

    /**
     * Get the string representation. The returned string can be used to
     * construct a new object.
     *
     * @return the path as a string
     */
    @Override
    public String toString() {
        return name;
    }

    /**
     * Get the scheme (prefix) for this file provider. This is similar to
     * <code>java.nio.file.spi.FileSystemProvider.getScheme</code>.
     *
     * @return the scheme
     */
    public abstract String getScheme();

    /**
     * Convert a file to a path. This is similar to
     * <code>java.nio.file.spi.FileSystemProvider.getPath</code>, but may return
     * an object even if the scheme doesn't match in case of the the default
     * file provider.
     *
     * @param path the path
     * @return the file path object
     */
    public abstract FilePath getPath(String path);

    /**
     * Get the unwrapped file name (without wrapper prefixes if wrapping /
     * delegating file systems are used).
     *
     * @return the unwrapped path
     */
    public FilePath unwrap() {
        return this;
    }
}
