/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.store.fs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * @author ActionTech
 * @createTime 2013-11-11
 */
public final class FileUtils {
    private static Logger logger = LoggerFactory.getLogger(FileUtils.class);

    private FileUtils() {
    }

    /**
     * Checks if a file exists. This method is similar to Java 7
     * <code>java.nio.file.Path.exists</code>.
     *
     * @param fileName the file name
     * @return true if it exists
     */
    public static boolean exists(String fileName) {
        return FilePath.get(fileName).exists();
    }

    /**
     * Create a directory (all required parent directories must already exist).
     * This method is similar to Java 7
     * <code>java.nio.file.Path.createDirectory</code>.
     *
     * @param directoryName the directory name
     */
    public static void createDirectory(String directoryName) {
        FilePath.get(directoryName).createDirectory();
    }

    /**
     * Create a new file. This method is similar to Java 7
     * <code>java.nio.file.Path.createFile</code>, but returns false instead of
     * throwing a exception if the file already existed.
     *
     * @param fileName the file name
     * @return true if creating was successful
     */
    public static boolean createFile(String fileName) {
        return FilePath.get(fileName).createFile();
    }

    /**
     * Delete a file or directory if it exists. Directories may only be deleted
     * if they are empty. This method is similar to Java 7
     * <code>java.nio.file.Path.deleteIfExists</code>.
     *
     * @param path the file or directory name
     */
    public static void delete(String path) {
        FilePath.get(path).delete();
    }

    /**
     * Get the canonical file or directory name. This method is similar to Java
     * 7 <code>java.nio.file.Path.toRealPath</code>.
     *
     * @param fileName the file name
     * @return the normalized file name
     */
    public static String toRealPath(String fileName) {
        return FilePath.get(fileName).toRealPath().toString();
    }

    /**
     * Get the parent directory of a file or directory. This method returns null
     * if there is no parent. This method is similar to Java 7
     * <code>java.nio.file.Path.getParent</code>.
     *
     * @param fileName the file or directory name
     * @return the parent directory name
     */
    public static String getParent(String fileName) {
        FilePath p = FilePath.get(fileName).getParent();
        return p == null ? null : p.toString();
    }

    /**
     * Check if the file name includes a path. This method is similar to Java 7
     * <code>java.nio.file.Path.isAbsolute</code>.
     *
     * @param fileName the file name
     * @return if the file name is absolute
     */
    public static boolean isAbsolute(String fileName) {
        return FilePath.get(fileName).isAbsolute();
    }

    /**
     * Rename a file if this is allowed. This method is similar to Java 7
     * <code>java.nio.file.Path.moveTo</code>.
     *
     * @param oldName the old fully qualified file name
     * @param newName the new fully qualified file name
     */
    public static void moveTo(String oldName, String newName) {
        FilePath.get(oldName).moveTo(FilePath.get(newName));
    }

    /**
     * Get the file or directory name (the last element of the path). This
     * method is similar to Java 7 <code>java.nio.file.Path.getName</code>.
     *
     * @param path the directory and file name
     * @return just the file name
     */
    public static String getName(String path) {
        return FilePath.get(path).getName();
    }

    /**
     * List the files and directories in the given directory. This method is
     * similar to Java 7 <code>java.nio.file.Path.newDirectoryStream</code>.
     *
     * @param path the directory
     * @return the list of fully qualified file names
     */
    public static List<String> newDirectoryStream(String path) {
        List<FilePath> list = FilePath.get(path).newDirectoryStream();
        int len = list.size();
        List<String> result = new ArrayList<>(len);
        for (FilePath aList : list) {
            result.add(aList.toString());
        }
        return result;
    }

    /**
     * Get the last modified date of a file. This method is similar to Java 7
     * <code>java.nio.file.attribute.Attributes.
     * readBasicFileAttributes(file).lastModified().toMillis()</code>
     *
     * @param fileName the file name
     * @return the last modified date
     */
    public static long lastModified(String fileName) {
        return FilePath.get(fileName).lastModified();
    }

    /**
     * Get the size of a file in bytes This method is similar to Java 7
     * <code>java.nio.file.attribute.Attributes.
     * readBasicFileAttributes(file).size()</code>
     *
     * @param fileName the file name
     * @return the size in bytes
     */
    public static long size(String fileName) {
        return FilePath.get(fileName).size();
    }

    /**
     * Check if it is a file or a directory.
     * <code>java.nio.file.attribute.Attributes.
     * readBasicFileAttributes(file).isDirectory()</code>
     *
     * @param fileName the file or directory name
     * @return true if it is a directory
     */
    public static boolean isDirectory(String fileName) {
        return FilePath.get(fileName).isDirectory();
    }

    /**
     * Open a random access file object. This method is similar to Java 7
     * <code>java.nio.channels.FileChannel.open</code>.
     *
     * @param fileName the file name
     * @param mode     the access mode. Supported are r, rw, rws, rwd
     * @return the file object
     */
    public static FileChannel open(String fileName, String mode) throws IOException {
        return FilePath.get(fileName).open(mode);
    }

    /**
     * Create an input stream to read from the file. This method is similar to
     * Java 7 <code>java.nio.file.Path.newInputStream</code>.
     *
     * @param fileName the file name
     * @return the input stream
     */
    public static InputStream newInputStream(String fileName) throws IOException {
        return FilePath.get(fileName).newInputStream();
    }

    /**
     * Create an output stream to writeDirectly into the file. This method is similar to
     * Java 7 <code>java.nio.file.Path.newOutputStream</code>.
     *
     * @param fileName the file name
     * @param append   if true, the file will grow, if false, the file will be
     *                 truncated first
     * @return the output stream
     */
    public static OutputStream newOutputStream(String fileName, boolean append) throws IOException {
        return FilePath.get(fileName).newOutputStream(append);
    }

    /**
     * Check if the file is writable. This method is similar to Java 7
     * <code>java.nio.file.Path.checkAccess(AccessMode.WRITE)</code>
     *
     * @param fileName the file name
     * @return if the file is writable
     */
    public static boolean canWrite(String fileName) {
        return FilePath.get(fileName).canWrite();
    }

    // special methods =======================================

    /**
     * Disable the ability to writeDirectly. The file can still be deleted afterwards.
     *
     * @param fileName the file name
     * @return true if the call was successful
     */
    public static boolean setReadOnly(String fileName) {
        return FilePath.get(fileName).setReadOnly();
    }

    /**
     * Get the unwrapped file name (without wrapper prefixes if wrapping /
     * delegating file systems are used).
     *
     * @param fileName the file name
     * @return the unwrapped
     */
    public static String unwrap(String fileName) {
        return FilePath.get(fileName).unwrap().toString();
    }

    // utility methods =======================================

    /**
     * Delete a directory or file and all subdirectories and files.
     *
     * @param path    the path
     * @param tryOnly whether errors should be ignored
     */
    public static void deleteRecursive(String path, boolean tryOnly) {
        if (exists(path)) {
            if (isDirectory(path)) {
                for (String s : newDirectoryStream(path)) {
                    deleteRecursive(s, tryOnly);
                }
            }
            if (tryOnly) {
                tryDelete(path);
            } else {
                delete(path);
            }
        }
    }

    /**
     * Create the directory and all required parent directories.
     *
     * @param dir the directory name
     */
    public static void createDirectories(String dir) {
        if (dir != null) {
            if (exists(dir)) {
                if (!isDirectory(dir)) {
                    // this will fail
                    createDirectory(dir);
                }
            } else {
                String parent = getParent(dir);
                createDirectories(parent);
                createDirectory(dir);
            }
        }
    }

    /**
     * Try to delete a file (ignore errors).
     *
     * @param fileName the file name
     * @return true if it worked
     */
    public static boolean tryDelete(String fileName) {
        try {
            logger.debug("try to delete " + fileName);
            FilePath.get(fileName).delete();
            logger.debug("delete " + fileName + " success");
            return true;
        } catch (Exception e) {
            logger.debug("delete " + fileName + " failed");
            return false;
        }
    }

    /**
     * Create a new temporary file.
     *
     * @param prefix       the prefix of the file name (including directory name if
     *                     required)
     * @param suffix       the suffix
     * @param deleteOnExit if the file should be deleted when the virtual machine exists
     * @return the name of the created file
     */
    public static String createTempFile(String prefix, String suffix, boolean deleteOnExit) throws IOException {
        return FilePath.get(prefix).createTempFile(suffix, deleteOnExit).toString();
    }

    /**
     * Fully read from the file. This will read all remaining bytes, or throw an
     * EOFException if not successful.
     *
     * @param channel the file channel
     * @param dst     the byte buffer
     */
    public static void readFully(FileChannel channel, ByteBuffer dst) throws IOException {
        do {
            int r = channel.read(dst);
            if (r < 0) {
                throw new EOFException();
            }
        } while (dst.remaining() > 0);
    }

    /**
     * Fully writeDirectly to the file. This will writeDirectly all remaining bytes.
     *
     * @param channel the file channel
     * @param src     the byte buffer
     */
    public static void writeFully(FileChannel channel, ByteBuffer src) throws IOException {
        do {
            channel.write(src);
        } while (src.remaining() > 0);
    }

    /**
     * copy
     *
     * @param source
     * @param dest
     * @throws IOException
     */
    public static void copy(File source, File dest) throws IOException {
        if (null == source || null == dest) {
            return;
        }
        FileChannel inputChannel = null;
        FileChannel outputChannel = null;
        FileInputStream fileInputStream = null;
        FileOutputStream fileOutputStream = null;
        try {
            fileInputStream = new FileInputStream(source);
            fileOutputStream = new FileOutputStream(dest);
            inputChannel = fileInputStream.getChannel();
            outputChannel = fileOutputStream.getChannel();
            if (null == inputChannel || null == outputChannel) {
                return;
            }
            outputChannel.transferFrom(inputChannel, 0, inputChannel.size());
        } finally {
            if (null != fileInputStream) {
                fileInputStream.close();
            }
            if (null != fileOutputStream) {
                fileOutputStream.close();
            }
            if (null != inputChannel) {
                inputChannel.close();
            }
            if (null != outputChannel) {
                outputChannel.close();
            }
        }
    }

}
