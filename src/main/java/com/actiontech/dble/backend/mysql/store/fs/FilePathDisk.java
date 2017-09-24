/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.store.fs;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.util.ResourceUtil;
import com.actiontech.dble.util.exception.TmpFileException;

import java.io.*;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.NonWritableChannelException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * @author ActionTech
 * @CreateTime 2014-9-8
 */
public class FilePathDisk extends FilePath {

    private static final String CLASSPATH_PREFIX = "classpath:";

    @Override
    public FileChannel open(String mode) throws IOException {
        FileDisk f;
        try {
            f = new FileDisk(name, mode);
        } catch (IOException e) {
            freeMemoryAndFinalize();
            try {
                f = new FileDisk(name, mode);
            } catch (IOException e2) {
                throw e;
            }
        }
        return f;
    }

    @Override
    public String getScheme() {
        return "file";
    }

    @Override
    public FilePath getPath(String path) {
        FilePathDisk p = new FilePathDisk();
        p.name = translateFileName(path);
        return p;
    }

    @Override
    public long size() {
        return new File(name).length();
    }

    /**
     * Translate the file name to the native format. This will replace '\' with
     * '/'.
     *
     * @param fileName the file name
     * @return the native file name
     */
    protected String translateFileName(String fileName) {
        fileName = fileName.replace('\\', '/');
        if (fileName.startsWith("file:")) {
            fileName = fileName.substring("file:".length());
        }
        return fileName;
    }

    @Override
    public void moveTo(FilePath newName) {
        File oldFile = new File(name);
        File newFile = new File(newName.name);
        if (oldFile.getAbsolutePath().equals(newFile.getAbsolutePath())) {
            return;
        }
        if (!oldFile.exists()) {
            throw TmpFileException.get(ErrorCode.ER_FILE_RENAME, name + " (not found)", newName.name);
        }
        if (newFile.exists()) {
            throw TmpFileException.get(ErrorCode.ER_FILE_RENAME, name, newName + " (exists)");
        }
        for (int i = 0; i < 2; i++) {
            boolean ok = oldFile.renameTo(newFile);
            if (ok) {
                return;
            }
            wait(i);
        }
        throw TmpFileException.get(ErrorCode.ER_FILE_RENAME, new String[]{name, newName.name});
    }

    protected void wait(int i) {
        if (i == 8) {
            System.gc();
        }
        // sleep at most 256 ms
        long sleep = Math.min(256, i * i);
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(sleep));
    }

    @Override
    public boolean createFile() {
        File file = new File(name);
        for (int i = 0; i < 2; i++) {
            try {
                return file.createNewFile();
            } catch (IOException e) {
                // 'access denied' is really a concurrent access problem
                wait(i);
            }
        }
        return false;
    }

    @Override
    public boolean exists() {
        return new File(name).exists();
    }

    @Override
    public void delete() {
        File file = new File(name);
        for (int i = 0; i < 2; i++) {
            boolean ok = file.delete();
            if (ok || !file.exists()) {
                return;
            }
            wait(i);
        }
        throw TmpFileException.get(ErrorCode.ER_FILE_DELETE, name);
    }

    @Override
    public List<FilePath> newDirectoryStream() {
        ArrayList<FilePath> list = new ArrayList<>();
        File f = new File(name);
        try {
            String[] files = f.list();
            if (files != null) {
                String base = f.getCanonicalPath();
                for (String file : files) {
                    list.add(getPath(base + file));
                }
            }
            return list;
        } catch (IOException e) {
            throw TmpFileException.convertIOException(ErrorCode.ER_IO_EXCEPTION, e, name);
        }
    }

    @Override
    public boolean canWrite() {
        return canWriteInternal(new File(name));
    }

    @Override
    public boolean setReadOnly() {
        File f = new File(name);
        return f.setReadOnly();
    }

    @Override
    public FilePath toRealPath() {
        try {
            String fileName = new File(name).getCanonicalPath();
            return getPath(fileName);
        } catch (IOException e) {
            throw TmpFileException.convertIOException(ErrorCode.ER_IO_EXCEPTION, e, name);
        }
    }

    @Override
    public FilePath getParent() {
        String p = new File(name).getParent();
        return p == null ? null : getPath(p);
    }

    @Override
    public boolean isDirectory() {
        return new File(name).isDirectory();
    }

    @Override
    public boolean isAbsolute() {
        return new File(name).isAbsolute();
    }

    @Override
    public long lastModified() {
        return new File(name).lastModified();
    }

    private static boolean canWriteInternal(File file) {
        try {
            if (!file.canWrite()) {
                return false;
            }
        } catch (Exception e) {
            // workaround for GAE which throws a
            // java.security.AccessControlException
            return false;
        }
        // File.canWrite() does not respect windows user permissions,
        // so we must try to open it using the mode "rw".
        // See also http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4420020
        RandomAccessFile r = null;
        try {
            r = new RandomAccessFile(file, "rw");
            return true;
        } catch (FileNotFoundException e) {
            return false;
        } finally {
            if (r != null) {
                try {
                    r.close();
                } catch (IOException e) {
                    //ignore error
                }
            }
        }
    }

    @Override
    public void createDirectory() {
        File dir = new File(name);
        for (int i = 0; i < 2; i++) {
            if (dir.exists()) {
                if (dir.isDirectory()) {
                    return;
                }
                throw TmpFileException.get(ErrorCode.ER_FILE_CREATE, name + " (a file with this name is already exists)");
            } else if (dir.mkdir()) {
                return;
            }
            wait(i);
        }
        throw TmpFileException.get(ErrorCode.ER_FILE_CREATE, name);
    }

    @Override
    public OutputStream newOutputStream(boolean append) throws IOException {
        try {
            File file = new File(name);
            File parent = file.getParentFile();
            if (parent != null) {
                FileUtils.createDirectories(parent.getAbsolutePath());
            }
            FileOutputStream out = new FileOutputStream(name, append);
            return out;
        } catch (IOException e) {
            freeMemoryAndFinalize();
            return new FileOutputStream(name);
        }
    }

    @Override
    public InputStream newInputStream() throws IOException {
        int index = name.indexOf(':');
        if (index > 1 && index < 20) {
            // if the ':' is in position 1, a windows file access is assumed:
            // C:.. or D:, and if the ':' is not at the beginning, assume its a
            // file name with a colon
            if (name.startsWith(CLASSPATH_PREFIX)) {
                String fileName = name.substring(CLASSPATH_PREFIX.length());
                if (!fileName.startsWith("/")) {
                    fileName = "/" + fileName;
                }
                InputStream in = ResourceUtil.getResourceAsStream(fileName);
                if (in == null) {
                    in = ResourceUtil.getResourceAsStreamForCurrentThread(fileName);
                }
                if (in == null) {
                    throw new FileNotFoundException("resource " + fileName);
                }
                return in;
            }
            // otherwise an URL is assumed
            URL url = new URL(name);
            InputStream in = url.openStream();
            return in;
        }
        FileInputStream in = new FileInputStream(name);
        return in;
    }

    /**
     * Call the garbage collection and run finalization. This close all files
     * that were not closed, and are no longer referenced.
     */
    static void freeMemoryAndFinalize() {
        Runtime rt = Runtime.getRuntime();
        long mem = rt.freeMemory();
        for (int i = 0; i < 16; i++) {
            rt.gc();
            long now = rt.freeMemory();
            rt.runFinalization();
            if (now == mem) {
                break;
            }
            mem = now;
        }
    }

    @Override
    public FilePath createTempFile(String suffix, boolean deleteOnExit) throws IOException {
        String fileName = name + ".";
        File dir = new File(fileName).getAbsoluteFile().getParentFile();
        FileUtils.createDirectories(dir.getAbsolutePath());
        return super.createTempFile(suffix, deleteOnExit);
    }

    /**
     * Uses java.io.RandomAccessFile to access a file.
     */
    static class FileDisk extends FileBase {

        private final RandomAccessFile file;
        private final String name;
        private final boolean readOnly;

        FileDisk(String fileName, String mode) throws FileNotFoundException {
            this.file = new RandomAccessFile(fileName, mode);
            this.name = fileName;
            this.readOnly = mode.equals("r");
        }

        @Override
        public void force(boolean metaData) throws IOException {
            file.getChannel().force(metaData);
        }

        @Override
        public FileChannel truncate(long newLength) throws IOException {
            // compatibility with JDK FileChannel#truncate
            if (readOnly) {
                throw new NonWritableChannelException();
            }
            if (newLength < file.length()) {
                file.setLength(newLength);
            }
            return this;
        }

        @Override
        public synchronized FileLock tryLock(long position, long size, boolean shared) throws IOException {
            return file.getChannel().tryLock(position, size, shared);
        }

        @Override
        public void implCloseChannel() throws IOException {
            file.close();
        }

        @Override
        public long position() throws IOException {
            return file.getFilePointer();
        }

        @Override
        public FileChannel position(long pos) throws IOException {
            file.seek(pos);
            return this;
        }

        @Override
        public long size() throws IOException {
            return file.length();
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            int len = file.read(dst.array(), dst.arrayOffset() + dst.position(), dst.remaining());
            if (len > 0) {
                dst.position(dst.position() + len);
            }
            return len;
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            int len = src.remaining();
            file.write(src.array(), src.arrayOffset() + src.position(), len);
            src.position(src.position() + len);
            return len;
        }

        @Override
        public String toString() {
            return name;
        }

    }
}
