/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.store;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.store.fs.FilePath;
import com.actiontech.dble.backend.mysql.store.fs.FileUtils;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.util.exception.TmpFileException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.List;

public class FileStore {
    private static final String SUFFIX_TEMP_FILE = ".temp.db";
    private static Logger logger = Logger.getLogger(FileStore.class);

    /**
     * The file path name.
     */
    protected String name;

    private List<String> fileNames;
    private List<FileChannel> files;
    private long filePos;
    private long fileLength;
    private final String mode;
    private final int mappedFileSize;
    private List<FileLock> locks;

    /**
     * Create a new file using the given settings.
     *
     * @param handler the callback object
     * @param name    the file name
     * @param mode    the access mode ("r", "rw", "rws", "rwd")
     */
    public FileStore(String name, String mode) {
        this.name = name;
        this.fileNames = new ArrayList<>();
        this.files = new ArrayList<>();
        this.locks = new ArrayList<>();
        this.mode = mode;
        this.mappedFileSize = DbleServer.getInstance().getConfig().getSystem().getMappedFileSize();
        try {
            createFile();
        } catch (IOException e) {
            throw TmpFileException.get(ErrorCode.ER_FILE_INIT, e, name);
        }
    }

    /**
     * Close the file.
     */
    public void close() {
        if (!this.files.isEmpty()) {
            for (FileChannel file : this.files) {
                try {
                    file.close();
                } catch (IOException e) {
                    logger.warn("close file error :", e);
                } finally {
                    // QUESTION_TODO if IOException,memory is release or not
                    FileCounter.getInstance().decrement();
                }
            }
            this.files.clear();
        }
    }

    /**
     * Close the file without throwing any exceptions. Exceptions are simply
     * ignored.
     */
    public void closeSilently() {
        try {
            close();
        } catch (Exception e) {
            //ignore error
        }
    }

    public void delete() {
        if (!this.fileNames.isEmpty()) {
            try {
                for (String fileName : this.fileNames) {
                    FileUtils.tryDelete(fileName);
                }
            } finally {
                this.fileNames.clear();
            }
        }
    }

    /**
     * Close the file (ignoring exceptions) and delete the file.
     */
    public void closeAndDeleteSilently() {
        if (!this.files.isEmpty()) {
            closeSilently();
            delete();
        }
        name = null;
    }

    /**
     * Read a number of bytes without decrypting.
     *
     * @param b   the target buffer
     * @param off the offset
     * @param len the number of bytes to read
     */
    protected void readFullyDirect(byte[] b, int off, int len) {
        readFully(b, off, len);
    }

    /**
     * Read a number of bytes.
     *
     * @param b   the target buffer
     * @param off the offset
     * @param len the number of bytes to read
     */
    public void readFully(byte[] b, int off, int len) {
        ByteBuffer buffer = ByteBuffer.wrap(b, off, len);
        read(buffer);
    }

    private int read(ByteBuffer buffer) {
        int len = 0;
        try {
            do {
                int index = (int) (filePos / mappedFileSize);
                long offset = filePos % mappedFileSize;
                if (index > files.size() - 1)
                    throw TmpFileException.get(ErrorCode.ER_FILE_READ, name);
                files.get(index).position(offset);
                int r = files.get(index).read(buffer);
                len += r;
                filePos += r;
                if (filePos >= fileLength - 1)
                    break;
            } while (buffer.hasRemaining());
        } catch (IOException e) {
            throw TmpFileException.get(ErrorCode.ER_FILE_READ, e, name);
        }
        return len;
    }

    public int read(ByteBuffer buffer, long endPos) {
        long remained = endPos - filePos;
        if (remained <= 0)
            return 0;

        if (remained < buffer.remaining()) {
            int newLimit = (int) (buffer.position() + remained);
            buffer.limit(newLimit);
        }
        return read(buffer);
    }

    /**
     * Go to the specified file location.
     *
     * @param pos the location
     */
    public void seek(long pos) {
        filePos = pos;
    }

    /**
     * Write a number of bytes without encrypting.
     *
     * @param b   the source buffer
     * @param off the offset
     * @param len the number of bytes to write
     */
    protected void writeDirect(byte[] b, int off, int len) {
        write(b, off, len);
    }

    /**
     * Write a number of bytes.
     *
     * @param b   the source buffer
     * @param off the offset
     * @param len the number of bytes to write
     */
    public void write(byte[] b, int off, int len) {
        write(ByteBuffer.wrap(b, off, len));
    }

    public void write(ByteBuffer buffer) {
        try {
            do {
                int index = (int) (filePos / mappedFileSize);
                if (index > files.size() - 1) {
                    createFile();
                }
                long offset = filePos % mappedFileSize;
                files.get(index).position(offset);
                int w = files.get(index).write(buffer);
                filePos += w;
            } while (buffer.remaining() > 0);
        } catch (IOException e) {
            closeFileSilently();
            throw TmpFileException.get(ErrorCode.ER_FILE_WRITE, e, name);
        }
        fileLength = Math.max(filePos, fileLength);
    }

    private void createFile() throws IOException {
        String newName = name;
        int index = newName.indexOf(':');
        String scheme = newName.substring(0, index);
        if (!FileCounter.getInstance().increament() && "nioMapped".equals(scheme)) {
            newName = "nio:Disk";
        }
        try {
            FilePath path = FilePath.get(newName).createTempFile(SUFFIX_TEMP_FILE, true);
            this.fileNames.add(path.toString());
            this.files.add(path.open(mode));
        } catch (IOException e) {
            if (e.getCause() instanceof OutOfMemoryError) {
                logger.info("no memory to mapped file,change to disk file");
                // memory is used by other user
                FileCounter.getInstance().decrement();
                newName = "nio:Disk";
                FilePath path = FilePath.get(newName).createTempFile(SUFFIX_TEMP_FILE, true);
                this.files.add(path.open(mode));
                this.fileNames.add(path.toString());
            } else {
                logger.warn("create file error :", e);
                throw e;
            }
        }
    }

    /**
     * Get the file size in bytes.
     *
     * @return the file size
     */
    public long length() {
        return fileLength;
    }

    /**
     * Get the current location of the file pointer.
     *
     * @return the location
     */
    public long getFilePointer() {
        return filePos;
    }

    public void force(boolean metaData) {
        try {
            for (FileChannel file : this.files) {
                file.force(metaData);
            }
        } catch (IOException e) {
            closeFileSilently();
            throw TmpFileException.get(ErrorCode.ER_FILE_FORCE, e, name);
        }
    }

    /**
     * Call fsync. Depending on the operating system and hardware, this may or
     * may not in fact write the changes.
     */
    public void sync() {
        try {
            for (FileChannel file : this.files) {
                file.force(true);
            }
        } catch (IOException e) {
            closeFileSilently();
            throw TmpFileException.get(ErrorCode.ER_FILE_SYNC, e, name);
        }
    }

    /**
     * Close the file.
     */
    public void closeFile() throws IOException {
        for (FileChannel file : this.files) {
            file.close();
        }
    }

    /**
     * Just close the file, without setting the reference to null. This method
     * is called when writing failed. The reference is not set to null so that
     * there are no NullPointerExceptions later on.
     */
    private void closeFileSilently() {
        try {
            closeFile();
        } catch (IOException e) {
            //ignore error
        }
    }

    /**
     * Re-open the file. The file pointer will be reset to the previous
     * location.
     */
    public void openFile() throws IOException {
        if (this.files.isEmpty()) {
            for (String fileName : fileNames) {
                this.files.add(FilePath.get(fileName).open(mode));
            }
        }
    }

    /**
     * Try to lock the file.
     *
     * @return true if successful
     */
    public synchronized boolean tryLock() {
        boolean isLocked = true;
        try {
            for (FileChannel file : this.files) {
                FileLock lock = file.tryLock();
                if (lock == null) {
                    isLocked = false;
                    break;
                }
                locks.add(lock);
            }
        } catch (Exception e) {
            // ignore OverlappingFileLockException
            return false;
        } finally {
            if (!isLocked) {
                for (FileLock lock : locks) {
                    try {
                        lock.release();
                    } catch (IOException e) {
                        //ignore error
                    }
                }
            }
        }
        return isLocked;
    }

    /**
     * Release the file lock.
     */
    public synchronized void releaseLock() {
        if (!files.isEmpty() && !locks.isEmpty()) {
            for (FileLock lock : locks) {
                try {
                    lock.release();
                } catch (IOException e) {
                    //ignore error
                }
            }
            locks.clear();
        }
    }
}
