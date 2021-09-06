package com.actiontech.dble.services.manager.dump;

import com.actiontech.dble.backend.mysql.store.fs.FileUtils;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.services.manager.dump.handler.InsertHandler;
import com.actiontech.dble.singleton.TraceManager;
import com.actiontech.dble.util.NameableExecutor;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.LockSupport;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Baofengqi
 */
public final class DumpFileReader {

    public static final Logger LOGGER = LoggerFactory.getLogger("dumpFileLog");
    public static final String EOF = "dump file eof";
    public static final Pattern CREATE_VIEW = Pattern.compile("CREATE\\s+VIEW\\s+`?([a-zA-Z_0-9\\-_]+)`?\\s+", Pattern.CASE_INSENSITIVE);
    public static final Pattern CREATE_VIEW1 = Pattern.compile("CREATE\\s+ALGORITHM", Pattern.CASE_INSENSITIVE);
    private StringBuilder tempStr = new StringBuilder(200);
    private final BlockingQueue<String> ddlQueue;
    private final BlockingQueue<String> insertQueue;
    private FileChannel fileChannel;
    private long fileLength;
    private long readLength;
    private int readPercent;
    private NameableExecutor nameableExecutor;

    public DumpFileReader(BlockingQueue<String> queue, BlockingQueue<String> insertQueue) {
        this.ddlQueue = queue;
        this.insertQueue = insertQueue;
    }

    public void open(String fileName) throws IOException {
        this.fileChannel = FileUtils.open(fileName, "r");
        this.fileLength = this.fileChannel.size();
    }

    public void start(ManagerService service, NameableExecutor executor) throws IOException, InterruptedException {
        this.nameableExecutor = executor;
        LOGGER.info("begin to read dump file.");
        TraceManager.TraceObject traceObject = TraceManager.threadTrace("dump-file-read");
        try {
            ByteBuffer buffer = ByteBuffer.allocate(0x20000);
            int byteRead = fileChannel.read(buffer);
            while (byteRead != -1) {
                if (service.getConnection().isClosed()) {
                    LOGGER.info("finish to read dump file, because the connection is closed.");
                    nameableExecutor.shutdownNow();
                    return;
                }
                readLength += byteRead;
                float percent = ((float) readLength / (float) fileLength) * 100;
                if (((int) percent) - readPercent > 5 || (int) percent == 100) {
                    readPercent = (int) percent;
                    LOGGER.info("dump file has bean read " + readPercent + "%");
                }
                readSQLByEOF(buffer.array(), byteRead);
                buffer.clear();
                byteRead = fileChannel.read(buffer);
            }
            if (null != tempStr && !StringUtil.isBlank(tempStr.toString())) {
                putSql(tempStr.toString());
                this.tempStr = null;
            }
            putSql(EOF);
        } finally {
            TraceManager.finishSpan(traceObject);
            try {
                if (fileChannel != null) {
                    fileChannel.close();
                }
            } catch (IOException e) {
                // ignore
                LOGGER.warn("close dump file error:" + e.getMessage());
            }
        }
    }

    // read one statement by ;
    private void readSQLByEOF(byte[] linesByte, int byteRead) throws InterruptedException {
        String stmts = new String(linesByte, 0, byteRead, StandardCharsets.UTF_8);
        boolean endWithEOF = stmts.endsWith(";") || stmts.endsWith(";\n");
        String[] lines = stmts.split(";\\r?\\n");
        int len = lines.length - 1;

        int i = 0;
        if (len > 0 && tempStr != null && !StringUtil.isBlank(tempStr.toString())) {
            tempStr.append(lines[0]);
            putSql(tempStr.toString());
            tempStr = null;
            i = 1;
        }

        for (; i < len; i++) {
            if (!StringUtil.isBlank(lines[i])) {
                putSql(lines[i]);
            }
        }

        if (!endWithEOF) {
            if (tempStr == null) {
                tempStr = new StringBuilder(lines[len]);
            } else {
                tempStr.append(lines[len]);
            }
        } else {
            if (tempStr != null && !StringUtil.isBlank(tempStr.toString())) {
                tempStr.append(lines[len]);
                putSql(tempStr.toString());
                tempStr = null;
            } else {
                if (!StringUtil.isBlank(lines[len])) {
                    putSql(lines[len]);
                }
            }
        }
    }

    public void putSql(String sql) throws InterruptedException {
        if (StringUtil.isBlank(sql)) {
            return;
        }
        Matcher matcher = InsertHandler.INSERT_STMT.matcher(sql);
        if (matcher.find()) {
            while (!this.ddlQueue.isEmpty() && !this.nameableExecutor.isShutdown()) {
                LockSupport.parkNanos(1000);
            }
            this.insertQueue.put(sql);
        } else {
            while (!this.insertQueue.isEmpty() && !this.nameableExecutor.isShutdown()) {
                LockSupport.parkNanos(1000);
            }
            this.ddlQueue.put(sql);
        }
    }
}
