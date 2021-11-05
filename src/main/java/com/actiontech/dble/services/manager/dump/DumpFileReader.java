package com.actiontech.dble.services.manager.dump;

import com.actiontech.dble.backend.mysql.store.fs.FileUtils;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.singleton.TraceManager;
import com.actiontech.dble.util.NameableExecutor;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.regex.Pattern;

/**
 * @author Baofengqi
 */
public final class DumpFileReader {

    public static final Logger LOGGER = LoggerFactory.getLogger("dumpFileLog");
    public static final String EOF = "dump file eof";
    public static final Pattern CREATE_VIEW = Pattern.compile("CREATE\\s+VIEW\\s+`?([a-zA-Z_0-9\\-_]+)`?\\s+", Pattern.CASE_INSENSITIVE);
    public static final Pattern CREATE_VIEW1 = Pattern.compile("CREATE\\s+ALGORITHM", Pattern.CASE_INSENSITIVE);
    private FileChannel fileChannel;
    private long fileLength;
    private long readLength;
    private int readPercent;
    private NameableExecutor nameableExecutor;
    private BlockingQueue<String> handleQueue;

    public DumpFileReader(BlockingQueue<String> handleQueue) {
        this.handleQueue = handleQueue;
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
                String stmt = new String(buffer.array(), 0, byteRead, StandardCharsets.UTF_8);
                this.handleQueue.put(stmt);
                buffer.clear();
                byteRead = fileChannel.read(buffer);
            }
            this.handleQueue.put(EOF);
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

    public static List<String> splitContent(String content, String separate) {
        List<String> list = Lists.newArrayList();
        while (true) {
            int j = content.indexOf(separate);
            if (j < 0) {
                if (!content.isEmpty() && !content.trim().isEmpty()) {
                    list.add(content);
                }
                break;
            }
            list.add(content.substring(0, j));
            content = content.substring(j + separate.length());
        }
        if (list.isEmpty()) {
            list.add(content);
        }
        return list;
    }

}
