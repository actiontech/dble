package com.actiontech.dble.manager.handler.dump;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.store.fs.FileUtils;
import com.actiontech.dble.manager.handler.dump.type.DumpContent;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.LockSupport;

public class DumpFileWriter implements Runnable {

    public static final Logger LOGGER = LoggerFactory.getLogger(DumpFileWriter.class);

    private BlockingQueue<DumpContent> queue;
    private Map<String, FileWriter> dataNodeWriters = new HashMap<>();
    private FileWriter errorFile;
    private List<String> allDataNodes;

    public DumpFileWriter() {
        this.queue = new ArrayBlockingQueue<>(10);
        this.allDataNodes = new ArrayList<>(DbleServer.getInstance().getConfig().getDataNodes().keySet());
        for (String dataNode : this.allDataNodes) {
            dataNodeWriters.put(dataNode, new FileWriter("/tmp/" + dataNode + ".dump"));
        }
        this.errorFile = new FileWriter("/tmp/error.log");
    }

    public void write(DumpContent content) {
        this.queue.offer(content);
    }

    public void writeError(String error) {
        try {
            errorFile.write(error.getBytes(), 0, error.getBytes().length);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        DumpContent content = null;
        FileWriter writer = null;
        while (true) {
            if (content == null) {
                try {
                    content = queue.take();
                } catch (InterruptedException e) {
                    writeError(e.getMessage());
                    return;
                }
            } else if (content.canDump()) {
                Collection<String> dataNodes = content.getDataNodes();
                if (dataNodes == null) {
                    dataNodes = allDataNodes;
                }
                for (String dataNode : dataNodes) {
                    writer = dataNodeWriters.get(dataNode);
                    String str;
                    while (content.hasNext()) {
                        str = content.get(dataNode);
                        if (writer != null && !StringUtil.isEmpty(str)) {
                            try {
                                byte[] ss = (str + ";").getBytes(StandardCharsets.UTF_8);
                                writer.write(ss, 0, ss.length);
                            } catch (IOException e) {
                                writeError(e.getMessage());
                            }
                        }
                    }
                }

                if (content.isFooter()) {
                    try {
                        flush();
                    } catch (IOException e) {
                        writeError(e.getMessage());
                    }
                    return;
                }
                content = null;
            }
            LockSupport.parkNanos(100);
        }
    }

    void flush() throws IOException {
        for (FileWriter writer : this.dataNodeWriters.values()) {
            writer.flush();
            writer.close();
        }
        errorFile.flush();
        errorFile.close();
    }

    class FileWriter {
        private FileChannel fileChannel;
        private ByteBuffer byteBuffer;

        FileWriter(String fileName) {
            try {
                this.fileChannel = FileUtils.open(fileName, "rw");
            } catch (IOException e) {
                e.printStackTrace();
                this.fileChannel = null;
            }
            this.byteBuffer = ByteBuffer.allocate(0x10000);
        }

        void write(byte[] stmt, int src, int len) throws IOException {
            int remaining = byteBuffer.remaining();
            if (remaining >= len) {
                byteBuffer.put(stmt, src, len);
            } else {
                byteBuffer.put(stmt, src, remaining);
                fileChannel.write(byteBuffer);
                byteBuffer.clear();
                write(stmt, remaining, len - remaining);
            }
        }

        void flush() throws IOException {
            fileChannel.write(byteBuffer);
        }

        void close() throws IOException {
            fileChannel.close();
            if (byteBuffer.isDirect()) {
                ((DirectBuffer) byteBuffer).cleaner().clean();
            }
        }
    }
}
