/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.manager;

import com.actiontech.dble.manager.handler.dump.Dispatcher;
import com.actiontech.dble.manager.handler.dump.DumpFileReader;
import com.actiontech.dble.manager.handler.dump.DumpFileWriter;
import org.junit.Test;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class SplitDumpHandlerTest {

    @Test
    public void readFile() {

        BlockingQueue<String> queue = new ArrayBlockingQueue<>(100);

        DumpFileReader reader = new DumpFileReader(queue);
        reader.setFileName("/Users/baofengqi/Desktop/nio:vote.ddl.sql");
        new Thread(reader).start();

        DumpFileWriter writer = new DumpFileWriter();

        Dispatcher dispatcher = new Dispatcher(queue, writer);
        new Thread(dispatcher).start();

        writer.run();
    }
}
