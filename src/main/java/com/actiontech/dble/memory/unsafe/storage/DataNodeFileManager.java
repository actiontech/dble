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

package com.actiontech.dble.memory.unsafe.storage;


import com.actiontech.dble.memory.unsafe.utils.JavaUtils;
import com.actiontech.dble.memory.unsafe.utils.ServerPropertyConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * Creates and maintains the logical mapping between logical blocks and physical on-disk
 * locations. One block is mapped to one file with a name given by its BlockId.
 * <p>
 * Block files are hashed among the directories listed in local.dir
 */
public class DataNodeFileManager {
    private static final Logger LOG = LoggerFactory.getLogger(DataNodeFileManager.class);
    private boolean deleteFilesOnStop;
    /**
     * TODO: delete tmp file
     */
    // The content of subDirs is immutable but the content of subDirs(i) is mutable. And the content
    // of subDirs(i) is protected by the lock of subDirs(i)
    // private val shutdownHook ;
    /* Create one local directory for each path mentioned in spark.local.dir; then, inside this
     * directory, create multiple subdirectories that we will hash files into, in order to avoid
     * having really large inodes at the top level. */

    private List<File> localDirs;
    private int subDirsPerLocalDir;

    private ConcurrentMap<Integer, ArrayList<File>> subDirs;


    public DataNodeFileManager(ServerPropertyConf conf, boolean deleteFilesOnStop) throws IOException {

        this.deleteFilesOnStop = deleteFilesOnStop;


        subDirsPerLocalDir = conf.getInt("server.diskStore.subDirectories", 64);
        localDirs = createLocalDirs(conf);
        subDirs = new ConcurrentHashMap<>(localDirs.size());


        for (int i = 0; i < localDirs.size(); i++) {
            ArrayList<File> list = new ArrayList<>(subDirsPerLocalDir);

            for (int j = 0; j < subDirsPerLocalDir; j++) {
                list.add(i, null);
            }

            subDirs.put(i, list);
        }

    }

    /**
     * Produces a unique block id and File suitable for storing local intermediate results.
     */
    public TempDataNodeId createTempLocalBlock() throws IOException {
        TempDataNodeId blockId = new TempDataNodeId(UUID.randomUUID().toString());

        while (getFile(blockId).exists()) {
            blockId = new TempDataNodeId(UUID.randomUUID().toString());
        }

        return blockId;
    }


    /**
     * Looks up a file by hashing it into one of our local subdirectories.
     */
    // This method should be kept in sync with
    // org.apache.spark.network.shuffle.ExternalShuffleBlockResolver#getFile().
    public File getFile(String filename) throws IOException {
        // Figure out which local directory it hashes to, and which subdirectory in that
        int hash = JavaUtils.nonNegativeHash(filename);
        int dirId = hash % localDirs.size();
        int subDirId = (hash / localDirs.size()) % subDirsPerLocalDir;

        synchronized (this) {
            File file = subDirs.get(dirId).get(subDirId);
            if (file != null) {
                LOG.warn(file.getName() + " exist !");
            } else {
                file = new File(localDirs.get(dirId), String.valueOf(subDirId));
                if (!file.exists() && !file.mkdir()) {
                    throw new IOException("Failed to create local dir in $newDir.");
                }
                subDirs.get(dirId).add(subDirId, file);
            }
        }

        return new File(subDirs.get(dirId).get(subDirId), filename);
    }

    public File getFile(ConnectionId connid) throws IOException {
        return getFile(connid.name);
    }

    /**
     * TODO config root
     * Create local directories for storing block data. These directories are
     * located inside configured local directories and won't
     * be deleted on JVM exit when using the external shuffle service.
     */
    private List<File> createLocalDirs(ServerPropertyConf conf) {

        String rootDirs = conf.getString("server.local.dirs", "datanode");

        String[] rdir = rootDirs.split(",");
        List<File> dirs = new ArrayList<>();
        for (String aRdir : rdir) {
            try {
                File localDir = JavaUtils.createDirectory(aRdir, "datenode");
                dirs.add(localDir);
            } catch (Exception e) {
                LOG.error("Failed to create local dir in " + aRdir + ". Ignoring this directory.");
            }
        }

        if (dirs.isEmpty()) {
            throw new RuntimeException("can't createLocalDirs in " + rootDirs);
        }
        return dirs;
    }

    /**
     * Cleanup local dirs and stop shuffle sender.
     */
    public void stop() {
        doStop();
    }

    private void doStop() {
        if (deleteFilesOnStop) {
            File localDir;
            int i = 0;
            System.out.println(localDirs.size());
            while (i < localDirs.size() && localDirs.size() > 0) {
                localDir = localDirs.get(i);
                //System.out.println(localDir);
                if (localDir.isDirectory() && localDir.exists()) {
                    try {
                        JavaUtils.deleteRecursively(localDir);
                    } catch (Exception e) {
                        LOG.error(e.getMessage());
                    }
                }
                i++;
            }
        }
    }
}
