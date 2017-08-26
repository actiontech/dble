package io.mycat.util.dataMigrator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


/**
 * DataMigrator
 * export from the old severs to files,and import to new servers
 *
 * @author haonan108
 */
public class DataMigrator {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataMigrator.class);

    private static DataMigratorArgs margs;

    private List<TableMigrateInfo> migrateTables;

    private ExecutorService executor;

    private List<DataNodeClearGroup> clearGroup = new ArrayList<>();

    public DataMigrator(String[] args) {
        margs = new DataMigratorArgs(args);
        executor = new ThreadPoolExecutor(margs.getThreadCount(), margs.getThreadCount(),
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(), new ThreadPoolExecutor.CallerRunsPolicy());


        try {
            createTempParentDir(margs.getTempFileDir());
            ConfigComparer loader = new ConfigComparer(margs.isAwaysUseMaster());
            migrateTables = loader.getMigratorTables();
            //create tables
            for (TableMigrateInfo table : migrateTables) {
                table.setTableStructure();
                table.createTableToNewDataNodes();
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            System.out.println(e.getMessage());
            System.exit(-1);
        }
    }

    public static void main(String[] args) throws SQLException {
        final long start = System.currentTimeMillis();
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
        System.out.println("\n" + format.format(new Date()) + " [1]-> creating migrator schedule and temp files for migrate...");
        //init
        DataMigrator migrator = new DataMigrator(args);

        //generate the tnp files
        migrator.createTempFiles();
        migrator.changeSize();
        migrator.printInfo();

        //migrate
        System.out.println("\n" + format.format(new Date()) + " [2]-> start migrate data...");
        migrator.migrateData();

        //cleaning redundant data
        System.out.println("\n" + format.format(new Date()) + " [3]-> cleaning redundant data...");
        migrator.clear();

        //validating tables migrate result
        System.out.println("\n" + format.format(new Date()) + " [4]-> validating tables migrate result...");
        migrator.validate();
        migrator.clearTempFiles();
        long end = System.currentTimeMillis();
        System.out.println("\n" + format.format(new Date()) + " migrate data complete in " + (end - start) + "ms");
    }

    public static DataMigratorArgs getMargs() {
        return margs;
    }

    public static void setMargs(DataMigratorArgs margs) {
        DataMigrator.margs = margs;
    }

    private void printInfo() {
        for (TableMigrateInfo table : migrateTables) {
            table.printMigrateInfo();
            table.printMigrateSchedule();
        }
    }

    //clearTempFiles
    private void clearTempFiles() {
        File tempFileDir = new File(margs.getTempFileDir());
        if (tempFileDir.exists() && margs.isDeleteTempDir()) {
            DataMigratorUtil.deleteDir(tempFileDir);
        }
    }

    //generate file according to the partition column
    private void createTempFiles() {
        for (TableMigrateInfo table : migrateTables) {
            createTableTempFiles(table);
        }
        executor.shutdown();
        while (true) {
            if (executor.isTerminated()) {
                break;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                LOGGER.error("error", e);
            }
        }
    }

    private void migrateData() throws SQLException {
        executor = new ThreadPoolExecutor(margs.getThreadCount(), margs.getThreadCount(),
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(), new ThreadPoolExecutor.CallerRunsPolicy());
        for (TableMigrateInfo table : migrateTables) {
            if (!table.isError()) { //ignore the error table
                List<DataNodeMigrateInfo> detailList = table.getDataNodesDetail();
                for (DataNodeMigrateInfo info : detailList) {
                    executor.execute(new DataMigrateRunner(table, info.getSrc(), info.getTarget(), table.getTableName(), info.getTempFile()));
                }
            }
        }
        executor.shutdown();
        while (true) {
            if (executor.isTerminated()) {
                break;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                LOGGER.error("error", e);
            }
        }
    }

    // recalculate the size
    private void changeSize() throws SQLException {
        for (TableMigrateInfo table : migrateTables) {
            if (!table.isExpantion()) {
                List<DataNode> oldDn = table.getOldDataNodes();
                long size = 0L;
                for (DataNode dn : oldDn) {
                    size += DataMigratorUtil.querySize(dn, table.getTableName());
                }
                table.setSize(size);
            }
        }
    }

    //validate the data
    private void validate() throws SQLException {
        for (TableMigrateInfo table : migrateTables) {
            if (table.isError()) {
                continue;
            }
            long size = table.getSize().get();
            long factSize = 0L;
            for (DataNode dn : table.getNewDataNodes()) {
                factSize += DataMigratorUtil.querySize(dn, table.getTableName());
            }
            if (factSize != size) {
                String message = "migrate error!after migrate should be:" + size + " but fact is:" + factSize;
                table.setError(true);
                table.setErrMessage(message);
            }
        }

        //print migrate result
        String title = "migrate result";
        Map<String, String> result = new HashMap<>();
        for (TableMigrateInfo table : migrateTables) {
            String resultMessage = table.isError() ? "fail! reason: " + table.getErrMessage() : "success";
            result.put(table.getSchemaAndTableName(), resultMessage);
        }
        String info = DataMigratorUtil.printMigrateInfo(title, result, "->");
        System.out.println(info);
    }

    //clear the tmp file and old data
    private void clear() {
        for (TableMigrateInfo table : migrateTables) {
            makeClearDataGroup(table);
        }
        for (DataNodeClearGroup group : clearGroup) {
            clearData(group.getTempFiles(), group.getTableInfo());
        }
    }

    //for performance
    //make group by ip ,every ip has a thread pool,it is configurable,the default is cpu/2
    private void makeClearDataGroup(TableMigrateInfo table) {
        List<DataNodeMigrateInfo> list = table.getDataNodesDetail();
        for (DataNodeMigrateInfo dnInfo : list) {
            DataNode src = dnInfo.getSrc();
            String ip = src.getIp();
            File f = dnInfo.getTempFile();
            DataNodeClearGroup group = getDataNodeClearGroup(ip, table);
            if (group == null) {
                group = new DataNodeClearGroup(ip, table);
                clearGroup.add(group);
            }
            group.getTempFiles().put(f, src);
        }
    }

    private DataNodeClearGroup getDataNodeClearGroup(String ip, TableMigrateInfo table) {
        DataNodeClearGroup result = null;
        for (DataNodeClearGroup group : clearGroup) {
            if (group.getIp().equals(ip) && group.getTableInfo().equals(table)) {
                result = group;
            }
        }
        return result;
    }

    private void clearData(Map<File, DataNode> map, TableMigrateInfo table) {
        if (table.isError()) {
            return;
        }
        ExecutorService executorService = new ThreadPoolExecutor(margs.getThreadCount(), margs.getThreadCount(),
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(), new ThreadPoolExecutor.CallerRunsPolicy());
        for (Entry<File, DataNode> et : map.entrySet()) {
            File f = et.getKey();
            DataNode srcDn = et.getValue();
            executorService.execute(new DataClearRunner(table, srcDn, f));
        }
        executorService.shutdown();
        while (true) {
            if (executorService.isTerminated()) {
                break;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                LOGGER.error("error", e);
            }
        }
    }

    private void createTempParentDir(String dir) {
        File outputDir = new File(dir);
        if (outputDir.exists()) {
            DataMigratorUtil.deleteDir(outputDir);
        }
        outputDir.mkdirs();
        outputDir.setWritable(true);
    }

    private void createTableTempFiles(TableMigrateInfo table) {
        List<DataNode> oldDn = table.getOldDataNodes();
        //generate tmp files and migrate jobs
        for (DataNode dn : oldDn) {
            executor.execute(new MigratorConditonFilesMaker(table, dn, margs.getTempFileDir(), margs.getQueryPageSize()));
        }
    }
}
