/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.performance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * test multi node merge (min,max ,sum ,order by ,limit ) performance
 *
 * @author wuzhi
 */
public class TestMergeSelectPerf {

    private static AtomicInteger finshiedCount = new AtomicInteger();
    private static AtomicInteger failedCount = new AtomicInteger();

    public static void addFinshed(int count) {
        finshiedCount.addAndGet(count);
    }

    public static void addFailed(int count) {
        failedCount.addAndGet(count);
    }

    private static Connection getCon(String url, String user, String passwd)
            throws SQLException {
        Connection theCon = DriverManager.getConnection(url, user, passwd);
        return theCon;
    }

    public static void main(String[] args) throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        if (args.length < 5) {
            System.out
                    .println("input param,format: [jdbcurl] [user] [password]  [threadpoolsize]  [executetimes] ");
            return;
        }
        int threadCount = 0;
        String url = args[0];
        String user = args[1];
        String password = args[2];
        threadCount = Integer.parseInt(args[3]);
        int executetimes = Integer.parseInt(args[4]);
        System.out.println("concerent threads:" + threadCount);
        System.out.println("execute sql times:" + executetimes);
        ArrayList<Thread> threads = new ArrayList<Thread>(threadCount);
        ArrayList<TravelRecordMergeJob> jobs = new ArrayList<TravelRecordMergeJob>(
                threadCount);
        for (int i = 0; i < threadCount; i++) {
            try {

                Connection con = getCon(url, user, password);
                System.out.println("create thread " + i);
                TravelRecordMergeJob job = new TravelRecordMergeJob(con,
                        executetimes, finshiedCount, failedCount);
                Thread thread = new Thread(job);
                threads.add(thread);
                jobs.add(job);
            } catch (Exception e) {
                System.out.println("failed create thread " + i + " err "
                        + e.toString());
            }
        }
        System.out.println("all thread started,waiting finsh...");
        System.out.println("success create thread count: " + threads.size());
        for (Thread thread : threads) {
            thread.start();
        }
        long start = System.currentTimeMillis();
        System.out.println("all thread started,waiting finsh...");
        boolean notFinished = true;
        while (notFinished) {
            notFinished = false;
            for (Thread thread : threads) {
                if (thread.isAlive()) {
                    notFinished = true;
                    break;
                }
            }
            report(jobs);
            Thread.sleep(1000);
        }
        report(jobs);
        System.out.println("total time :" + (System.currentTimeMillis() - start) / 1000);
    }

    public static void report(ArrayList<TravelRecordMergeJob> jobs) {
        int tps = 0;
        for (TravelRecordMergeJob job : jobs) {
            tps += job.getTPS();
        }
        System.out.println("finishend:" + finshiedCount.get() + " failed:"
                + failedCount.get());
        System.out.println("tps:" + tps);
    }
}