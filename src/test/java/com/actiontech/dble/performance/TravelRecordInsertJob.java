/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.performance;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class TravelRecordInsertJob implements Runnable {
    private final long endId;
    private long finsihed;
    private final long batchSize;
    private final AtomicLong finshiedCount;
    private final AtomicLong failedCount;

    private final SimpleConPool conPool;

    public TravelRecordInsertJob(SimpleConPool conPool, long totalRecords,
                                 long batchSize, long startId, AtomicLong finshiedCount,
                                 AtomicLong failedCount) {
        super();
        this.conPool = conPool;
        this.endId = startId + totalRecords - 1;
        this.batchSize = batchSize;
        this.finsihed = startId;
        this.finshiedCount = finshiedCount;
        this.failedCount = failedCount;
    }

    private long insert(Connection con, List<Map<String, String>> list)
            throws SQLException {
        PreparedStatement ps;

        String sql = "insert into travelrecord (id,user_id,traveldate,fee,days) values(?,?,?,?,?)";
        ps = con.prepareStatement(sql);
        for (Map<String, String> map : list) {
            ps.setLong(1, Long.parseLong(map.get("id")));
            ps.setString(2, (String) map.get("user_id"));
            ps.setString(3, (String) map.get("traveldate"));
            ps.setString(4, (String) map.get("fee"));
            ps.setString(5, (String) map.get("days"));
            ps.addBatch();
        }
        ps.executeBatch();
        con.commit();
        ps.clearBatch();
        ps.close();
        return list.size();
    }

    private List<Map<String, String>> getNextBatch() {
        if (finsihed >= endId) {
            return Collections.emptyList();
        }
        long end = (finsihed + batchSize) < this.endId ? (finsihed + batchSize)
                : endId;
        // the last batch
        if (end + batchSize > this.endId) {
            end = this.endId;
        }
        List<Map<String, String>> list = new ArrayList<Map<String, String>>(
                Integer.valueOf((end - finsihed) + ""));
        for (long i = finsihed; i <= end; i++) {
            Map<String, String> m = new HashMap<String, String>();
            m.put("id", i + "");
            m.put("user_id", "user " + i);
            m.put("traveldate", getRandomDay(i));
            m.put("fee", i % 10000 + "");
            m.put("days", i % 10000 + "");
            list.add(m);
        }
        // System.out.println("finsihed :" + finsihed + "-" + end);
        finsihed += list.size();
        return list;
    }

    private String getRandomDay(long i) {
        int year = Long.valueOf(i % 10 + 2000).intValue();
        int month = Long.valueOf(i % 11 + 1).intValue();
        int day = Long.valueOf(i % 27 + 1).intValue();
        return year + "-" + month + "-" + day;

    }

    @Override
    public void run() {
        Connection con = null;
        try {

            List<Map<String, String>> batch = getNextBatch();
            while (!batch.isEmpty()) {
                try {
                    if (con == null || con.isClosed()) {
                        con = conPool.getConnection();
                        con.setAutoCommit(false);
                    }

                    insert(con, batch);
                    finshiedCount.addAndGet(batch.size());
                } catch (Exception e) {
                    e.printStackTrace();
                    try {
                        con.rollback();
                    } catch (SQLException e1) {
                        e1.printStackTrace();
                        e1.printStackTrace();
                    }
                    failedCount.addAndGet(batch.size());
                }
                batch = getNextBatch();
            }
        } finally {
            if (con != null) {
                this.conPool.returnCon(con);
            }
        }

    }
}