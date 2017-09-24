/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.route.function;

import com.actiontech.dble.config.model.rule.RuleAlgorithm;
import com.actiontech.dble.util.ResourceUtil;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author mycat
 */
public class PartitionByFileMap extends AbstractPartitionAlgorithm implements RuleAlgorithm {
    private static final long serialVersionUID = 1884866019947627284L;
    private String mapFile;
    private Map<Object, Integer> app2Partition;
    /**
     * Map<Object, Integer> app2Partition key's type:default 0 means Integer,other means String
     */
    private int type;

    /**
     * DEFAULT_NODE key
     */
    private static final String DEFAULT_NODE = "DEFAULT_NODE";

    /**
     * defaultNode:-1 means no default node ,other means the default node index
     * <p>
     * use defaultNode,the unexpected value will router to the default value.
     * Otherwise will report error like this:can't find datanode for sharding column:column_name val:ffffffff
     */
    private int defaultNode = -1;

    @Override
    public void init() {

        initialize();
    }

    public void setMapFile(String mapFile) {
        this.mapFile = mapFile;
    }

    public void setType(int type) {
        this.type = type;
    }

    public void setDefaultNode(int defaultNode) {
        this.defaultNode = defaultNode;
    }

    @Override
    public Integer calculate(String columnValue) {
        try {
            Object value = columnValue;
            if (type == 0) {
                value = Integer.valueOf(columnValue);
            }
            Integer rst = null;
            Integer pid = app2Partition.get(value);
            if (pid != null) {
                rst = pid;
            } else {
                rst = app2Partition.get(DEFAULT_NODE);
            }
            return rst;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("columnValue:" + columnValue + " Please check if the format satisfied.", e);
        }
    }

    @Override
    public Integer[] calculateRange(String beginValue, String endValue) {
        //all node
        return new Integer[0];
    }

    @Override
    public int getPartitionNum() {
        Set<Integer> set = new HashSet<>(app2Partition.values());
        int count = set.size();
        return count;
    }

    private void initialize() {
        BufferedReader in = null;
        try {
            // FileInputStream fin = new FileInputStream(new File(fileMapPath));

            InputStream fin = ResourceUtil.getResourceAsStreamFromRoot(mapFile);
            if (fin == null) {
                throw new RuntimeException("can't find class resource file " + mapFile);
            }
            in = new BufferedReader(new InputStreamReader(fin));

            app2Partition = new HashMap<>();

            for (String line = null; (line = in.readLine()) != null; ) {
                line = line.trim();
                if (line.startsWith("#") || line.startsWith("//")) {
                    continue;
                }
                int ind = line.indexOf('=');
                if (ind < 0) {
                    continue;
                }
                try {
                    String key = line.substring(0, ind).trim();
                    int pid = Integer.parseInt(line.substring(ind + 1).trim());
                    if (type == 0) {
                        app2Partition.put(Integer.parseInt(key), pid);
                    } else {
                        app2Partition.put(key, pid);
                    }
                } catch (Exception e) {
                    //ignore error
                }
            }
            //set default node
            if (defaultNode >= 0) {
                app2Partition.put(DEFAULT_NODE, defaultNode);
            }
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            } else {
                throw new RuntimeException(e);
            }

        } finally {
            try {
                in.close();
            } catch (Exception e2) {
                //ignore error
            }
        }
    }
}
