/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.route.function;

import com.actiontech.dble.config.model.rule.RuleAlgorithm;
import com.actiontech.dble.util.ResourceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionByFileMap.class);
    private static final long serialVersionUID = 1884866019947627284L;
    private String mapFile;
    private String ruleFile;
    private Map<Object, Integer> app2Partition;
    private int partitionNum = 0;
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
     * Otherwise will report error like this:can't find shardingnode for sharding column:column_name val:ffffffff
     */
    private int defaultNode = -1;
    private int hashCode = 1;

    @Override
    public void init() {
        initialize();
        initHashCode();
    }

    @Override
    public void selfCheck() {

    }

    public void setMapFile(String mapFile) {
        this.mapFile = mapFile;
    }

    public String getMapFile() {
        return mapFile;
    }

    public void setType(int type) {
        this.type = type;
        propertiesMap.put("type", String.valueOf(type));
    }

    public void setDefaultNode(int defaultNode) {
        if (defaultNode >= 0 || defaultNode == -1) {
            this.defaultNode = defaultNode;
        } else {
            LOGGER.warn("enum algorithm default node less than 0 and is not -1, use -1 replaced.");
        }
        propertiesMap.put("defaultNode", String.valueOf(defaultNode));
    }

    @Override
    public Integer calculate(String columnValue) {
        try {
            if (columnValue == null || columnValue.equalsIgnoreCase("NULL")) {
                return app2Partition.get(DEFAULT_NODE);
            }

            Object value = columnValue;
            if (type == 0) {
                value = Integer.valueOf(columnValue);
            }
            Integer rst;
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
        return partitionNum;
    }


    public void setRuleFile(String ruleFile) {
        this.ruleFile = ruleFile;
    }

    private void initialize() {
        StringBuilder sb = new StringBuilder("{");
        BufferedReader in = null;
        try {
            String fileName = mapFile != null ? mapFile : ruleFile;
            if (mapFile != null && ruleFile != null) {
                throw new RuntimeException("Configuration duplication in " + this.getClass().getName() + " ruleFile & mapFile both exist");
            } else if (mapFile == null && ruleFile == null) {
                throw new RuntimeException("One of the ruleFile and mapFile need config in " + this.getClass().getName());
            }
            InputStream fin = ResourceUtil.getResourceAsStreamFromRoot(fileName);
            if (fin == null) {
                throw new RuntimeException("can't find class resource file " + mapFile);
            }
            in = new BufferedReader(new InputStreamReader(fin));

            app2Partition = new HashMap<>();

            int iRow = 0;
            for (String line; (line = in.readLine()) != null; ) {
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
                    String value = line.substring(ind + 1).trim();
                    int pid = Integer.parseInt(value);
                    if (type == 0) {
                        app2Partition.put(Integer.parseInt(key), pid);
                    } else {
                        app2Partition.put(key, pid);
                    }
                    if (iRow > 0) {
                        sb.append(",");
                    }
                    iRow++;
                    sb.append("\"");
                    sb.append(key);
                    sb.append("\":");
                    sb.append("\"");
                    sb.append(value);
                    sb.append("\"");
                } catch (Exception e) {
                    //ignore error
                }
            }
            sb.append("}");
            propertiesMap.put("mapFile", sb.toString());
            //set default node
            if (defaultNode >= 0) {
                app2Partition.put(DEFAULT_NODE, defaultNode);
            }
            Set<Integer> set = new HashSet<>(app2Partition.values());
            partitionNum = set.size();
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            } else {
                throw new RuntimeException(e);
            }

        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (Exception e2) {
                //ignore error
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionByFileMap other = (PartitionByFileMap) o;
        if (other.defaultNode != defaultNode) {
            return false;
        }
        if (other.type != type) {
            return false;
        }
        if (other.app2Partition.size() != app2Partition.size()) {
            return false;
        }
        for (Map.Entry<Object, Integer> entry : app2Partition.entrySet()) {
            Integer otherValue = other.app2Partition.get(entry.getKey());
            if (!entry.getValue().equals(otherValue)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    private void initHashCode() {
        if (defaultNode != 0) {
            hashCode *= defaultNode;
        }
        if (type != 0) {
            hashCode *= type;
        }
        hashCode *= app2Partition.size();
    }
}
