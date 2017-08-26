package io.mycat.config.loader.console;

/**
 * ZookeeperPath
 *
 *
 * author:liujun
 * Created:2016/9/15
 *
 *
 *
 *
 */
public enum ZookeeperPath {

    /**
     * the local path where zk will write to
     */
    ZK_LOCAL_WRITE_PATH("./"),;
    private String key;

    ZookeeperPath(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }


}
