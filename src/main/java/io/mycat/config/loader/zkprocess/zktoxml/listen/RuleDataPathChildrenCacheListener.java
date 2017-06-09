package io.mycat.config.loader.zkprocess.zktoxml.listen;

import com.google.common.io.Files;
import io.mycat.config.model.SystemConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;

import java.io.File;
import java.io.IOException;

/**
 * Created by magicdoom on 2016/10/27.
 */
public class RuleDataPathChildrenCacheListener implements PathChildrenCacheListener {
    @Override public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        ChildData data = event.getData();
        switch (event.getType()) {
            case CHILD_ADDED:
            case CHILD_UPDATED:
                add(data) ;
                break;
            case CHILD_REMOVED:
                delete(data);
                break;
            default:
                break;
        }
    }

    private void add(ChildData childData) throws IOException {
        String name = childData.getPath().substring(childData.getPath().lastIndexOf("/")+1);
        byte[] data = childData.getData();
        File file = new File(
                SystemConfig.getHomePath() + File.separator + "conf" + File.separator + "ruledata",
                name);
        Files.write(data,file);
    }

    private void delete(ChildData childData) throws IOException {
        String name = childData.getPath().substring(childData.getPath().lastIndexOf("/")+1);
        File file = new File(
                SystemConfig.getHomePath() + File.separator + "conf" + File.separator + "ruledata",
                name);
        if(file.exists())
         file.delete();
    }

}
