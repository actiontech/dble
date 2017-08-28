package io.mycat.config.loader.zkprocess.comm;

/**
 *
 * @author liujun
 * @date 2015/2/4
 * @vsersion 0.0.1
 */
public interface NotifyService {

    /**
     * notify interface
     *
     * @return true for success ,false for failed
     * @throws Exception
     */
    boolean notifyProcess() throws Exception;
}
