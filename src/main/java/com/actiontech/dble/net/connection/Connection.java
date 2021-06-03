package com.actiontech.dble.net.connection;


import javax.annotation.Nonnull;

/**
 * Created by szf on 2020/6/15.
 */
public interface Connection {


    void closeGracefully(@Nonnull String reason);

    void closeImmediately(String reason);

    /**
     * Connection forced to close function
     * would be called by IO error .....
     * the service would get the error message and resoponse to it
     *
     * @param reason
     */
    void close(String reason);


    /**
     * businessReasonClose the connection
     * service not response to the event because the service should already know about the event
     */
    void businessClose(String reason);
}
