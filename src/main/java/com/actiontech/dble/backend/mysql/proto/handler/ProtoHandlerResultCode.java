package com.actiontech.dble.backend.mysql.proto.handler;

/**
 * Created by szf on 2020/6/17.
 */

public enum ProtoHandlerResultCode {
    /**
     * receive a complete packet
     */

    COMPLETE_PACKET,

    /**
     * receive a part of big packet.
     */
    PART_OF_BIG_PACKET,
    BUFFER_PACKET_UNCOMPLETE,
    BUFFER_NOT_BIG_ENOUGH,

    /**
     * receive ssl packet
     */
    SSL_PROTO_PACKET,
    SSL_APP_PACKET,
    SSL_BUFFER_NOT_BIG_ENOUGH,

    @Deprecated
    REACH_END_BUFFER,
    /**
     * receive a complete packet and has rest data exists in buffer.
     */
    @Deprecated
    STLL_DATA_REMING
}
