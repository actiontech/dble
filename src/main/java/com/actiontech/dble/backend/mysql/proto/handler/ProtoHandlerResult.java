package com.actiontech.dble.backend.mysql.proto.handler;

public class ProtoHandlerResult {
    final ProtoHandlerResultCode code;
    final int offset;
    final int packetLength;
    final byte[] packetData;

    public ProtoHandlerResult(ProtoHandlerResultCode code, int offset, byte[] packetData) {
        this.code = code;
        this.offset = offset;
        this.packetData = packetData;
        this.packetLength = 0;
    }

    public ProtoHandlerResult(ProtoHandlerResultCode code, int offset) {
        this.code = code;
        this.offset = offset;
        this.packetData = null;
        this.packetLength = 0;
    }

    public ProtoHandlerResult(ProtoHandlerResultCode code, int offset, int packetLength) {
        this.code = code;
        this.offset = offset;
        this.packetData = null;
        this.packetLength = packetLength;
    }

    public ProtoHandlerResultCode getCode() {
        return code;
    }

    public int getOffset() {
        return offset;
    }

    public byte[] getPacketData() {
        return packetData;
    }

    public int getPacketLength() {
        return packetLength;
    }
}


