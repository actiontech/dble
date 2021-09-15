package com.actiontech.dble.backend.mysql.proto.handler;


import org.jetbrains.annotations.Nullable;

public final class ProtoHandlerResult {
    final ProtoHandlerResultCode code;
    final int offset;
    final int packetLength;
    final byte[] packetData;
    final boolean hasMorePacket;

    private ProtoHandlerResult(ProtoHandlerResultCode code, int offset, int packetLength, byte[] packetData, boolean hasMorePacket) {
        this.code = code;
        this.offset = offset;
        this.packetLength = packetLength;
        this.packetData = packetData;
        this.hasMorePacket = hasMorePacket;
    }

    public ProtoHandlerResultCode getCode() {
        return code;
    }

    public int getOffset() {
        return offset;
    }

    @Nullable
    public byte[] getPacketData() {
        return packetData;
    }

    public int getPacketLength() {
        return packetLength;
    }

    public boolean isHasMorePacket() {
        return hasMorePacket;
    }

    public static ProtoHandlerResultBuilder builder() {
        return new ProtoHandlerResultBuilder();
    }


    public static final class ProtoHandlerResultBuilder {
        ProtoHandlerResultCode code;
        int offset;
        int packetLength;
        byte[] packetData;
        boolean hasMorePacket;

        private ProtoHandlerResultBuilder() {
        }


        public ProtoHandlerResultBuilder setCode(ProtoHandlerResultCode val) {
            this.code = val;
            return this;
        }

        public ProtoHandlerResultBuilder setOffset(int val) {
            this.offset = val;
            return this;
        }

        public ProtoHandlerResultBuilder setPacketLength(int val) {
            this.packetLength = val;
            return this;
        }

        public ProtoHandlerResultBuilder setPacketData(byte[] val) {
            this.packetData = val;
            return this;
        }

        public ProtoHandlerResultBuilder setHasMorePacket(boolean val) {
            this.hasMorePacket = val;
            return this;
        }

        public ProtoHandlerResult build() {
            return new ProtoHandlerResult(code, offset, packetLength, packetData, hasMorePacket);
        }
    }
}


