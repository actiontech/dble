package io.mycat.manager.handler;

import java.nio.ByteBuffer;

class PackageBufINf {
    private byte packetId;
    private ByteBuffer buffer;

    public byte getPacketId() {
        return packetId;
    }

    public void setPacketId(byte packetId) {
        this.packetId = packetId;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public void setBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }
}
