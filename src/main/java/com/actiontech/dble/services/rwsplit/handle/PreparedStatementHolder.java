package com.actiontech.dble.services.rwsplit.handle;

public class PreparedStatementHolder {

    private final byte[] prepareOrigin;
    private final int paramsCount;
    private byte[] executeOrigin;
    private final boolean mustMaster;
    private byte[] fieldType;
    private boolean needAddFieldType;
    private String prepareSql;

    public PreparedStatementHolder(byte[] prepareOrigin, int paramsCount, boolean mustMaster, String sql) {
        this.prepareOrigin = prepareOrigin;
        this.paramsCount = paramsCount;
        this.mustMaster = mustMaster;
        this.prepareSql = sql;
    }

    public boolean isMustMaster() {
        return mustMaster;
    }

    public byte[] getPrepareOrigin() {
        return prepareOrigin;
    }

    public byte[] getExecuteOrigin() {
        return executeOrigin;
    }

    public void setExecuteOrigin(byte[] executeOrigin) {
        int nullBitMapSize = (paramsCount + 7) / 8;
        byte newParameterBoundFlag = executeOrigin[14 + nullBitMapSize];
        if (newParameterBoundFlag == (byte) 1) {
            fieldType = new byte[2 * paramsCount];
            System.arraycopy(executeOrigin, 15 + nullBitMapSize, fieldType, 0, 2 * paramsCount);

        } else if (fieldType != null) {
            needAddFieldType = true;
        }
        this.executeOrigin = executeOrigin;
    }

    public byte[] getFieldType() {
        return fieldType;
    }

    // doesn't contain field type except first execute packet
    public boolean isNeedAddFieldType() {
        return needAddFieldType;
    }

    public int getParamsCount() {
        return paramsCount;
    }

    public String getPrepareSql() {
        return prepareSql;
    }
}
