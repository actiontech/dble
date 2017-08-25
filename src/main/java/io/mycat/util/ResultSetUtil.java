package io.mycat.util;

import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.RowDataPacket;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

/**
 * @author struct
 */
public final class ResultSetUtil {
    private ResultSetUtil() {
    }

    public static int toFlag(ResultSetMetaData metaData, int column)
            throws SQLException {

        int flags = 0;
        if (metaData.isNullable(column) == 1) {
            flags |= 1;
        }

        if (metaData.isSigned(column)) {
            flags |= 16;
        }

        if (metaData.isAutoIncrement(column)) {
            flags |= 128;
        }

        return flags;
    }

    public static void resultSetToFieldPacket(String charset,
                                              List<FieldPacket> fieldPks, ResultSet rs,
                                              boolean isSpark) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        int colunmCount = metaData.getColumnCount();
        if (colunmCount > 0) {
            //String values="";
            for (int i = 0; i < colunmCount; i++) {
                int j = i + 1;
                FieldPacket fieldPacket = new FieldPacket();
                fieldPacket.setOrgName(StringUtil.encode(metaData.getColumnName(j), charset));
                fieldPacket.setName(StringUtil.encode(metaData.getColumnLabel(j), charset));
                if (!isSpark) {
                    fieldPacket.setOrgTable(StringUtil.encode(metaData.getTableName(j), charset));
                    fieldPacket.setTable(StringUtil.encode(metaData.getTableName(j), charset));
                    fieldPacket.setDb(StringUtil.encode(metaData.getSchemaName(j), charset));
                    fieldPacket.setFlags(toFlag(metaData, j));
                }
                fieldPacket.setLength(metaData.getColumnDisplaySize(j));

                fieldPacket.setDecimals((byte) metaData.getScale(j));
                int javaType = MysqlDefs.javaTypeDetect(
                        metaData.getColumnType(j), fieldPacket.getDecimals());
                fieldPacket.setType((byte) (MysqlDefs.javaTypeMysql(javaType) & 0xff));
                fieldPks.add(fieldPacket);
                //values+=metaData.getColumnLabel(j)+"|"+metaData.getColumnName(j)+"  ";
            }
            // System.out.println(values);
        }


    }

    public static RowDataPacket parseRowData(byte[] row,
                                             List<byte[]> fieldValues) {
        RowDataPacket rowDataPkg = new RowDataPacket(fieldValues.size());
        rowDataPkg.read(row);
        return rowDataPkg;
    }

    public static String getColumnValAsString(byte[] row,
                                              List<byte[]> fieldValues, int columnIndex) {
        RowDataPacket rowDataPkg = new RowDataPacket(fieldValues.size());
        rowDataPkg.read(row);
        byte[] columnData = rowDataPkg.fieldValues.get(columnIndex);
        return new String(columnData);
    }

    public static byte[] getColumnVal(byte[] row, List<byte[]> fieldValues,
                                      int columnIndex) {
        RowDataPacket rowDataPkg = new RowDataPacket(fieldValues.size());
        rowDataPkg.read(row);
        byte[] columnData = rowDataPkg.fieldValues.get(columnIndex);
        return columnData;
    }

    public static byte[] fromHex(String hexString) {
        String[] hex = hexString.split(" ");
        byte[] b = new byte[hex.length];
        for (int i = 0; i < hex.length; i++) {
            b[i] = (byte) (Integer.parseInt(hex[i], 16) & 0xff);
        }

        return b;
    }
}
