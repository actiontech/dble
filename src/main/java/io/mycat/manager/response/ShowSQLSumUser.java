package io.mycat.manager.response;

import io.mycat.backend.mysql.PacketUtil;
import io.mycat.config.Fields;
import io.mycat.manager.ManagerConnection;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.ResultSetHeaderPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.statistic.stat.UserSqlRWStat;
import io.mycat.statistic.stat.UserStat;
import io.mycat.statistic.stat.UserStatAnalyzer;
import io.mycat.util.FormatUtil;
import io.mycat.util.LongUtil;
import io.mycat.util.StringUtil;

import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.Map;

/**
 * 查询用户的 SQL 执行情况
 * <p>
 * 1、用户 R/W数、读占比、并发数
 * 2、请求时间范围
 * 3、请求的耗时范围
 * 4、Net 进/出 字节数
 *
 * @author zhuam
 */
public class ShowSQLSumUser {

    private static DecimalFormat decimalFormat = new DecimalFormat("0.00");

    private static final int FIELD_COUNT = 11;
    private static final ResultSetHeaderPacket HEADER = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] FIELDS = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket EOF = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        HEADER.packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("ID", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("USER", Fields.FIELD_TYPE_VARCHAR);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("R", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("W", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("R%", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("MAX", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("NET_IN", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("NET_OUT", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].packetId = ++packetId;

        //22-06h, 06-13h, 13-18h, 18-22h
        FIELDS[i] = PacketUtil.getField("TIME_COUNT", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        //<10ms, 10ms-200ms, 200ms-1s, >1s
        FIELDS[i] = PacketUtil.getField("TTL_COUNT", Fields.FIELD_TYPE_VAR_STRING);
        FIELDS[i++].packetId = ++packetId;

        FIELDS[i] = PacketUtil.getField("LAST_TIME", Fields.FIELD_TYPE_LONGLONG);
        FIELDS[i++].packetId = ++packetId;
        EOF.packetId = ++packetId;
    }

    public static void execute(ManagerConnection c, boolean isClear) {
        ByteBuffer buffer = c.allocate();

        // write header
        buffer = HEADER.write(buffer, c, true);

        // write fields
        for (FieldPacket field : FIELDS) {
            buffer = field.write(buffer, c, true);
        }

        // write eof
        buffer = EOF.write(buffer, c, true);

        // write rows
        byte packetId = EOF.packetId;
        int i = 0;

        Map<String, UserStat> statMap = UserStatAnalyzer.getInstance().getUserStatMap();
        for (UserStat userStat : statMap.values()) {
            i++;
            RowDataPacket row = getRow(userStat, i, c.getCharset()); //getRow(sqlStat,sql, c.getCharset());
            row.packetId = ++packetId;
            buffer = row.write(buffer, c, true);

        }

        if (isClear) {
            UserStatAnalyzer.getInstance().reset();
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer, c, true);

        // write buffer
        c.write(buffer);
    }

    private static RowDataPacket getRow(UserStat userStat, long idx, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(LongUtil.toBytes(idx));
        if (userStat == null) {
            row.add(StringUtil.encode(("not fond"), charset));
            return row;
        }

        String user = userStat.getUser();
        UserSqlRWStat rwStat = userStat.getRWStat();
        long r = rwStat.getRCount();
        long w = rwStat.getWCount();
        String rStr = decimalFormat.format(1.0D * r / (r + w));
        int max = rwStat.getConcurrentMax();

        row.add(StringUtil.encode(user, charset));
        row.add(LongUtil.toBytes(r));
        row.add(LongUtil.toBytes(w));
        row.add(StringUtil.encode(String.valueOf(rStr), charset));
        row.add(StringUtil.encode(String.valueOf(max), charset));
        row.add(LongUtil.toBytes(rwStat.getNetInBytes()));
        row.add(LongUtil.toBytes(rwStat.getNetOutBytes()));
        row.add(StringUtil.encode(rwStat.getExecuteHistogram().toString(), charset));
        row.add(StringUtil.encode(rwStat.getTimeHistogram().toString(), charset));
        row.add(StringUtil.encode(FormatUtil.formatDate(rwStat.getLastExecuteTime()), charset));

        return row;
    }

}
