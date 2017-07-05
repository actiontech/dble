package io.mycat.server.response;

import com.google.common.base.Strings;
import io.mycat.MycatServer;
import io.mycat.backend.mysql.PacketUtil;
import io.mycat.config.ErrorCode;
import io.mycat.config.Fields;
import io.mycat.config.MycatConfig;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.config.model.UserConfig;
import io.mycat.meta.SchemaMeta;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.ResultSetHeaderPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.server.ServerConnection;
import io.mycat.server.parser.ServerParse;
import io.mycat.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * show tables impl
 *
 * @author yanglixue
 */
public class ShowTables {
	private static final String LIKE_KEY = "like";
	private static String TABLE_PAT = "^\\s*(show){1}" +
			"(\\s+full){0,1}" +
			"(\\s+tables){1}" +
			"(\\s+(from|in){1}\\s+([a-zA-Z_0-9]{1,})){0,1}" +
			"((\\s+(like){1}\\s+\\'((. *){0,})\\'\\s*)|(\\s+(where){1}\\s+((. *){0,})\\s*)){0,1}" +
			"\\s*$";
	public static Pattern pattern = Pattern.compile(TABLE_PAT, Pattern.CASE_INSENSITIVE);
	public static void response(ServerConnection c, String stmt, boolean isFull) {
		String showSchema = getShowTableFrom(stmt);
		if (showSchema != null && MycatServer.getInstance().getConfig().getSystem().isLowerCaseTableNames()) {
			showSchema = showSchema.toLowerCase();
		}
		String cSchema = showSchema == null ? c.getSchema() : showSchema;
		if (cSchema == null) {
			c.writeErrMessage("3D000", "No database selected", ErrorCode.ER_NO_DB_ERROR);
			return;
		}
		SchemaConfig schema = MycatServer.getInstance().getConfig().getSchemas().get(cSchema);
		if (schema == null) {
			c.writeErrMessage("42000", "Unknown database '" + cSchema + "'", ErrorCode.ER_BAD_DB_ERROR);
			return;
		}

		MycatConfig conf = MycatServer.getInstance().getConfig();
		UserConfig user = conf.getUsers().get(c.getUser());
		if (user == null || !user.getSchemas().contains(cSchema)) {
			c.writeErrMessage("42000", "Access denied for user '" + c.getUser() + "' to database '" + cSchema + "'", ErrorCode.ER_DBACCESS_DENIED_ERROR);
			return;
		}
		//不分库的schema，show tables从后端 mysql中查
		String node = schema.getDataNode();
		if (!Strings.isNullOrEmpty(node)) {
			c.execute(stmt, ServerParse.SHOW);
			return;
		}
		responseDirect(c, stmt, cSchema, isFull);
	}

	private static void responseDirect(ServerConnection c, String stmt, String cSchema, boolean isFull) {
		ByteBuffer buffer = c.allocate();
		Map<String, String> tableMap = getTableSet(stmt, cSchema);
		if (isFull) {
			byte packetId = writeFullTablesHeader(buffer, c, tableMap, cSchema);
			writeRowEof(buffer, c, packetId);
		} else {
			byte packetId = writeTablesHeader(buffer, c, tableMap, cSchema);
			writeRowEof(buffer, c, packetId);
		}
	}

	public static byte writeFullTablesHeader(ByteBuffer buffer, ServerConnection c, Map<String, String> tableMap, String cSchema) {
		int FIELD_COUNT = 2;
		ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
		FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
		EOFPacket eof = new EOFPacket();
		int i = 0;
		byte packetId = 0;
		header.packetId = ++packetId;
		fields[i] = PacketUtil.getField("Tables in " + cSchema, Fields.FIELD_TYPE_VAR_STRING);
		fields[i].packetId = ++packetId;
		fields[i + 1] = PacketUtil.getField("Table_type  ", Fields.FIELD_TYPE_VAR_STRING);
		fields[i + 1].packetId = ++packetId;
		eof.packetId = ++packetId;
		// write header
		buffer = header.write(buffer, c, true);
		// write fields
		for (FieldPacket field : fields) {
			buffer = field.write(buffer, c, true);
		}
		buffer = eof.write(buffer, c, true);
		for (String name : tableMap.keySet()) {
			RowDataPacket row = new RowDataPacket(FIELD_COUNT);
			row.add(StringUtil.encode(name.toLowerCase(), c.getCharset()));
			row.add(StringUtil.encode(tableMap.get(name), c.getCharset()));
			row.packetId = ++packetId;
			buffer = row.write(buffer, c, true);
		}
		return packetId;
	}

	public static byte writeTablesHeader(ByteBuffer buffer, ServerConnection c, Map<String, String> tableMap, String cSchema) {
		int FIELD_COUNT = 1;
		ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
		FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
		EOFPacket eof = new EOFPacket();
		int i = 0;
		byte packetId = 0;
		header.packetId = ++packetId;
		fields[i] = PacketUtil.getField("Tables in " + cSchema, Fields.FIELD_TYPE_VAR_STRING);
		fields[i].packetId = ++packetId;
		eof.packetId = ++packetId;
		// write header
		buffer = header.write(buffer, c, true);
		// write fields
		for (FieldPacket field : fields) {
			buffer = field.write(buffer, c, true);
		}
		// write eof
		eof.write(buffer, c, true);
		for (String name : tableMap.keySet()) {
			RowDataPacket row = new RowDataPacket(FIELD_COUNT);
			row.add(StringUtil.encode(name.toLowerCase(), c.getCharset()));
			row.packetId = ++packetId;
			buffer = row.write(buffer, c, true);
		}
		return packetId;
	}
	private static void writeRowEof(ByteBuffer buffer, ServerConnection c, byte packetId) {

		// write last eof
		EOFPacket lastEof = new EOFPacket();
		lastEof.packetId = ++packetId;
		buffer = lastEof.write(buffer, c, true);

		// post write
		c.write(buffer);
	}

	public static Map<String, String> getTableSet(String stmt, String cSchema) {
		//在这里对于没有建立起来的表格进行过滤，去除尚未新建的表格
		SchemaMeta schemata = MycatServer.getInstance().getTmManager().getCatalogs().get(cSchema);
		if (schemata == null) {
			return new HashMap<>();
		}
		Map meta = schemata.getTableMetas();
		TreeMap<String, String> tableMap = new TreeMap<>();
		Map<String, SchemaConfig> schemas = MycatServer.getInstance().getConfig().getSchemas();
		String like =getShowTableLike(stmt);
		if (null == like) {
			for (TableConfig tbConfig : schemas.get(cSchema).getTables().values()) {
				String tbName = tbConfig.getName();
				if (meta.get(tbName) != null) {
					String tbType = tbConfig.getTableType() == TableConfig.TableTypeEnum.TYPE_GLOBAL_TABLE ? "GLOBAL TABLE" : "SHARDING TABLE";
					tableMap.put(tbName, tbType);
				}
			}
		} else {
			String p = "^" + like.replaceAll("%", ".*");
			Pattern pattern = Pattern.compile(p, Pattern.CASE_INSENSITIVE);
			Matcher ma;

			for (TableConfig tbConfig : schemas.get(cSchema).getTables().values()) {
				String tbName = tbConfig.getName();
				ma = pattern.matcher(tbName);
				if (ma.matches() && meta.get(tbName) != null) {
					String tbType = tbConfig.getTableType() == TableConfig.TableTypeEnum.TYPE_GLOBAL_TABLE ? "GLOBAL TABLE" : "SHARDING TABLE";
					tableMap.put(tbName, tbType);
				}
			}
		}
		return tableMap;
	}

	public static String getShowTableFrom(String sql) {
		Matcher ma = pattern.matcher(sql);
		if (ma.matches()) {
			return ma.group(6);
		}
		return null;
	}
	private static String getShowTableLike(String sql) {
		Matcher ma = pattern.matcher(sql);
		if (ma.matches()) {
			return ma.group(10);
		}
		return null;
	}
}
