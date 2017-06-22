package io.mycat.server.response;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Strings;

import io.mycat.MycatServer;
import io.mycat.backend.mysql.PacketUtil;
import io.mycat.config.ErrorCode;
import io.mycat.config.Fields;
import io.mycat.config.MycatConfig;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.UserConfig;
import io.mycat.meta.SchemaMeta;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.ResultSetHeaderPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.server.ServerConnection;
import io.mycat.server.parser.ServerParse;
import io.mycat.server.util.SchemaUtil;
import io.mycat.util.StringUtil;

import static io.mycat.server.parser.ServerParseShow.FULL_TABLE_CHECK;
import static io.mycat.server.parser.ServerParseShow.TABLE_CHECK;

/**
 * show tables impl
 * @author yanglixue
 *
 */
public class ShowTables {
    private static final String SCHEMA_KEY = "schemaName";
    private static final String LIKE_KEY = "like";

	public static void response(ServerConnection c, String stmt, boolean isFull) {
		String showSchema = SchemaUtil.parseShowTableSchema(stmt);
		String cSchema = showSchema == null ? c.getSchema() : showSchema;
		SchemaConfig schema = MycatServer.getInstance().getConfig().getSchemas().get(cSchema);
		if (schema == null) {
			c.writeErrMessage(ErrorCode.ER_NO_DB_ERROR, "No database selected");
			return;
		}
		//不分库的schema，show tables从后端 mysql中查
		String node = schema.getDataNode();
		if (!Strings.isNullOrEmpty(node)) {
			c.execute(stmt, ServerParse.SHOW);
			return;
		}
		responseDirect(c, stmt, isFull);
	}

	private static void responseDirect(ServerConnection c, String stmt, boolean isFull) {
		if (isFull) {
			responseDirectShowFullTables(c, stmt);
		} else {
			responseDirectShowTables(c, stmt);
		}
	}
	private static void responseDirectShowFullTables(ServerConnection c, String stmt) {
		int FIELD_COUNT = 2;
		ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
		FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
		EOFPacket eof = new EOFPacket();
		Map<String, String> parm = buildShowFullTablesFields(c, stmt);
		Set<String> tableSet = getTableSet(c, parm, c.getSchema());
		int i = 0;
		byte packetId = 0;
		header.packetId = ++packetId;
		fields[i] = PacketUtil.getField("Tables in " + parm.get(SCHEMA_KEY), Fields.FIELD_TYPE_VAR_STRING);
		fields[i].packetId = ++packetId;
		fields[i+1] = PacketUtil.getField("Table_type  " , Fields.FIELD_TYPE_VAR_STRING);
		fields[i+1].packetId = ++packetId;
		eof.packetId = ++packetId;
		ByteBuffer buffer = c.allocate();
		// write header
		buffer = header.write(buffer, c,true);
		// write fields
		for (FieldPacket field : fields) {
			buffer = field.write(buffer, c,true);
		}
		// write eof
		buffer = eof.write(buffer, c,true);
		// write rows
		packetId = eof.packetId;
		for (String name : tableSet) {
			RowDataPacket row = new RowDataPacket(FIELD_COUNT);
			row.add(StringUtil.encode(name.toLowerCase(), c.getCharset()));
			row.add(StringUtil.encode("SHARDING TABLE", c.getCharset()));
			row.packetId = ++packetId;
			buffer = row.write(buffer, c,true);
		}
		// write last eof
		EOFPacket lastEof = new EOFPacket();
		lastEof.packetId = ++packetId;
		buffer = lastEof.write(buffer, c,true);

		// post write
		c.write(buffer);
	}
	private static void responseDirectShowTables(ServerConnection c, String stmt) {
		int FIELD_COUNT = 1;
		ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
		FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
		EOFPacket eof = new EOFPacket();
		Map<String, String> parm = buildShowTablesFields(c, stmt);
		Set<String> tableSet = getTableSet(c, parm, c.getSchema());
		int i = 0;
		byte packetId = 0;
		header.packetId = ++packetId;
		fields[i] = PacketUtil.getField("Tables in " + parm.get(SCHEMA_KEY), Fields.FIELD_TYPE_VAR_STRING);
		fields[i].packetId = ++packetId;
		eof.packetId = ++packetId;
		ByteBuffer buffer = c.allocate();
		// write header
		buffer = header.write(buffer, c, true);
		// write fields
		for (FieldPacket field : fields) {
			buffer = field.write(buffer, c, true);
		}
		// write eof
		buffer = eof.write(buffer, c, true);

		// write rows
		packetId = eof.packetId;
		for (String name : tableSet) {
			RowDataPacket row = new RowDataPacket(FIELD_COUNT);
			row.add(StringUtil.encode(name.toLowerCase(), c.getCharset()));
			row.packetId = ++packetId;
			buffer = row.write(buffer, c, true);
		}
		// write last eof
		EOFPacket lastEof = new EOFPacket();
		lastEof.packetId = ++packetId;
		buffer = lastEof.write(buffer, c, true);

		// post write
		c.write(buffer);
	}

	public static Set<String> getTableSet(ServerConnection c, String stmt, boolean isFull) {
		Map<String, String> parm = buildFields(c, stmt, isFull);
		return getTableSet(c, parm, c.getSchema());
	}

    private static Set<String> getTableSet(ServerConnection c, Map<String, String> parm,String cSchema)
    {
		//在这里对于没有建立起来的表格进行过滤，去除尚未新建的表格
		SchemaMeta schemata = MycatServer.getInstance().getTmManager().getCatalogs().get(cSchema);
		if (schemata == null) {
			return new HashSet<>();
		}
		Map meta = schemata.getTableMetas();
		TreeSet<String> tableSet = new TreeSet<>();
		MycatConfig conf = MycatServer.getInstance().getConfig();

		Map<String, UserConfig> users = conf.getUsers();
		UserConfig user = users == null ? null : users.get(c.getUser());
		if (user != null) {
			Map<String, SchemaConfig> schemas = conf.getSchemas();
			for (String name : schemas.keySet()) {
				if (null != parm.get(SCHEMA_KEY) && parm.get(SCHEMA_KEY).toUpperCase().equals(name.toUpperCase())) {
					if (null == parm.get(LIKE_KEY)) {
						//tableSet.addAll(schemas.get(name).getTables().keySet());
						for (String tname : schemas.get(name).getTables().keySet()) {
							if (meta.get(tname) != null) {
								tableSet.add(tname);
							}
						}
					} else {
						String p = "^" + parm.get(LIKE_KEY).replaceAll("%", ".*");
						Pattern pattern = Pattern.compile(p, Pattern.CASE_INSENSITIVE);
						Matcher ma;

						for (String tname : schemas.get(name).getTables().keySet()) {
							ma = pattern.matcher(tname);
							if (ma.matches() && meta.get(tname) != null) {
								tableSet.add(tname);
							}
						}
					}
				}
			}
		}
		return tableSet;
	}

	private static Map<String, String> buildFields(ServerConnection c, String stmt, boolean isFull) {
		if (isFull) {
			return buildShowFullTablesFields(c, stmt);
		} else {
			return buildShowTablesFields(c, stmt);
		}
	}
	private static Map<String, String> buildShowFullTablesFields(ServerConnection c, String stmt) {
		Map<String,String> map = new HashMap<>();
		Pattern pattern = Pattern.compile(FULL_TABLE_CHECK,Pattern.CASE_INSENSITIVE);
		Matcher ma = pattern.matcher(stmt);
		if(ma.find()){
			String schemaName=ma.group(6);
			if (null !=schemaName && (!"".equals(schemaName)) && (!"null".equals(schemaName))){
				map.put(SCHEMA_KEY, schemaName);
			}
			String like = ma.group(9);
			if (null !=like && (!"".equals(like)) && (!"null".equals(like))){
				map.put(LIKE_KEY, like);
			}
		}
		if(null==map.get(SCHEMA_KEY)){
			map.put(SCHEMA_KEY, c.getSchema());
		}
		return  map;
	}
	private static Map<String, String> buildShowTablesFields(ServerConnection c, String stmt) {
		Map<String, String> map = new HashMap<>();
		Pattern pattern = Pattern.compile(TABLE_CHECK,Pattern.CASE_INSENSITIVE);
		Matcher ma = pattern.matcher(stmt);
		if (ma.find()) {
			String schemaName = ma.group(5);
			if (null != schemaName && (!"".equals(schemaName)) && (!"null".equals(schemaName))) {
				map.put(SCHEMA_KEY, schemaName);
			}
			String like = ma.group(8);
			if (null != like && (!"".equals(like)) && (!"null".equals(like))) {
				map.put(LIKE_KEY, like);
			}
		}
		if (null == map.get(SCHEMA_KEY)) {
			map.put(SCHEMA_KEY, c.getSchema());
		}
		return map;
	}
	
}
