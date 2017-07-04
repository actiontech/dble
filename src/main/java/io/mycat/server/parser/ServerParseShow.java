/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese 
 * opensource volunteers. you can redistribute it and/or modify it under the 
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * Any questions about this component can be directed to it's project Web address 
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.server.parser;

import io.mycat.route.parser.util.ParseUtil;
import io.mycat.server.response.ShowTables;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author mycat
 */
public final class ServerParseShow {

	public static final int OTHER = -1;
	public static final int DATABASES = 1;
	public static final int DATASOURCES = 2;
	public static final int TABLES = 5;
    public static final int FULLTABLES =6;
    public static final int CHARSET = 7;

	public static int parse(String stmt, int offset) {
		int i = offset;
		for (; i < stmt.length(); i++) {
			switch (stmt.charAt(i)) {
				case ' ':
				case '\r':
				case '\n':
				case '\t':
					continue;
				case 'F':
				case 'f':
					return showTableType(stmt);
				case '/':
				case '#':
					i = ParseUtil.comment(stmt, i);
					continue;
				case 'D':
				case 'd':
					return dataCheck(stmt, i);
				case 'T':
				case 't':
					return showTableType(stmt);
				case 'S':
				case 's':
					return schemasCheck(stmt, i);
				case 'C':
				case 'c':
					return charsetCheck(stmt, i);
				default:
					return OTHER;
			}
		}
		return OTHER;
	}



	// SHOW DATA
	static int dataCheck(String stmt, int offset) {
		if (stmt.length() > offset + "ata?".length()) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			if ((c1 == 'A' || c1 == 'a') && (c2 == 'T' || c2 == 't')
					&& (c3 == 'A' || c3 == 'a')) {
				switch (stmt.charAt(++offset)) {
				case 'B':
				case 'b':
					return showDatabases(stmt, offset);
				case 'S':
				case 's':
					return showDataSources(stmt, offset);
				default:
					return OTHER;
				}
			}
		}
		return OTHER;
	}



	private static boolean isShowTableMatched(String stmt, String pat1) {
		Pattern pattern = Pattern.compile(pat1, Pattern.CASE_INSENSITIVE);
		Matcher ma = pattern.matcher(stmt);

		boolean flag = ma.matches();
		return flag;
	}

	// SHOW DATABASES
	static int showDatabases(String stmt, int offset) {
		if (stmt.length() > offset + "ases".length()) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			char c4 = stmt.charAt(++offset);
			if ((c1 == 'A' || c1 == 'a')
					&& (c2 == 'S' || c2 == 's')
					&& (c3 == 'E' || c3 == 'e')
					&& (c4 == 'S' || c4 == 's')
					&& (stmt.length() == ++offset || ParseUtil.isEOF(stmt
							.charAt(offset)))) {
				return DATABASES;
			}
		}
		return OTHER;
	}

	//show schemas
	static int schemasCheck(String stmt, int offset) {
		if (stmt.length() > offset + "chemas".length()) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			char c4 = stmt.charAt(++offset);
			char c5 = stmt.charAt(++offset);
			char c6 = stmt.charAt(++offset);
			if ((c1 == 'C' || c1 == 'c')
					&& (c2 == 'H' || c2 == 'h')
					&& (c3 == 'E' || c3 == 'e')
					&& (c4 == 'M' || c4 == 'm')
					&& (c5 == 'A' || c5 == 'a')
					&& (c6 == 'S' || c6 == 's')
					&& (stmt.length() == ++offset || ParseUtil.isEOF(stmt
					.charAt(offset)))) {
				return DATABASES;
			}
		}
		return OTHER;
	}

	//show charset
	static int charsetCheck(String stmt, int offset) {
		if (stmt.length() > offset + "harset".length()) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			char c4 = stmt.charAt(++offset);
			char c5 = stmt.charAt(++offset);
			char c6 = stmt.charAt(++offset);
			if ((c1 == 'H' || c1 == 'h')
					&& (c2 == 'A' || c2 == 'a')
					&& (c3 == 'R' || c3 == 'r')
					&& (c4 == 'S' || c4 == 's')
					&& (c5 == 'E' || c5 == 'e')
					&& (c6 == 'T' || c6 == 't')
					&& (stmt.length() == ++offset || ParseUtil.isEOF(stmt
					.charAt(offset)))) {
				return CHARSET;
			}
		}
		return OTHER;
	}

	// SHOW DATASOURCES
	static int showDataSources(String stmt, int offset) {
		if (stmt.length() > offset + "ources".length()) {
			char c1 = stmt.charAt(++offset);
			char c2 = stmt.charAt(++offset);
			char c3 = stmt.charAt(++offset);
			char c4 = stmt.charAt(++offset);
			char c5 = stmt.charAt(++offset);
			char c6 = stmt.charAt(++offset);
			if ((c1 == 'O' || c1 == 'o')
					&& (c2 == 'U' || c2 == 'u')
					&& (c3 == 'R' || c3 == 'r')
					&& (c4 == 'C' || c4 == 'c')
					&& (c5 == 'E' || c5 == 'e')
					&& (c6 == 'S' || c6 == 's')
					&& (stmt.length() == ++offset || ParseUtil.isEOF(stmt
							.charAt(offset)))) {
				return DATASOURCES;
			}
		}
		return OTHER;
	}

	public static int showTableType(String sql) {
		Pattern pattern = ShowTables.pattern;
		Matcher ma = pattern.matcher(sql);
		if (ma.matches()) {
			if (ma.group(2) != null) {
				return FULLTABLES;
			} else {
				return TABLES;
			}
		} else {
			return OTHER;
		}
	}



}