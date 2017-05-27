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
package io.mycat.route.function;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.LinkedList;

import io.mycat.config.model.rule.RuleAlgorithm;

/**
 * auto partition by Long ,can be used in auto increment primary key partition
 * 
 * @author wuzhi
 */
public class AutoPartitionByLong extends AbstractPartitionAlgorithm implements RuleAlgorithm{

	private static final long serialVersionUID = 5752372920655270639L;
	private String mapFile;
	private LongRange[] longRongs;
	
	private int defaultNode = -1;
	@Override
	public void init() {

		initialize();
	}

	public void setMapFile(String mapFile) {
		this.mapFile = mapFile;
	}

	@Override
	public Integer calculate(String columnValue)  {
//		columnValue = NumberParseUtil.eliminateQoute(columnValue);
		try {
			long value = Long.parseLong(columnValue);
			Integer rst = null;
			for (LongRange longRang : this.longRongs) {
				if (value <= longRang.valueEnd && value >= longRang.valueStart) {
					return longRang.nodeIndx;
				}
			}
			//数据超过范围，暂时使用配置的默认节点
			if (rst == null && defaultNode >= 0) {
				return defaultNode;
			}
			return rst;
		} catch (NumberFormatException e){
			throw new IllegalArgumentException(new StringBuilder().append("columnValue:").append(columnValue).append(" Please eliminate any quote and non number within it.").toString(),e);
		}
	}

	/**
	 * 判断是不是存在不在表格里面的边际条件
	 * @param columnValue
	 * @return
	 */
	public boolean isUseDefaultNode(String columnValue)  {
		try {
			long value = Long.parseLong(columnValue);
			Integer rst = null;
			for (LongRange longRang : this.longRongs) {
				if (value <= longRang.valueEnd && value >= longRang.valueStart) {
					return false;
				}
			}
			//数据超过范围，暂时使用配置的默认节点
			if (rst == null && defaultNode >= 0) {
				return true;
			}
		} catch (NumberFormatException e){
			throw new IllegalArgumentException(new StringBuilder().append("columnValue:").append(columnValue).append(" Please eliminate any quote and non number within it.").toString(),e);
		}
		return false;
	}

	
	@Override
	public Integer[] calculateRange(String beginValue, String endValue)  {
		Integer begin = 0, end = 0;
		if(isUseDefaultNode(beginValue) || isUseDefaultNode(endValue)){//如果beginValue或者是endValue中存在边际条件
			begin = 0;
			end = longRongs.length -1;
		}else {
			begin = calculate(beginValue);
			end = calculate(endValue);
		}



		if (begin == null || end == null) {
			return new Integer[0];
		}
		if (end >= begin) {
			int len = end - begin + 1;
			Integer[] re = new Integer[len];

			for (int i = 0; i < len; i++) {
				re[i] = begin + i;
			}
			return re;
		} else {
			return new Integer[0];
		}
	}

	@Override
	public int getPartitionNum() {
		int nPartition = longRongs.length;
		return nPartition;
	}

	private void initialize() {
		BufferedReader in = null;
		try {
			// FileInputStream fin = new FileInputStream(new File(fileMapPath));
			InputStream fin = this.getClass().getClassLoader()
					.getResourceAsStream(mapFile);
			if (fin == null) {
				throw new RuntimeException("can't find class resource file "
						+ mapFile);
			}
			in = new BufferedReader(new InputStreamReader(fin));
			LinkedList<LongRange> longRangeList = new LinkedList<LongRange>();

			for (String line = null; (line = in.readLine()) != null;) {
				line = line.trim();
				if (line.startsWith("#") || line.startsWith("//")) {
					continue;
				}
				int ind = line.indexOf('=');
				if (ind < 0) {
					System.out.println(" warn: bad line int " + mapFile + " :"
							+ line);
					continue;
				}
					String pairs[] = line.substring(0, ind).trim().split("-");
					long longStart = NumberParseUtil.parseLong(pairs[0].trim());
					long longEnd = NumberParseUtil.parseLong(pairs[1].trim());
					int nodeId = Integer.parseInt(line.substring(ind + 1)
							.trim());
					longRangeList
							.add(new LongRange(nodeId, longStart, longEnd));

			}
			longRongs = longRangeList.toArray(new LongRange[longRangeList
					.size()]);
		} catch (Exception e) {
			if (e instanceof RuntimeException) {
				throw (RuntimeException) e;
			} else {
				throw new RuntimeException(e);
			}

		} finally {
			try {
				in.close();
			} catch (Exception e2) {
			}
		}
	}
	
	public int getDefaultNode() {
		return defaultNode;
	}

	public void setDefaultNode(int defaultNode) {
		this.defaultNode = defaultNode;
	}

	static class LongRange {
		public final int nodeIndx;
		public final long valueStart;
		public final long valueEnd;

		public LongRange(int nodeIndx, long valueStart, long valueEnd) {
			super();
			this.nodeIndx = nodeIndx;
			this.valueStart = valueStart;
			this.valueEnd = valueEnd;
		}

	}
}