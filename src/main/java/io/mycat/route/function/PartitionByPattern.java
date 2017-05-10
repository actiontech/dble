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
import java.util.HashSet;
import java.util.regex.Pattern;

import io.mycat.config.model.rule.RuleAlgorithm;

/**
 * auto partition by Long
 * 
 * @author hexiaobin
 */
public class PartitionByPattern extends AbstractPartitionAlgorithm implements RuleAlgorithm {
	private static final int PARTITION_LENGTH = 1024;
	private int patternValue = PARTITION_LENGTH;// 分区长度，取模数值
	private String mapFile;
	private LongRange[] longRongs;
    	private Integer[] allNode;
    	private boolean outOfOrder = false;
	private int defaultNode = 0;// 包含非数值字符，默认存储节点
    	private static final  Pattern pattern = Pattern.compile("[0-9]*");;

	@Override
	public void init() {
		initialize();
	}
    
 	public void setMapFile(String mapFile) {
		this.mapFile = mapFile;
	}

	public void setPatternValue(int patternValue) {
		this.patternValue = patternValue;
	}

	public void setDefaultNode(int defaultNode) {
		this.defaultNode = defaultNode;
	}

    	private Integer findNode(long hash) {
		Integer rst = null;
		for (LongRange longRang : this.longRongs) {
			if (hash <= longRang.valueEnd && hash >= longRang.valueStart) {
				return longRang.nodeIndx;
			}
		}
		
		return rst;
	}

	@Override
	public Integer calculate(String columnValue) {
		if (!isNumeric(columnValue)) {
			return defaultNode;
		}
		
		long value = Long.parseLong(columnValue);
		long hash = value % patternValue;
		return findNode(hash);
	}

    	/* x2 - x1 < m
	 *     n1 < n2 ---> n1 - n2  		type1
	 *     n1 > n2 ---> 0 - n2 && n1 - L  	type2
	 * x2 - x1 >= m
	 *     L 				type3
	 */
    	private void calc_aux(HashSet<Integer> ids, long begin, long end) {
	    	for (LongRange longRang : this.longRongs) {
		    	if (longRang.valueEnd < begin) {
			    	continue;
			}
			if (longRang.valueStart > end) {
			    	break;
			}
			ids.add(longRang.nodeIndx);
		}
	}
    
    	private Integer[] calc_type1(long begin, long end) {
	    	HashSet<Integer> ids = new HashSet<Integer>();
		
		calc_aux(ids, begin, end);

		return ids.toArray(new Integer[ids.size()]);
	}

    	private Integer[] calc_type2(long begin, long end) {
	    	HashSet<Integer> ids = new HashSet<Integer>();

		calc_aux(ids, begin, patternValue);
		calc_aux(ids, 0, end);

		return ids.toArray(new Integer[ids.size()]);
	}

    	private Integer[] calc_type3() {
	    	return allNode;
	}

    	/* NODE:
	 * if the order of range and the order of nodeid don't match, we give all nodeid.
	 * so when writing configure, be cautious
	 */
    	public Integer[] calculateRange(String beginValue, String endValue)  {
		if (!isNumeric(beginValue) || !isNumeric(endValue)) {
		    	return calc_type3();
		}
		long bv = Long.parseLong(beginValue);
		long ev = Long.parseLong(endValue);
		long hbv = bv % patternValue;
		long hev = ev % patternValue;

		if (findNode(hbv) == null || findNode(hbv) == null) {
		    	return calc_type3();
		}

		if (ev >= bv) {
			if (ev - bv >= patternValue) {
			    	return calc_type3();
			}

			if (hbv < hev) {
			    	return calc_type1(hbv, hev);
			} else {
			    	return calc_type2(hbv, hev);
			}
		} else {
		    	return new Integer[0];
		}
	}
		
	@Override
	public int getPartitionNum() {
		int nPartition = this.longRongs.length;
		return nPartition;
	}

	public static boolean isNumeric(String str) {
		return pattern.matcher(str).matches();
	}

    	private void initialize_aux(LinkedList<LongRange> ll, LongRange lr) {
	    	if (ll.size() == 0) {
		    	ll.add(lr);
		} else {
		    	LongRange tmp;
			for (int i = ll.size() - 1; i > -1; i--) {
				tmp = ll.get(i);
				if (tmp.valueStart < lr.valueStart) {
				    	ll.add(i + 1, lr);
					return;
				}
			}
			ll.add(0, lr);
		}    
	}

	private void initialize() {
		BufferedReader in = null;
		try {
			// FileInputStream fin = new FileInputStream(new File(fileMapPath));
			InputStream fin = this.getClass().getClassLoader().getResourceAsStream(mapFile);
			if (fin == null) {
				throw new RuntimeException("can't find class resource file " + mapFile);
			}
			in = new BufferedReader(new InputStreamReader(fin));
			LinkedList<LongRange> longRangeList = new LinkedList<LongRange>();
			HashSet<Integer> ids = new HashSet<Integer>();

			for (String line = null; (line = in.readLine()) != null;) {
				line = line.trim();
				if (line.startsWith("#") || line.startsWith("//")) {
					continue;
				}
				
				int ind = line.indexOf('=');
				if (ind < 0) {
					System.out.println(" warn: bad line int " + mapFile + " :" + line);
					continue;
				}
				
				String pairs[] = line.substring(0, ind).trim().split("-");
				long longStart = Long.parseLong(pairs[0].trim());
				long longEnd = Long.parseLong(pairs[1].trim());
				int nodeId = Integer.parseInt(line.substring(ind + 1).trim());
				
				ids.add(nodeId);
				initialize_aux(longRangeList, new LongRange(nodeId, longStart, longEnd));
			}
			
			allNode = ids.toArray(new Integer[ids.size()]);
			longRongs = longRangeList.toArray(new LongRange[longRangeList.size()]);
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
