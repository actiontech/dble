package io.mycat.backend.mysql.xa.recovery.impl;

import io.mycat.backend.mysql.xa.CoordinatorLogEntry;
import io.mycat.backend.mysql.xa.Deserializer;
import io.mycat.backend.mysql.xa.Serializer;
import io.mycat.backend.mysql.xa.recovery.DeserialisationException;
import io.mycat.backend.mysql.xa.recovery.Repository;
import io.mycat.config.loader.zkprocess.comm.ZkConfig;
import io.mycat.config.loader.zkprocess.comm.ZkParamCfg;
import io.mycat.util.KVPathUtil;
import io.mycat.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by huqing.yan on 2017/6/30.
 */
public class KVStoreRepository implements Repository {
	public static final Logger logger = LoggerFactory.getLogger(KVStoreRepository.class);
	private String logPath;
	private CuratorFramework zkConn =ZKUtils.getConnection();
	public KVStoreRepository() {
		init();
	}
	@Override
	public void init() {
		logPath = KVPathUtil.XALOG+ ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID);
	}

	@Override
	public void put(String id, CoordinatorLogEntry coordinatorLogEntry) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void remove(String id) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CoordinatorLogEntry get(String coordinatorId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Collection<CoordinatorLogEntry> getAllCoordinatorLogEntries() {
		String data = null;
		try {
			if (zkConn.checkExists().forPath(logPath) != null) {
				try {
					byte[] raw = zkConn.getData().forPath(logPath);
					if (raw != null) {
						data = new String(raw, StandardCharsets.UTF_8);
					}
				} catch (Exception e) {
					logger.warn("KVStoreRepository.getAllCoordinatorLogEntries error", e);
				}
			}
		} catch (Exception e2) {
			logger.warn("KVStoreRepository error", e2);
		}
		if (data == null) {
			return Collections.emptyList();
		}
		Map<String, CoordinatorLogEntry> coordinatorLogEntries = new HashMap<>();
		String logs[] = data.split(Serializer.LINE_SEPARATOR);
		for (String log : logs) {
			try {
				CoordinatorLogEntry coordinatorLogEntry = Deserializer.fromJSON(log);
				coordinatorLogEntries.put(coordinatorLogEntry.getId(), coordinatorLogEntry);
			} catch (DeserialisationException e) {
				logger.warn("Unexpected EOF - logfile not closed properly last time? ", e);
			}
		}
		return coordinatorLogEntries.values();
	}

	@Override
	public boolean writeCheckpoint(Collection<CoordinatorLogEntry> checkpointContent) {
		try {
			StringBuilder sb = new StringBuilder();
			for (CoordinatorLogEntry coordinatorLogEntry : checkpointContent) {
				sb.append(Serializer.toJSON(coordinatorLogEntry));
			}
			byte[] data = sb.toString().getBytes(StandardCharsets.UTF_8);
			if (zkConn.checkExists().forPath(logPath) == null) {
				zkConn.create().creatingParentsIfNeeded().forPath(logPath);
			}
			zkConn.setData().forPath(logPath, data);
			return true;
		}catch (Exception e) {
			logger.warn("Failed to write checkpoint", e);
			return false;
		}
	}

	@Override
	public void close() {
	}
}
