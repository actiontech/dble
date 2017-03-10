package io.mycat.backend.mysql.store;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import io.mycat.MycatServer;
import io.mycat.memory.environment.Hardware;

public class FileCounter {
	private static final Logger logger = Logger.getLogger(FileCounter.class);
	private static FileCounter fileCounter = new FileCounter();

	private final Lock lock;
	private final int maxFileSize;
	private int currentNum;

	private FileCounter() {
		this.lock = new ReentrantLock();
		long totalMem = Hardware.getSizeOfPhysicalMemory();
		long freeMem = Hardware.getFreeSizeOfPhysicalMemoryForLinux();
		long currentMem = Math.min(totalMem / 2, freeMem);
		this.maxFileSize = (int)(currentMem / (MycatServer.getInstance().getConfig().getSystem().getMappedFileSize() / 1024));
		logger.info("current mem is " + currentMem + "kb. max file size is " + maxFileSize);
		this.currentNum = 0;
	}

	public static FileCounter getInstance() {
		return fileCounter;
	}

	public boolean increament() {
		lock.lock();
		try {
			if (this.currentNum >= maxFileSize)
				return false;
			this.currentNum++;
			return true;
		} finally {
			lock.unlock();
		}
	}

	public boolean decrement() {
		lock.lock();
		try {
			if (this.currentNum <= 0)
				return false;
			this.currentNum--;
			return true;
		} finally {
			lock.unlock();
		}
	}
}
