package io.mycat.backend.mysql.nio.handler.query;

import java.util.concurrent.atomic.AtomicBoolean;

import io.mycat.MycatServer;
import io.mycat.server.NonBlockingSession;

/**
 * 拥有自己的thread的dmlhandler
 * 
 * @author ActionTech
 * @CreateTime 2014年11月27日
 */
public abstract class OwnThreadDMLHandler extends BaseDMLHandler {
	/* 当前是否需要结束ownthread，ownthread运行中时为true */
	private AtomicBoolean ownJobFlag;
	private Object ownThreadLock = new Object();
	private boolean preparedToRecycle;

	public OwnThreadDMLHandler(long id, NonBlockingSession session) {
		super(id, session);
		this.ownJobFlag = new AtomicBoolean(false);
		this.preparedToRecycle = false;
	}

	@Override
	public final void onTerminate() throws Exception {
		if (ownJobFlag.compareAndSet(false, true)) {
			// thread未启动即进入了terminate
			recycleResources();
		} else {// thread已经启动
			synchronized (ownThreadLock) {
				if (!preparedToRecycle) { // 还未进入释放资源的地步
					terminateThread();
				}
			}
		}
	}

	/**
	 * @param objects
	 *            有可能会用到的参数
	 */
	protected final void startOwnThread(final Object... objects) {
		MycatServer.getInstance().getComplexQueryExecutor().execute(new Runnable() {
			@Override
			public void run() {
				if (terminate.get())
					return;
				if (ownJobFlag.compareAndSet(false, true)) {
					try {
						ownThreadJob(objects);
					} finally {
						synchronized (ownThreadLock) {
							preparedToRecycle = true;
						}
						recycleResources();
					}
				}
			}
		});
	}

	protected abstract void ownThreadJob(Object... objects);

	/* 通过一些动作，可以让running的thread终结 */
	protected abstract void terminateThread() throws Exception;

	/* 线程结束后需要执行的动作 */
	protected abstract void recycleResources();

}
