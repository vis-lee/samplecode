package org.itri.ccma.editlog.manager2;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.itri.ccma.editlog.interfaces.ITransactionManager2;

public class TransactionManager_Fake implements ITransactionManager2 {
	public static final Log LOG = LogFactory.getLog(TransactionManager_Fake.class);
	private AtomicLong fakeID;

	private TransactionManager_Fake() {
		LOG.info("[ TM-F ] - TransactionManager_Fake initialized ...");
		fakeID = new AtomicLong(0);
	}

	private static TransactionManager_Fake instance;

	public static TransactionManager_Fake getInstance() {
		if (instance == null) {
			synchronized (TransactionManager_Fake.class) {
				if (instance == null) {
					instance = new TransactionManager_Fake();
				}
			}
		}
		return instance;
	}

	@Override
	public long init() {
		return init(0);
	}

	@Override
	public long init(long timeoutSecondValue) {
		return fakeID.incrementAndGet();
	}

	@Override
	public boolean commit(long runtimeID) {
		return true;
	}

	@Override
	public boolean abort(long runtimeID) {
		return true;
	}

	@Override
	public long registerEvent(long runtimeID, byte moduleType, long event) {
		return 0;
	}

	@Override
	public long registerEvent(long runtimeID, byte moduleType, long[] event) {
		return 0;
	}

	@Override
	public boolean log(long runtimeID, byte moduleType, byte[] data) {
		return true;
	}

	@Override
	public boolean notifyFlush(byte moduleType, long event, long seqID) {
		return true;
	}

	@Override
	public long getMaxSeqClosedID() {
		return Long.MAX_VALUE;
	}

	@Override
	public long getMaxSeqCommittedID() {

		return Long.MAX_VALUE;
	}

	@Override
	public void shutdown() {
		instance = null;
		LOG.info("[ TM-F ] - TransactionManager_Fake shutdown ...");
	}

}
