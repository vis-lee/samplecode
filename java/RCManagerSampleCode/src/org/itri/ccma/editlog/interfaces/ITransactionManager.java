package org.itri.ccma.editlog.interfaces;

public interface ITransactionManager {
	public long init();

	public long init(long timeoutSecondValue);

	public boolean commit(long transID);

	public boolean abort(long transID);

	public boolean register(long transID, byte eventType, long flushElement,
			byte[] data);

	public boolean register(long transID, byte eventType, long flushElement);

	public boolean register(long transID, byte eventType, byte[] data);

	public boolean notifyFlush(byte moludeType, long event);

	public long getMaxSeqClosedID();

	public long getMaxSeqCommittedID();

}
