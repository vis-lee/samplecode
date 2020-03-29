package org.itri.ccma.editlog.interfaces;

public interface ITransactionManager2 {

	public long init(); // return runtimeID

	public long init(long timeoutSecondValue); //return runtimeID

	public boolean commit(long runtimeID);

	public boolean abort(long runtimeID);

	public long registerEvent(long runtimeID, byte moduleType, long event); // return seqID

	public long registerEvent(long runtimeID, byte moduleType, long[] event); //return seqID
	
	public boolean log(long runtimeID, byte moduleType, byte[] data); 

	public boolean notifyFlush(byte moduleType, long event, long seqID);

	public long getMaxSeqClosedID();

	public long getMaxSeqCommittedID();

	public void shutdown();
	
	public static final int DEFAULT_INIT_ID = 0;

}