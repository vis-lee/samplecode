
package org.itri.ccma.server.namenode.interfaces;





public interface IProtocolDSS4DMS {

	public static final long versionID = 531L;
	
	public int updateBackupPolicy(long volumeUUID);//// =0 if success, else fail.
	public int deleteVolumeNotify(long volumeUUID);//// =0 if success, else fail.
	
	
	/**
	 * update Reference Count to DSS
	 * @param updateID
	 * @param ackType
	 * @param totalSizeIncRCs
	 * @param incRCs
	 * @param totalSizeDecRCs
	 * @param decRCs
	 * @return	>0, total transmission
	 * 			<0, error code
	 */
	public int updateRC(long updateID, int totalSizeIncRCs, long[] incRCs,
			int totalSizeDecRCs, long[] decRCs);
	
	static final int DSS_RPC_OK = 1;
	
	/**
	 * chech DSS state
	 * @return	
	 * 			-7001 DSS DOWN
	 */
	public int checkDssState();
	
	static final int DSS_RUNNING = 1;
	static final int DSS_DOWN = -7001;
}
