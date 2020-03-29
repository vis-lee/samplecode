package org.itri.ccma.server.namenode.rcmanager.interfaces;

import java.util.List;

import org.itri.ccma.server.namenode.interfaces.IRCMAPI4DSS;


public interface IRCManager /*extends IUpdateRemote*/extends IRCMAPI4DSS{

	
	public boolean flushRCManager();
	

	/**
	 * add HBs into update list
	 * @param	tranxID
	 * @size	array length
	 * @param	updateType	1  : increase
	 * 						-1 : decrease
	 * @return	0 - success
	 * 			-1 - add fail
	 * 			-2 - the transaction ID has been extinct.
	 */
	public int addRCUpdate(long tranxID, List<Long> hbids, int size, int updateType);
	
	public static final int RCU_TYPE_INC_RC = 1;
	public static final int RCU_TYPE_DEC_RC = -1;
	
	
	public int shutdownRCManager();

	
}
