package org.itri.ccma.server.namenode.rcmanager.interfaces;

import java.io.IOException;

import org.itri.ccma.server.namenode.interfaces.IRCMAPI4DSS;
import org.itri.ccma.server.namenode.rcmanager.RCUArrayList;

public interface IUpdateRemote extends IRCMAPI4DSS{

	//update the rcs to persistent module.
	public int doUpdate();
	
	//ack back from persisten module.
	//public int ackUpdate(long updateID);

	public long doUpdate(RCUArrayList pendingLists, int dssConsumeRate) throws IOException, ClassNotFoundException;
	
}
