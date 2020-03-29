package org.itri.ccma.server.namenode.rcmanager;

import java.io.IOException;
import java.util.Random;

import org.itri.ccma.server.namenode.interfaces.IProtocolDSS4DMS;

public class FakeDSSAPIs implements IProtocolDSS4DMS {

	
	private RCManager rcm = null;
	
	private Random rand = new Random();
	
	
	
	public FakeDSSAPIs() {
		super();
		
	}


	public FakeDSSAPIs(RCManager rcm) {
		super();
		
		this.rcm = rcm;
		
	}



	/**
	 * @return the rcm
	 */
	public RCManager getRcm() {
		return rcm;
	}






	/**
	 * @param rcm the rcm to set
	 */
	public void setRcm(RCManager rcm) {
		this.rcm = rcm;
	}






	@Override
	public int updateRC(long updateID, int totalSizeIncRCs, long[] incRCs,
			int totalSizeDecRCs, long[] decRCs) {

		Ack2RCM acker = new Ack2RCM(updateID);
		acker.start();
		
		return DSS_RPC_OK;
	}
	


//	@Override
//	public long getProtocolVersion(String protocol, long clientVersion)
//			throws IOException {
//		// TODO Auto-generated method stub
//		return 0;
//	}






	@Override
	public int updateBackupPolicy(long volumeUUID) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	

	
	class Ack2RCM extends Thread{
		
		long updateID = -1;
		
		
		
		public Ack2RCM(long updateID) {
			super();
			this.updateID = updateID;
		}



		public void run(){
			
			if(rcm != null){
				
				//random wait time
				try {
					Thread.sleep(rand.nextInt(1));
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
				rcm.ackRCUpdate(updateID);
			}
			
		}
		
	}




	@Override
	public int checkDssState() {
		// TODO Auto-generated method stub
		return DSS_RUNNING;
	}


	@Override
	public int deleteVolumeNotify(long volumeUUID) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	
	
}
