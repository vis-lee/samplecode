package org.itri.ccma.server.namenode.rcmanager;

import java.io.IOException;
import java.util.Random;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.itri.ccma.dms_dss_common.protocol.IProtocolDSS4DMS;
import org.itri.ccma.server.namenode.DMSConstant;

public class ImitateDSSAPIs implements IProtocolDSS4DMS {

	public final Log LOG = LogFactory.getLog(RCMultiThreadsTest.class.getName());
	
	
	private RCManager rcm = null;
	
	private Random rand = new Random();
	
	private int DSS_STATE = DSS_RUNNING;
	
	
	public ImitateDSSAPIs() {
		super();
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




	/**
	 * @return the dSS_STATE
	 */
	public int getDSS_STATE() {
		return DSS_STATE;
	}






	/**
	 * Set dss down for testing.
	 */
	public void setDSSDown() {
		DSS_STATE = DSS_DOWN;
	}

	/**
	 * Set dss running for testing.
	 */
	public void setDSSRunning() {
		DSS_STATE = DSS_RUNNING;
	}





	@Override
	public int updateRC(long updateID, int totalSizeIncRCs, long[] incRCs,
			int totalSizeDecRCs, long[] decRCs) {

		Ack2RCM acker = new Ack2RCM(updateID);
		acker.start();
		
		return DSS_RPC_OK;
	}
	
	
	
	void checkTransportContentCorrection(){
		
		
		//check the content
		if(RCMTestDataGenerator.getSize()> 0){
			
			//get pending list
			RCUArrayList list = rcm.getPendingList();
			
			RCElement orig = null;
			
			for(RCElement e : list){
				
				Long id = new Long(e.getTranxID());
				
				//get original content from data set
				orig = RCMTestDataGenerator.getRecordData( id );
				
				Assert.assertTrue(("RC element is not match, rce of dataset = " + orig.toString()
							+ ", rce of pending list = " + e.toString()), orig.equals(e) );
				
				//release reference
				orig = null;
				
				RCMTestDataGenerator.removeRecordData(id);
				
				id = null;
				
			}
			
		}
		
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
					Thread.sleep(rand.nextInt(DMSConstant.RCM_UPDATE_PERIODMS*2));
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				checkTransportContentCorrection();
				
				rcm.ackRCUpdate(updateID);
			}
			
		}
		
		
		
		
	}


	@Override
	public long getProtocolVersion(String protocol, long clientVersion)
			throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}



	@Override
	public int updateBackupPolicy(long volumeUUID) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int checkDssState() {

		return DSS_STATE;
	}






	@Override
	public int deleteVolumeNotify(long volumeUUID) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	

}
