package org.itri.ccma.server.namenode.rcmanager;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.itri.ccma.editlog.manager2.TransactionManager_Fake;

public class TestHA_Basic extends TestEnv{

	
	public final Log LOG = LogFactory.getLog(this.getName());
	
	private RCUArrayList tl;
	private int ulSize = 1000;
	
	private HARCElement harce;
	private HARCUpdate harcu;
	
	protected int nr_ths = 100;
	protected int nr_th_tasks = 1*1000;
	
	public TestHA_Basic(String testName) {
		super(testName, null, TransactionManager_Fake.getInstance(), null);
		initHA();
	}
	
	public void initHA(){
		
		harce = rcm.harce;
		harcu = rcm.harcu;
		
		//set dss down
		idss.setDSSDown();
		
	}
	
	public void testHA_Basic(){
		
		//create elog data...
		tl = new RCUArrayList();
		
		//build test env
		long stid = 0;
		while( (stid = rand.nextLong()) < 0 );
		
		rcm.setStartIndexID(stid-1);
		rcm.stopUpdater();
		
		//undo test
		LOG.info("start Undo Test...");
		testRCRecovery_Undo(stid, tl, ulSize);

		//set updateID
		tl.setUpdateID(rcm.getUpdateID());
		tl.setsTranxID(stid);
		tl.seteTranxID(stid+ulSize-1);
		
		
		//redo
		LOG.info("start Redo Test...");
		testRCRecovery_Redo(stid, tl);
		
		//check result
		LOG.info("Checking result...");
		haVerify_Basic(tl);
		
		
		//test after recovery
		rcm.initUpdater(1);
		LOG.info("Test rcm initAfterRecovery()...");
		rcm.initAfterRecovery();
		
		
		//test normal flow
		normal_flow();
		
		LOG.info("Test Done...");
		return;
	}


	private void normal_flow() {
		
		// build env
		this.ielog = new ImitateEditlog();
		rcm.swapELog(ielog);
		idss.setDSSRunning();
		
		/*
		 * start normal case test
		 */
		try {
			//wait 1 secs
			Thread.sleep(1*1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		//start multi-threads again to simulate normal case
		runMultiThreadsAddTest(nr_ths, nr_th_tasks);
	}

	private void haVerify_Basic(RCUArrayList tl) {
		
		//read recovered list from persist
		PersistToFile toFile = rcm.getFileDev();
		
		try {
			RCUArrayList hal = toFile.read(tl.getUpdateID());
			
			assertTrue("HA base case fail: test list = " + tl.toString()
					+ ", ha_list = " + hal.toString(), tl.equals(hal) );
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
	}

	private void testRCRecovery_Undo(long stid, RCUArrayList tl, int ulsize) {

		
		for(int i = 0; i < ulsize; i++){
			
			Long TID = new Long(stid+i);
			
			//generate test content and fill to update list
			RCElement rce = RCMTestDataGenerator.getRecordData(TID);
			
			//add to test list
			tl.add(rce);
			
			try {
				harce.undo(null);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			//fill to rcm
			//addToRCM(TID, rce);
			
		}
		
		//test ul undo
		try {
			harcu.undo(null);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public void testRCRecovery_Redo(long stid, RCUArrayList tl){
		
		try {
			int i = 0;
			
			//simulate redo from caller
			for(RCElement e : tl){
				
				addToRCM( stid + i++, e );
			}
			
			/*
			 * call HA_RCU handler in base case.
			 */
			byte[] tlLog = tl.getLogInfo();
			harcu.redo(tlLog);
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
		
	}

	@Override
	int verify() {
		waitForFileModeDone();
		return 0;
	}

	@Override
	void runTestFunc(int nr_tasks) {
		default_runTestFunc(nr_tasks);
		
	}

	@Override
	Log getLogger() {
		return LOG;
	}

}
