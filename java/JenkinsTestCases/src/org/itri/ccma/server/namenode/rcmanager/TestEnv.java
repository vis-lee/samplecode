/**
 * 
 */
package org.itri.ccma.server.namenode.rcmanager;

import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.itri.ccma.editlog.interfaces.ITransactionManager2;
import org.itri.ccma.server.namenode.rcmanager.interfaces.IRCManager;

import junit.framework.TestCase;

/**
 * @author Vis Lee
 *
 */
public abstract class TestEnv extends TestCase {

	protected RCManager rcm;
	
	protected ITransactionManager2 ielog;
	
	protected ImitateDSSAPIs idss;
	
	protected Random rand = new Random();
	
	
	/* (non-Javadoc)
	 * @see junit.framework.TestCase#setUp()
	 */
	@Override
	protected void setUp() throws Exception {
		
		//create data set
		RCMTestDataGenerator.initDataSet();
		
		super.setUp();
	}

	/* (non-Javadoc)
	 * @see junit.framework.TestCase#tearDown()
	 */
	@Override
	protected void tearDown() throws Exception {
		
		/*
		 * remember to start updater before you call flush.
		 */
		rcm.flushRCManager();
		
		super.tearDown();
	}

	
	public TestEnv( String testName ){
		
		this(testName, null, null, null);
	}
	
	
	public TestEnv( String testName, RCManager rcm, ITransactionManager2 ielog, ImitateDSSAPIs idss){
		
		super(testName);
		
		if(ielog == null){
			ielog = new ImitateEditlog();
		}
		this.ielog = ielog;
		
		
		//init DSSAPIs
		if(idss == null){
			idss = new ImitateDSSAPIs();
		}
		this.idss = idss;
		
		//set update interval to 1 second
		if(rcm == null){
			rcm = new RCManager(ielog, idss, 4, (long)1);
		}
		this.rcm = rcm;
		
		idss.setRcm(this.rcm);
		
		//set iLAPI to my rcm
		this.rcm.getPersistDev().setiLAPI(this.rcm);

		this.rcm.startUpdater();
	}
	
	
	
	
	protected void addToRCM( long tranxID, RCElement rce ){
		
		int result = -1;
		
		result = rcm.addRCUpdate(tranxID, rce.getIncHBIDs(), rce.getIncHBIDsSize(), IRCManager.RCU_TYPE_INC_RC);
		
		//check if the array isn't empty
		if( rce.getIncHBIDsSize() != 0 ){
			assertTrue("add inc rcs fail, result = " + result, result == 0);
		}
		
		
		result = rcm.addRCUpdate(tranxID, rce.getDecHBIDs(), rce.getDecHBIDsSize(), IRCManager.RCU_TYPE_DEC_RC);
		
		if( rce.getDecHBIDsSize() != 0 ){
			assertTrue("add dec rcs fail, result = " + result, result == 0);
		}
		
	}
	
	
	protected void varifyData(long tranxID){
		
		//get original data
		RCElement rc_orig = RCMTestDataGenerator.getRecordData( tranxID );
		
		//get comparison data
		RCElement stored = rcm.getRCElement( tranxID );
		
		if(stored == null){
			stored = rcm.getRCElement( tranxID );
		}
		
		assertTrue("source data is null!", rc_orig != null);
		assertTrue("rcm element is null!", stored != null);
		
		assertTrue( "the rc_orig = " + rc_orig.toString() + ", the stored = " + stored.toString(), rc_orig.equals(stored) );
		
	}
	
	

	protected void resetRCMID(){
		
		rcm.resetRCMID();
	}
	
	
	//******************************************************************************************//
	//																							//
	// 											threads											//
	//																							//
	//******************************************************************************************//
	

	public void runMultiThreadsAddTest(int nr_ths, int nr_th_tasks){
		
		Log LOG = getLogger();
		
		if(LOG == null){
			LOG = LogFactory.getLog(this.getName());
		}
		
		LOG.info("runMultiThreadsAddTest start~");
		
		
		
		Adder[] adders = new Adder[nr_ths];
		
		for(int i = 0; i < nr_ths; i++){
			
			adders[i] =  new Adder(i, nr_th_tasks);
			
		}
		
		for(int i = 0; i < nr_ths; i++){
			
			adders[i].start();
			
		}

		for(int i = 0; i < nr_ths; i++){
			
			try {
				adders[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
		}
		
		
		LOG.info("waiting for updater done~");
		
		//check whether RCM clean all lists.
		while(rcm.size() != 0){
			
			LOG.info("rcm.size() = " + rcm.size());
			
			try {
				Thread.sleep(1*1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		verify();
		
		LOG.info("runMultiThreadsAddTest finish~");
		
		
	}
	
	
	
	
	
	public class Adder extends Thread{
		

		int nr_tasks = Integer.MAX_VALUE;
		
		
		public Adder(int i, int nr_tasks) {
			
			super("Adder ["+i+"]");
			
			if(nr_tasks > 0){
				this.nr_tasks = nr_tasks;
			}
		}

		public void run() {
			
			runTestFunc(nr_tasks);
		}
		
		
	}
	
	
	
	abstract int verify();
	abstract void runTestFunc(int nr_tasks);
	abstract Log getLogger();
	
	
	
	void default_runTestFunc(int nr_tasks) {
		
		long tid = 0;
		
		while( nr_tasks-- > 0 ){
			
			tid = ielog.init();
			
			Long TID = new Long(tid);
			
			//get the content
			RCElement rce = RCMTestDataGenerator.getRecordData(TID);
			
			//sleep random time
			try {
				//sleep for a while to imitate random (out of sequence) add.
				Thread.sleep( rand.nextInt(10) );
				
				//add into list
				addToRCM(tid, rce);
				
				if(rand.nextBoolean()){
					
					//limit to 1 sec at most.
					Thread.sleep( rand.nextInt(1*10) );
					
				}
				
				//commit to edlog
				ielog.commit(tid);
				
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			rce = null;
			TID = null;
			
			//LOG.debug(this.getName() + " : Finish tid = " + tid + " add work.");
			
			Thread.yield();
		}
		
	}
	
	
	void waitForFileModeDone(){
		
		Log LOG = getLogger();
		int numFiles = 0;
		
		PersistToFile fileDev = rcm.getFileDev();
		
		//check file task state
		while( (numFiles = fileDev.checkState()) > 0){
			
			try {
				
				//sleep 3 secs
				Thread.sleep(3*1000);
				
				LOG.debug("remain files = " + numFiles);
				
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		LOG.info(this.getName() + " finish~");
	}
	
	
}
