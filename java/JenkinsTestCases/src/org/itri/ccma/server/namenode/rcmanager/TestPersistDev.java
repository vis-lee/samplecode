/**
 * 
 */
package org.itri.ccma.server.namenode.rcmanager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author Vis Lee
 *
 */
public class TestPersistDev extends TestEnv {

	
	public final Log LOG = LogFactory.getLog(this.getName());
	
	private DevController dc;
	
	protected int nr_ths = 100;
	
	protected int nr_th_tasks = 1*1000;
	
	
	public TestPersistDev(String testName) {
		super(testName);
		// TODO Auto-generated constructor stub
	}
	
    /**
     * Main method passes this test to the text test runner.
     *
     * @param args
     */
    public static void main( String args[] )
    {
        String[] testCaseName = { TestPersistDev.class.getName() };
        junit.textui.TestRunner.main( testCaseName );
    }
    

	//******************************************************************************************//
	//																							//
	// 										TEST CASES											//
	//																							//
	//******************************************************************************************//
    
    private void setTestParameters(){
    	
    	//set max retry count
		RCPersistance rcp = rcm.getPersistDev();
		rcp.setMaxRetry(10);
		
		//redirect the ackAPI entry to rcm, because we didn't run whole namenode.
		rcp.setiLAPI(rcm);
		rcp.switchToRemoteMode();
		
		//clear folder
		PersistToFile fileDev = rcm.getFileDev();
		fileDev.cleanPersistDev();
		
    }
    
    
    private void dcJoin(){
    	
    	try {
			
			//wait dc finish;
			dc.join();
			
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
    }
    
    /**
     * simple case:
     * 	1. start multi threads to add data to RCM
     *  2. wait 10 secs
     *  3. set dss status to down
     *  4. the consequence updates should be store to files
     *  5. after 40 seconds
     *  6. set dss status to running
     *  7. the File Cleaner should start to read files and send updates to dss.
     *     (there are no updater activity in this test)
     */
	public void testPersistDevSimple(){
		
		setTestParameters();
		
		//start DevController
		dc = new DevController();
		dc.start();
		
		
		//reset the RCMID
		resetRCMID();
		
		//start adders
		runMultiThreadsAddTest(nr_ths, nr_th_tasks);
		
		
		dcJoin();
		
	}

	
	
	/**
	 * additional behavior: perform multi-threading add again while toFile flag is true 
	 * and remote device come back to running state which means FC is sending files to remote.
	 */
	public void testPersistDevNormal(){
		
		setTestParameters();
		
		//start DevController
		dc = new DevControllerNormal();
		dc.start();
		
		//reset the RCMID
		resetRCMID();
		
		//start adders
		runMultiThreadsAddTest(nr_ths, nr_th_tasks);
		
		
		dcJoin();
		
	}
    

	/* (non-Javadoc)
	 * @see org.itri.ccma.server.namenode.rcmanager.TestEnv#varify()
	 */
	@Override
	int verify() {
		
		waitForFileModeDone();
		
		return 0;
	}

	/* (non-Javadoc)
	 * This function exactly same as the RCMultiThreadsTest
	 * @see org.itri.ccma.server.namenode.rcmanager.TestEnv#runTestFunc(int)
	 */
	@Override
	void runTestFunc(int nr_tasks) {
		
		default_runTestFunc(nr_tasks);

	}

	/* (non-Javadoc)
	 * @see org.itri.ccma.server.namenode.rcmanager.TestEnv#getLogger()
	 */
	@Override
	Log getLogger() {
		
		return LOG;
	}
	
	
	

	/**
	 * Device controller will set remote device down for testing the interact between PersistToFile and PersistToRemote.
	 * case 1 - it should write the request to a file while the remote device down.
	 * case 2 - and controller set the device status to running after a long period to see whether the PersistDev sending files to the device or not.
	 * @author Vis Lee
	 *
	 */
	public class DevController extends Thread{
		

		int nr_tasks = Integer.MAX_VALUE;
		
		
		public DevController() {
			
			super("DevController");
			
		}

		public void run() {
			
			try {
				
				LOG.info("start DevController");
				
				controlFlow();
				
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		protected void controlFlow() throws InterruptedException {

			
			//sleep 10 secs
			Thread.sleep(10*1000);
			
			LOG.info("set DSS state to \"down\"! ");
			
			//set dss down
			idss.setDSSDown();
			
			
			
			//sleep 40 secs
			Thread.sleep(40*1000);

			LOG.info("set DSS state to \"running\"! ");
			
			//set dss run
			idss.setDSSRunning();
			
			
			
		}
		
		
		
		
	}
	
	
	

	/**
	 * Device controller will set remote device down for testing the interact between PersistToFile and PersistToRemote.
	 * case 1 - it should write the request to a file while the remote device down.
	 * case 2 - and controller set the device status to running after a long period to see whether the PersistDev sending files to the device or not.
	 * @author Vis Lee
	 *
	 */
	public class DevControllerNormal extends DevController{
		

		int nr_tasks = Integer.MAX_VALUE;
		
		
		public void run() {
			
			try {
				
				LOG.info("start DevControllerNormal");
				
				controlFlow();
				
				//wait 6 secs
				Thread.sleep(6*1000);
				
				//start multi-threads again to simulate normal case
				runMultiThreadsAddTest(nr_ths, nr_th_tasks);
				
			} catch (InterruptedException e) {
				
				e.printStackTrace();
			}
		}
		
		
	}
	
	
	
	
	
	
	
	
	
	

}
