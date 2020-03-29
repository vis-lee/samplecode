package org.itri.ccma.server.namenode.rcmanager;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.itri.ccma.server.namenode.rcmanager.RCManager;

public class RCMNullRefTest extends TestEnv{

	public final Log LOG = LogFactory.getLog(RCMNullRefTest.class.getName());
	
	
	protected int nr_data = 100*1000;
	
	
	private AtomicLong curID = new AtomicLong(0);
	
	
	protected int nr_ths = 100;
	
	protected int nr_th_tasks = 1*1000;
	
	
	public RCMNullRefTest( String testName ) {
		super(testName, new RCManager(null, null, 4, 1), null, null);
	}

	

    /**
     * Main method passes this test to the text test runner.
     *
     * @param args
     */
    public static void main( String args[] )
    {
        String[] testCaseName = { RCMNullRefTest.class.getName() };
        junit.textui.TestRunner.main( testCaseName );
    }


	
	
	//******************************************************************************************//
	//																							//
	// 										TEST CASES											//
	//																							//
	//******************************************************************************************//

	public void testMultiThreadsAddTest(){
		
		resetRCMID();
		runMultiThreadsAddTest(nr_ths, nr_th_tasks);
		
	}
	
	
	
	
	
	//******************************************************************************************//
	//																							//
	// 											threads											//
	//																							//
	//******************************************************************************************//






	@Override
	int verify() {
		// TODO Auto-generated method stub
		return 0;
	}



	@Override
	void runTestFunc(int nr_tasks) {
		
		long tid = 0;
		
		while( (tid = curID.getAndIncrement()) < nr_data ){
			
			//get the content
			RCElement rce = RCMTestDataGenerator.getDisposableData();
			
			//sleep random time
			try {
				
				//sleep for a while to imitate random (out of sequence) add.
				Thread.sleep( rand.nextInt(10) );
				
				//add into list
				addToRCM(Long.MAX_VALUE, rce);
				
				if(rand.nextBoolean()){
					
					//limit to 1 sec at most.
					Thread.sleep( rand.nextInt(1*10) );
					
				}
				
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			rce = null;
			
			//LOG.debug(this.getName() + " : Finish tid = " + tid + " add work.");
			
			Thread.yield();
		}
		
	}



	@Override
	Log getLogger() {
		
		return LOG;
	}
	
	
	
	
	
	
	
	
	//******************************************************************************************//
	//																							//
	// 											END												//
	//																							//
	//******************************************************************************************//
	
}
