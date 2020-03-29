package org.itri.ccma.server.namenode.rcmanager;

import java.util.LinkedList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class RCMNormalCasesTests extends TestEnv{

	public final Log LOG = LogFactory.getLog(RCMNormalCasesTests.class.getName());
	
	protected int nr_data = 100*1000;
	
	
	public RCMNormalCasesTests( String testName ) {
		super(testName, null, new ImitateEditlog(), null);
	}

	

    /**
     * Main method passes this test to the text test runner.
     *
     * @param args
     */
    public static void main( String args[] )
    {
        String[] testCaseName = { RCMNormalCasesTests.class.getName() };
        junit.textui.TestRunner.main( testCaseName );
    }

	

	
	
	
	//******************************************************************************************//
	//																							//
	// 										TEST CASES											//
	//																							//
	//******************************************************************************************//
	
	
	public void testRunAddTest(){
		
		testRunAddTest(nr_data);
		
	}
	
	
	public void testRunAddTest( int numAdds ){
		
		rcm.stopUpdater();
		
		rcm.resetRCMID();
		
		for(int i = 0; i < numAdds; i++){
			
			addToRCM( ielog.init(), RCMTestDataGenerator.getRecordData((long) i) );
			
		}
		
		LOG.info(this.getName() + " add finished");
		
		
		long tid = 0;
		
		
		LinkedList<Long> pedids = ((ImitateEditlog)ielog).getPendingIDs();
		
		//get and check
		while(pedids.size() > 0){
			
			tid = pedids.poll();
			varifyData( tid );
			ielog.commit( tid );
			
		}
		
		LOG.info(this.getName() + " varify data finished");
		
		doUpdateRCListTest();
		
	}
	
	public void doUpdateRCListTest(){
		
		rcm.initUpdater(1);
		
		rcm.startUpdater();
		
		
		//check whether RCM clean all lists.
		while(rcm.size() != 0){
			
			LOG.info(this.getName() + " rcm.size() = " + rcm.size());
			
			try {
				Thread.sleep(1*1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		LOG.info(this.getName() + " doUpdateRCListTest finished");

	}



	@Override
	int verify() {
		// TODO Auto-generated method stub
		return 0;
	}



	@Override
	void runTestFunc(int nr_tasks) {
		// TODO Auto-generated method stub
		
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
