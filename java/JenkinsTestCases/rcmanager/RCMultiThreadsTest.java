package org.itri.ccma.server.namenode.rcmanager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class RCMultiThreadsTest extends TestEnv{

	public final Log LOG = LogFactory.getLog(RCMultiThreadsTest.class.getName());
	
	protected int nr_ths = 100;
	
	protected int nr_th_tasks = 1*1000;
	
	
	
	public RCMultiThreadsTest( String testName ) {
		super(testName);
	}

	

    /**
     * Main method passes this test to the text test runner.
     *
     * @param args
     */
    public static void main( String args[] )
    {
        String[] testCaseName = { RCMultiThreadsTest.class.getName() };
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

	

	@Override
	int verify() {
		// TODO Auto-generated method stub
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
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	//******************************************************************************************//
	//																							//
	// 											END												//
	//																							//
	//******************************************************************************************//
	
}
