/**
 * 
 */
package org.itri.ccma.server.namenode.rcmanager;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * @author Vis Lee
 * This test case is used to do a stress test for infinite time.
 */
public class InfiniteMThreadsTest extends RCMultiThreadsTest {

	
	
	public InfiniteMThreadsTest(String testName) {
		super(testName);
		// TODO Auto-generated constructor stub
	}
	
	

    /**
     * A unit test suite for JUnit
     *
     * @return The test suite
     */
    public static Test suite()
    {
    	
    	//TestSuite suite = new TestSuite(RCMNormalCasesTests.class);
    	TestSuite suite = new TestSuite();
    	
    	suite.addTest( 
    			new InfiniteMThreadsTest("testInfiniteMThreadsTest") 
			    	{
    					public void runTest() throws Exception
		                {
    						resetRCMID();
    						this.runMultiThreadsAddTest(nr_ths, -1);
		                }
			    	} 
    			);
    	
		return suite;
    	
    }
	
	
}
