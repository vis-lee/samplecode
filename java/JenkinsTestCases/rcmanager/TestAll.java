package org.itri.ccma.server.namenode.rcmanager;

import java.io.File;
import org.itri.ccma.server.namenode.DMSConstant;
import org.itri.ccma.util.DMSUtil;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class TestAll extends TestCase {
	
	public static Test suite() {
		
		
		//delete rcm folder for test
		File f = new File(DMSConstant.RCM_DIR);
		DMSUtil.deleteDirectory(f);
		
		TestSuite suite = new TestSuite();
		
		suite.addTestSuite(RCMNormalCasesTests.class);
		suite.addTestSuite(RCMultiThreadsTest.class);
		suite.addTestSuite(RCMNullRefTest.class);
		suite.addTestSuite(TestPersistDev.class);
		suite.addTestSuite(TestHA_Basic.class);
		
    	//delete rcm folder after test
		DMSUtil.deleteDirectory(f);
		
		return suite;
	}
	
	
}
