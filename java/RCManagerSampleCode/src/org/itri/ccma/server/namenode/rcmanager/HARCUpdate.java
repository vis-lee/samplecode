/**
 * 
 */
package org.itri.ccma.server.namenode.rcmanager;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.itri.ccma.editlog.interfaces.IEditlogModule;
import org.itri.ccma.editlog.manager2.EditlogEngine;
import org.itri.ccma.server.namenode.DMSConstant;
import org.itri.ccma.server.namenode.DMSFlags;
import org.itri.ccma.server.namenode.rcmanager.interfaces.IRCUPersistance;

import junit.framework.Assert;

/**
 * @author Vis Lee
 *
 */
public class HARCUpdate implements IEditlogModule {


	public final Log LOG = LogFactory.getLog(this.getClass().getName());
	
	private RCManager rcm = null;
	
	public HARCUpdate(RCManager rcm) {
		
		if(DMSFlags.ENABLE_EDITLOG){
			
			//register updater module to elog
			EditlogEngine.registerModule(DMSConstant.MODULE_ID_RCUpdate, this, this.getClass().getName());
			
		}
		
		this.rcm = rcm;
	}
	

	@Override
	public void undo(byte[] data) throws Exception {
		// do nothing

	}

	protected Integer stroeToFileJob(RCUArrayList ul) throws IOException, ClassNotFoundException{
		
		int retcode = -1;
		int rCount = 3;
		
		RCPersistance pdev = rcm.getPersistDev();
		
		pdev.acquireLock();
		pdev.switchToFileMode();
		
		while( (retcode != IRCUPersistance.DEV_WRITE_DONE) && (rCount > 0) ){
			retcode = rcm.doUpdate(ul);
			rCount--;
		}
		
		if( retcode != IRCUPersistance.DEV_WRITE_DONE ){
			
			String s = new String();
			
			switch(retcode){
				case IRCUPersistance.DEV_WRITE_FAIL:
					s = "DEV_WRITE_FAIL";
					break;
				case IRCUPersistance.DEV_WRITE_EXIST:
					s = "DEV_WRITE_EXIST";
					break;
				default:
					s = "UNKOWN ERROR CODE!";
			}
			
			LOG.error("Recover Fail~! NO~ God bless you... retcode = " + s);
		}
		
		
		pdev.releaseLock();
		
		return retcode;
		
	}
	
	protected int stroeToFile(RCUArrayList ul) throws Exception{
		
		ExecutorService executor = Executors.newFixedThreadPool(1);
		FutureTask<Integer> future = new FutureTask<Integer>(
                new FileStorer(ul)
                );
		
        executor.execute(future);
        
        future.isDone();
        
        executor.shutdown();
        
		return future.get();
		
	}

	@Override
	public void redo(byte[] data) throws Exception {
		
		RCUArrayList ul = new RCUArrayList();

		ul.setLogInfo(data);
		
		if(LOG.isDebugEnabled()){
			
			LOG.info(ul.toString());
		}
		
		//update info from edit log record
		long ruid = ul.getUpdateID();
		long rsTranxID = ul.getsTranxID();
		long reTranxID = ul.geteTranxID();
		
		ul = rcm.getPendingList();
		
		if(rcm.setupUpdate(ul, reTranxID, Integer.MAX_VALUE) > 0){

			if(LOG.isDebugEnabled()){
				//the generated update info from collectRCElements should be as the same as record 
				Assert.assertTrue("orig_uid = " + ruid + ", uid = " + ul.getUpdateID(),
						ruid == ul.getUpdateID());
				Assert.assertTrue("orig_rStranxID = " + rsTranxID + ", stranxID = " + ul.getsTranxID(),
						rsTranxID == ul.getsTranxID());
				Assert.assertTrue("orig_reTranxID = " + reTranxID + ", eTranxID = " + ul.geteTranxID(),
						reTranxID == ul.geteTranxID());
			}
			
			if( stroeToFile(ul) < 0 ){
				LOG.error("help! You got a Bbbiiiggg~~~ troble here! " +
						"Store to File FAILLLLLL~\n UL = " + ul.toString());
			}
			
		}else{
			
			/*
			 * this case is no elements we can collect from RCM which is 
			 * because RCMID updated eTranxID before crash, so updateID is old.
			 * Therefore, we correct updateID here.
			 */
			
			if(reTranxID == rcm.geteTranxID()){
			
				//update eTranxID again to cause updateID increase.
				rcm.updateID(reTranxID);
			
			}else{
				
				LOG.error("FATAL Error! I have no idea about why you come here...");
			}
			
		}
		

	}

	@Override
	public boolean flushTimeout(long flushElement) {

		LOG.warn("Hello stranger, you shouldn't see this message, but if you do... " +
				"that means somthing corruptted and I don't know why. " +
				"actually we set our timeout value to 1min'30sec in default, " +
				"therefore we should handled all cases before editlog timeout. " +
				"But you are here, so you can understand something out of control... " +
				"maybe check the timeout value of the updater which should be 1 minute " +
				"and the timeout value of the editlog which should be 2 minutes. good luck~");
		
		//try to simply switch to file mode
		RCPersistance pdev = rcm.getPersistDev();
		
		LOG.warn("try to swtich RCUPersistant device to FileMode!");
		
		pdev.acquireLock();
		pdev.switchToFileMode();
		pdev.releaseLock();
		
		//wait for flush
		rcm.flushRCManager();
		
		return true;
	}

	
	class FileStorer implements Callable<Integer>{
		
		RCUArrayList ul;
		
		public FileStorer(RCUArrayList ul) {
			super();
			this.ul = ul;
		}

//		public void run(){
//			
//			try {
//				int retcode = stroeToFile(ul);
//			} catch (IOException e) {
//				e.printStackTrace();
//				LOG.error("help! You got a Bbbiiiggg~~~ troble here!\n UL = " + ul.toString());
//			} catch (ClassNotFoundException e) {
//				e.printStackTrace();
//				LOG.error("help! You got a Bbbiiiggg~~~ troble here!\n UL = " + ul.toString());
//			}
//		}

		@Override
		public Integer call() throws Exception {
			return stroeToFileJob(ul);
		}
	}
	
}
