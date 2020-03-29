/**
 * 
 */
package org.itri.ccma.server.namenode.rcmanager;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.itri.ccma.server.namenode.interfaces.IRCMAPI4DSS;
import org.itri.ccma.server.namenode.rcmanager.exception.NoSupportMethodException;
import org.itri.ccma.server.namenode.rcmanager.interfaces.IRCUPersistance;
import org.itri.ccma.server.namenode.DMSConstant;

/**
 * @author Vis Lee
 * 
 */
public class RCPersistance {

	public final Log LOG = LogFactory.getLog(RCPersistance.class.getName());

	private IRCUPersistance toRemote;
	private IRCUPersistance toFile;

	// interface for Local API
	protected IRCMAPI4DSS iLAPI;

	protected int retry = 0;
	protected int maxRetry = DMSConstant.RCM_MAX_RETRY;

	// indicate state change.
	private AtomicBoolean toFileFlag = new AtomicBoolean(false);

	
	public static final int RCASE_INC = 1;
	public static final int RCASE_RESET = 0;

	public static final long cleaner_interval = 5 * 1000;
	
	public FileCleaner fc = null;
	
	public ReadWriteLock onGoing;
	
	
	
	public static final int ACKTYPE_REMOTE = 128;
	public static final int ACKTYPE_FILE = 256;

	public RCPersistance(IRCMAPI4DSS iLAPI,
			IRCUPersistance toRemote, IRCUPersistance toFile) {

		this(iLAPI, toRemote, toFile, -1);
	}

	public RCPersistance(IRCMAPI4DSS iLAPI,
			IRCUPersistance toRemote, IRCUPersistance toFile, int maxRetry) {

		super();
		this.iLAPI = iLAPI;
		this.toRemote = toRemote;
		this.toFile = toFile;

		if (maxRetry > 0) {
			this.maxRetry = maxRetry;
		}
		
		onGoing = new ReentrantReadWriteLock();
		
		//there are files...
		if (toFile.checkState() > 0){
			switchToFileMode();
		}
		
	}


	/**
	 * @return the toFileFlag
	 */
	public boolean isToFileFlag() {
		return this.toFileFlag.get();
	}

	/**
	 * @param toFileFlag
	 *            the toFileFlag to set
	 */
	private void setToFileFlag(boolean toFileFlag) {
		this.toFileFlag.set(toFileFlag);
	}
	
//	public boolean getToFileFlag() {
//		return this.toFileFlag.get();
//	}
	

	protected void switchToFileMode() {

		// set toFile if retry exceeds the limits
		setToFileFlag(true);
		
		// also start the fc
		startFileCleaner();
		
	}
	
	protected void switchToRemoteMode() {

		// set toFile if retry exceeds the limits
		setToFileFlag(false);
		
	}
	
	
	/**
	 * @param iLAPI the iLAPI to set
	 */
	protected void setiLAPI(IRCMAPI4DSS iLAPI) {
		this.iLAPI = iLAPI;
	}

	
	
	/**
	 * @return the maxRetry
	 */
	protected int getMaxRetry() {
		return maxRetry;
	}

	/**
	 * @param maxRetry the maxRetry to set
	 */
	protected void setMaxRetry(int maxRetry) {
		this.maxRetry = maxRetry;
	}
	
	
	

	public void acquireLock(){
		
		onGoing.writeLock().lock();
	}
	
	public void releaseLock(){
		
		onGoing.writeLock().unlock();
	}
	
	
	
	private void startFileCleaner(){
		
		if(fc == null){
			
			synchronized(this){
				
				if(fc == null){
					fc = new FileCleaner();
					//set higher priority
					fc.setPriority((Thread.MAX_PRIORITY-Thread.NORM_PRIORITY)/2);
					fc.start();
				}
			}
		}
		
	}

	//******************************************************************************************//
	//																							//
	//										to Remote											//
	//																							//
	//******************************************************************************************//

	private int persistToRemote(RCUArrayList pendingLists) throws IOException {

		int retcode = -1;

		// if there is files need to handle
		if (isToFileFlag()/*toFile.checkState() == IRCUPersistance.DEV_RUNNING*/) {

			/*
			 * handle the prior files.
			 */
			// store pendinglist to file first
			retcode = persistToFile(pendingLists);

		} else {

			// to dss
			retcode = toRemote.write(pendingLists);
			
			if(retcode > 0){
				
				// reset retry counter
				this.retry = 0;
			}
		}

		
		return retcode;

	}

	private long remoteComplete(long updateID)
			throws NoSupportMethodException {

		long retcode = -1;

		retcode = toRemote.complete(updateID);
		
		return retcode;

	}

	//******************************************************************************************//
	//																							//
	//										to files											//
	//																							//
	//******************************************************************************************//

	private int persistToFile(RCUArrayList pendingList) throws IOException {

		int retcode = -1;

		if (isToFileFlag()) {

			// set params in DSSDev
			toRemote.prepareData(pendingList);
			
			// store pendinglist to file, and should be acknowledge to ACKTYPE_REMOTE
			retcode = toFile.write(pendingList);

			switch(retcode){
			
				case IRCUPersistance.DEV_WRITE_DONE:
					
					// commit from DMSAPI entry
					Ack2RCM acker = new Ack2RCM(Thread.currentThread(), pendingList.getUpdateID(), ACKTYPE_REMOTE);
					acker.start();
					break;
					
				case IRCUPersistance.DEV_WRITE_FAIL:
					
					// recover
					toFile.cleanData(pendingList.getUpdateID());
					break;
					
				case IRCUPersistance.DEV_WRITE_EXIST:
					
					// try to fix it
					retcode = compareWithFileContent(pendingList);
					break;
					
				default:
					break;
			
			}
			
		}

		return retcode;
	}

	private RCUArrayList readFromFile() throws IOException,
			ClassNotFoundException {

		RCUArrayList lists = null;

		if (isToFileFlag()) {

			lists = toFile.read();

		} else {

			LOG.error("You want to read from file, but it's not in toFile mode, toFileFlag = "
					+ isToFileFlag());
		}

		return lists;
	}

	private long fileComplete(long updateID)
			throws NoSupportMethodException {

		long retcode = -1;

		retcode = toFile.complete(updateID);
		
		return retcode;

	}
	
	private int compareWithFileContent(RCUArrayList pendingList){
		
		int retcode = -1;
		
		try {
			
			/*
			 * lock still be holden here. so we can read directly.
			 */
			RCUArrayList fl = toFile.read();
			
			//compare them
			if(pendingList.equals(fl)){
				
				//they are the same list. correct.
				retcode = IRCUPersistance.DEV_WRITE_DONE;
				
			}else{
				
				LOG.error("the updateID of updating list is duplicated, but content is different!\n"
						+ "UL = " + pendingList.toString() + "\n"
						+ "FL = " + fl.toString());
				
				//caller should handle this case by increase updateID.
				retcode = IRCUPersistance.DEV_WRITE_EXIST;
			}
			
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			retcode = IRCUPersistance.DEV_READ_FAIL;
		} catch (NoSupportMethodException e) {
			e.printStackTrace();
			retcode = IRCUPersistance.DEV_READ_FAIL;
		} catch (IOException e) {
			e.printStackTrace();
			retcode = IRCUPersistance.DEV_READ_FAIL;
		} finally {
			
			if(retcode == IRCUPersistance.DEV_READ_FAIL){
				//file corrupted, clean it.
				toFile.cleanData(pendingList.getUpdateID());
			}
		}
		
		return retcode;
		
	}

	//******************************************************************************************//
	//																							//
	//										Main entry 											//
	//																							//
	//******************************************************************************************//

	/*
	 * Entry for updater. only allow 1 persist device active simultaneously.
	 * and there are 4 cases need to be handled here:
	 * 1. remote device is running and toFile flag is false:
	 * 		this is normal case, data persist to remote and acknowledge from remote.
	 * 
	 * 2. remote device is down and toFile flag is false:
	 * 		start to count retry counter while dss down and return false to caller.
	 * 		caller should do error handling here to put the data back to it's container.
	 * 
	 * 3. remote device is down and toFile flag is true:
	 * 		this means we already switch to file mode which will persist data to file
	 * 		and (*) call acknowledge from the entry of IRCMAPI4DSS, ex: namenode, to
	 * 		complete the normal remote flow, like notify elog to flush this transaction.
	 * 		also, start FileCleaner to detect the remote state and send files to remote
	 * 		while remote is coming back.
	 * 
	 * 4. remote device is running and toFile flag is true:
	 * 		this is a temporary state but complicate case because updater and FC active
	 * 		simultaneously. Therefore, a lock is used to prevent make sure only 1 device
	 * 		is acting. 
	 * 		updater would try to update data to device and we would persist to file in 
	 * 		this state because the update should be FIFO sequence.
	 * 		FC would try to update data to remote ASAP. It will set toFile flag to false
	 * 		to indicate the persist mode switch from file mode back to remote mode.
	 * 
	 * 
	 */
	public int persist(RCUArrayList pendingLists) throws IOException {

		int retcode = -1;

		try {
			// this should be the most case.
			if (toRemote.checkState() == IRCUPersistance.DEV_RUNNING) {
				
				retcode = persistToRemote(pendingLists);
				
			} else {
	
				// to file
				retcode = persistToFile(pendingLists);
			}
			
		} catch (IOException e) {

			e.printStackTrace();
			throw e;

		} finally {

			if (retcode < 0) {
				retryHandler(RCASE_INC);
			}
			
		}

		
		return retcode;

	}

	//entry for updater and fileCleaner
	public long complete(long updateID) throws NoSupportMethodException {

		/*
		 * we use updateID to identify this ACK belong to which dev.
		 * execute it directly and the dev will return the result.
		 * TODO another approach is to send a flag to remote then 
		 * remote piggyback to indicate which module should be ack.  
		 */
		long eTranxID = remoteComplete(updateID);
		
		//means this is ackToFile
		if(eTranxID == IRCUPersistance.ID_NOT_FOUND){
			
			if(fileComplete(updateID) == 0){
				
				//wait fc done
				checkFCState();
				
				//notify fc
				fc.notifyRemoteAck();
			}
		}

		return eTranxID;

	}

	private void checkFCState() {
		
		if(fc != null){
			
			//ack thread should wait for fc to TIME_WAIT state
			while(fc.getState() != Thread.State.TIMED_WAITING){
				
				try {
					
					Thread.sleep(10);
					
				} catch (InterruptedException e) {
					
					e.printStackTrace();
					LOG.warn("interrupt while I am waitting for FC done and waitting my notify~");
					
					return;
				}
			}
			
		}
		
		
	}

	/**
	 * retry handler would change to file mode after #RCM_WAIT_ACK_TIMEOUT (default is 1 mins) 
	 * and retry half of #RCM_WAIT_ACK_TIMEOUT times (the updater wake up every 1 second in default).
	 * ie, it will be changed to FileMode after 1 minute and 30 seconds in default.
	 * @param type
	 */
	protected void retryHandler(int type) {

		switch (type) {

		case RCASE_INC:

			// retry used to count failures
			if (++retry > maxRetry) {

				LOG.info("switch from toRemote mode to toFile mode, retry = "
						+ retry);
				
				switchToFileMode();

			}

			break;

		case RCASE_RESET:

			if (retry < maxRetry) {
				
				LOG.warn("retry_reset case, but the retry is NOT exceeds maxRetry, retry = "
						+ retry);
				
			}
			
			this.retry = 0;

			// switch back
			switchToRemoteMode();

			break;

		default:
			LOG.error("unknown code = " + type);
			break;

		}

	}

	//******************************************************************************************//
	//																							//
	// 										cleaner 											//
	// 																							//
	//******************************************************************************************//

	public void stopPersistDev(){
		
		if(fc != null && fc.isAlive()){
			
			//stop fc by set toFileFlag to false
			switchToRemoteMode();
			
			//interrupt fc if remote device is down.
			if(toRemote.checkState() != IRCUPersistance.DEV_RUNNING){
				fc.interrupt();
			}
			
			//wait fc done
			try {
				fc.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	class FileCleaner extends Thread {

		RCUArrayList lists = null;
		Boolean remoteAck = false;

		public void run()
		{
			//check remote state
			while(true){
				
				if(toRemote.checkState() == IRCUPersistance.DEV_RUNNING){
					
					//to guarantee atomic operation in send and response. 
					acquireLock();
					
					try {
						
						//if there are files need to be handled
						if(toFile.checkState() > 0){
								
							//read the very first file to send
							lists = readFromFile();
							
							toRemote.write(lists);
							
							//clean data to indicate that this transaction should be ack to File.
							toRemote.cleanData(lists.getUpdateID());
							
							waitRemoteAck();
							
						} else {
							
							//release toFile flag
							retryHandler(RCASE_RESET);
							
						}
							
					} catch (IOException e) {
						e.printStackTrace();
					} catch (ClassNotFoundException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						e.printStackTrace();
						
					} finally {
						
						//release lock after end of transaction.
						releaseLock();
					}
					
				} else {
					
					//remote is not running, waiting...
					try {
						Thread.sleep(cleaner_interval);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				
				
			}// end of while
		}
		
		

		protected void waitRemoteAck() throws InterruptedException{
			
			synchronized(remoteAck){
				/*
				 * wait for DSS notify back.
				 * But if time out, we retransmit the this list.
				 */
				remoteAck.wait(DMSConstant.RCM_WAIT_ACK_TIMEOUT);
			}
		}
		
		
		protected void notifyRemoteAck(){
			
			synchronized(remoteAck){
				//notify to fc. it is OK even if fc has been waked up due to timeout.
				remoteAck.notify();
			}
		}
		
		
		
	}

	
	
	
	//******************************************************************************************//
	//																							//
	// 											Acker 											//
	// 																							//
	//******************************************************************************************//
	
	class Ack2RCM extends Thread{
		
		long updateID = -1;
		int ackType = -1;
		Thread parent;
		
		
		public Ack2RCM(Thread parent, long updateID, int ackType) {
			super();
			this.parent = parent;
			this.updateID = updateID;
			this.ackType = ackType;
		}


		public void run(){
			
			//wait until parent status change;
			//while(parent.getState() != Thread.State.TIMED_WAITING){
			while(parent.getState() == Thread.State.RUNNABLE /*running state*/ ){
				
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
			LOG.debug("ACKER ack updateID = " + updateID);
			
			if(iLAPI != null){
				
				iLAPI.ackRCUpdate(updateID);
				
			} else {
				
				LOG.error("the Local API impl instance is NULL!");
			}
			
		}
		
		
	}
	
	
	
	
	
	
}
