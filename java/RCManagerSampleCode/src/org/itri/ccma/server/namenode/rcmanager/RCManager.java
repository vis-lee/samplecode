/**
 * 
 */
package org.itri.ccma.server.namenode.rcmanager;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.itri.ccma.server.namenode.DMSConstant;
import org.itri.ccma.server.namenode.DMSFlags;
import org.itri.ccma.server.namenode.interfaces.IProtocolDSS4DMS;
import org.itri.ccma.server.namenode.rcmanager.exception.NoSupportMethodException;
import org.itri.ccma.server.namenode.rcmanager.interfaces.IRCManager;
import org.itri.ccma.server.namenode.rcmanager.interfaces.IRCUPersistance;
import org.itri.ccma.utils.DssRpcCall;
import org.itri.ccma.utils.NameNode;
import org.itri.ccma.editlog.interfaces.ITransactionManager2;
import org.itri.ccma.editlog.manager2.TransactionManager;

/**
 * @author Vis Lee
 * <p>dear stranger, the mission of you is not only knowing how to send reference count
 * to remote but also understanding how to management threads here. Because there are many
 * thread's context here, for example: BM caller, updater and file cleaner, in addition,
 * the acker for unit tests etc. It's kind of complicate, but still you are the ONE, don't 
 * be scared, don't hesitate, collide your sword and go get your glory...
 * </p>
 */
public class RCManager implements IRCManager{
	
	public final Log LOG = LogFactory.getLog(RCManager.class.getName());
	
	//rclid and eTranxID
	private RCMID rcmid;
	
	//update entry pool
	private RCElementsContainer rcec;
	
	//pending lists
	protected RCUArrayList pendingList;
	
	private Updater updater;
	
	private RCPersistance persistDev;
	
	private ITransactionManager2 elog;
	
	private boolean ongoing;

	protected PersistToDSS dssDev;
	protected PersistToFile fileDev;
	
	
	protected HARCUpdate harcu;
	protected HARCElement harce;
	
	static private RCManager me = null;
	
	
	public RCManager(){
		
		IProtocolDSS4DMS idss = null;
		ITransactionManager2 elog = null;
		
		idss = DssRpcCall.getInstance();
		
		if(DMSFlags.ENABLE_EDITLOG){
			elog = TransactionManager.getInstance();
		}
		
		initRCManager(elog, idss, -1, -1);
		
	}
	
	public RCManager(ITransactionManager2 elog, IProtocolDSS4DMS idss) {
		super();
		initRCManager(elog, idss, -1, -1);
	}
	
	public RCManager(ITransactionManager2 elog, IProtocolDSS4DMS idss, long updateInterval) {
		super();
		initRCManager(elog, idss, -1, updateInterval);
	}

	
	public RCManager(ITransactionManager2 elog, IProtocolDSS4DMS idss, int numSlots, long updateInterval) {
		super();
		initRCManager(elog, idss, numSlots, updateInterval);
	}


	
	public boolean initRCManager(ITransactionManager2 elog, IProtocolDSS4DMS idss, int numSlots, long updateInterval){
		
		//init RCMID
		rcmid = new RCMID(DMSConstant.RCM_DIR);
		
		if(elog == null){
			elog = new FakeEditlog();
		}
		this.elog = elog;
		
		if(idss == null){
			idss = new FakeDSSAPIs(this);
		}

		//init map
		rcec = new RCElementsContainer(numSlots, elog);
		
		//init pending list container
		pendingList = new RCUArrayList();

		//init persistent devices
		dssDev = new PersistToDSS(idss, this);
		fileDev = new PersistToFile(DMSConstant.RCM_DIR);
		
		persistDev = new RCPersistance(NameNode.getInstance(), dssDev, fileDev);

		initUpdater(updateInterval);
		
		me = this;
		
		//register to elog
		harcu = new HARCUpdate(this);
		harce = new HARCElement(this);
		
		return true;
	}
	
	/**
	 * setup something after recovery.
	 * N.B. TM init TransactionManager_Fake when recovery stage, 
	 * so we need to get TransactionManager_Real here.
	 */
	public void initAfterRecovery(){
		
		startUpdater();
		
		//check whether the container is empty or not
		if( size() != 0 ){
			
			LOG.info("the RCElementsContainer have some elements remainded " +
					"after recover, rcec = " + rcec.toString());
			
			//flush the recovery stuff
			FlushUpdates( getLastElement().getTranxID() );
			
		}
		
		//get TransactionManager_Real instance.
		swapELog(TransactionManager.getInstance());
		
		//reset eTranxID by the elog init id, it would be better if the elog init id defined to a constant.
		setStartIndexID(ITransactionManager2.DEFAULT_INIT_ID);
		
	}
	
	
	
	protected void swapELog(ITransactionManager2 elog) {
		/*
		 *  any submodule used elog should 
		 *  place setELog() function here.
		 */
		this.elog = elog;
		rcec.setElog(elog);
	}

	/**
	 * @return the dssDev
	 */
	protected PersistToDSS getDssDev() {
		return dssDev;
	}

	/**
	 * @return the fileDev
	 */
	protected PersistToFile getFileDev() {
		return fileDev;
	}

	
	/**
	 * @return the persistDev
	 */
	protected RCPersistance getPersistDev() {
		return persistDev;
	}
	
	
	static public RCManager getRCManger(){
		
		return me;
	}
	
	

	//******************************************************************************************//
	//																							//
	// 											RCMID											//
	//																							//
	//******************************************************************************************//
	
	/**
	 * set start eTranxID if needed. ex: editlog reuses ID after recover.
	 */
//	@Override
	protected int setStartIndexID(long id) {
		rcmid.seteTranxID(id);
		return 0;
	}

	
	public void resetRCMID(){
		rcmid.reset();
	}

	
	protected void updateID(long eTranxID){
		
		//update tranx id
		rcmid.updateID(eTranxID);
	}
	
	protected long geteTranxID(){
		return rcmid.geteTranxID();
	}
	
	protected long getUpdateID(){
		return rcmid.getUpdateID();
	}
	
	
	//******************************************************************************************//
	//																							//
	// 										RC Container										//
	//																							//
	//******************************************************************************************//

	/**
	 * add one HB into update list
	 * @param updateType	1  : increase
	 * 						-1 : decrease	
	 * @return	0  - success;
	 * 			-1 - add fail;
	 * 			-2 - the transaction ID was extinct.;
	 */
	@Override
	public int addRCUpdate(long tranxID, List<Long> hbids, int size, int updateType) {
		
		//means no elog
		if( tranxID == Long.MAX_VALUE ){
			
			tranxID = elog.init();
			
		}
		
		//check id boundary
		if( tranxID <= rcmid.geteTranxID() ){
			
			LOG.error("FATAL ERROR! you wanna add prior tranxID = " + tranxID + ", but eTranxID = " + rcmid.geteTranxID());
			return -2;
		}
		
		//add to rclm
		if( rcec.addToContainer(tranxID, hbids, size, updateType) ){
			
			return 0;
			
		}else{
			
			if(size != 0){
				LOG.error("FATAL ERROR! Add fail! tranxID = " + tranxID + ", eTranxID = " + rcmid.geteTranxID());
			}else{
				LOG.debug("Add empty rcs! tranxID = " + tranxID );
			}
			
			return -1;
		}
		
	}

	
	
	protected RCElement getRCElement(long tranxID){
		
		return rcec.getRCElement(tranxID);
	}

	protected List getIncHBIDs(long tranxID){
		
		return rcec.getRCElement(tranxID).getIncHBIDs();
	}
	
	protected List getDecHBIDs(long tranxID){
		
		return rcec.getRCElement(tranxID).getDecHBIDs();
	}

	public int size(){
		return rcec.size();
	}
	
	protected boolean isEmpty(){
		return rcec.isEmpty();
	}
	
	protected void dumpALL(){
		rcec.dumpALL();
	}
	
	protected RCElement getLastElement(){
		return rcec.getLastElement();
	}
	

	/**
	 * retrieve elements that can't exceed the restrain or the ID can't over the latest committed transaction ID.
	 * @param restrain
	 * @return number of HBs of this update. -10, if fail.
	 */
	protected int collectRCElements(RCUArrayList rcList, long maxID, int restrain){
		
		int retcode = -1;
		
		//get sub list from map
		retcode = rcec.getNextUpdateGroup( rcList, maxID, restrain );
		
		if(retcode > 0){
			
			//assign updateID
			long updateID = rcmid.getUpdateID();
			
			//setup the IDs
			if(!rcList.isEmpty()){
				
				//tranxID of first element
				rcList.setsTranxID( rcList.get(0).getTranxID() );
				
				//tranxID of last element
				rcList.seteTranxID( rcList.get(rcList.size()-1).getTranxID() );
			}
			
			rcList.setUpdateID(updateID);
			
		} else {
			
			retcode = -10;
			rcList.clear();
		}
		
		
		return retcode;
	}
	

	
	void clearPendingLists(RCUArrayList pendingLists){
		
		//remove updated rc elements and notify to elog
		rcec.removeFromContainer(pendingLists);
		
		
		//release all reference for GC
		pendingLists.clear();
	}
	

	private long logUpdateToELog(RCUArrayList rcList){
		
		long runtimeID = -1;
		
		try {
			
			byte[] logInfo = null;
			
			logInfo = rcList.getLogInfo();
			
			// init this update to editlog
			runtimeID = elog.init();
			
			// log data
			elog.log(runtimeID, DMSConstant.MODULE_ID_RCUpdate, logInfo);
			
			/*
			 * commit my close to edit log which means I have to done this definitely.
			 * Is it possible that this transaction been closed before DSSAck?
			 * The answer is NO. Because the RCElements haven't been closed.
			 */
			elog.commit(runtimeID);
			
			rcList.setRuntimeID(runtimeID);
			
			
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		
		return runtimeID;
		
	}
	
	/**
	 * Collect available RC elements for updating. 
	 * Journalizing a RCU log for new transaction. 
	 * @param rcList - 	the container for collect lists.
	 * @param restrain -	the among of elements can't exceed the restrain.
	 * @return runtimeID - the edit log's runtime ID
	 */
	protected synchronized long setupUpdate(RCUArrayList rcList, long maxID, int restrain) {
		
		long runtimeID = -1;
		int retcode = -1;
		
		//no one is ongoing, we can go.
		if(!ongoing){ 
			
			//rcList is clean
			if( rcList.getRuntimeID() < ITransactionManager2.DEFAULT_INIT_ID ){
				
				//collect data from rcec
				if( (retcode = collectRCElements(rcList, maxID, restrain)) < 0 ){
					
					return retcode;
				}
				
			}else{
				
				LOG.info("Attention: ongoing flag is false but pendingList " +
						"have data. It's OK if this is retransmission" +
						", updateID() = " + rcList.getUpdateID() + 
						", NumOfRCElements() = " + rcList.size() + 
						", runtimeID = " + rcList.getRuntimeID() + 
						" which will be changed latter.");
				
			}
			
			runtimeID = logUpdateToELog(rcList);
			setOngoingFlag(true);
			
		}else{
			
			LOG.info("Previouse request is ongoing!");
		}
		
		
		return runtimeID;
		
	}
	
	protected int doUpdate(RCUArrayList pendingList) /*throws IOException, ClassNotFoundException, InterruptedException*/ {
		
		int retcode = -1;
		
		try{
			
			retcode = persistDev.persist(pendingList);
			
			switch(retcode){
			
				case IRCUPersistance.DEV_WRITE_DONE:
					
					// do nothing for now
					break;
					
				case IRCUPersistance.DEV_WRITE_FAIL:
					
					// do nothing for now
					break;
					
				case IRCUPersistance.DEV_WRITE_EXIST:
					
					// update my update ID record
					updateID( geteTranxID() );
					pendingList.setUpdateID( getUpdateID() );
					break;
					
				default:
					break;
			
			}
			
		} catch (IOException e) {
			
			retcode = -602;
			LOG.error(e.getMessage());
			//throw e;
			
		}
		
		return retcode;
		
	}

	
	/**
	 * abort an update if timeout
	 * @param updateID
	 * @param runtimeID
	 * @return
	 */
	synchronized protected boolean abortUpdate(long updateID, long runtimeID) {
		
		boolean retcode = false;
		
		if(ongoing){
			
			//don't clear pendingList, we should update the same list again
			//pendingList.clear();
			
			retcode = elog.abort(runtimeID);
			
			if(retcode == true){
				LOG.warn("Abort the transaction success!! updateID = " + updateID + ", runtimeID = " + runtimeID);
			} else {
				LOG.error("Abort the transaction failed!! updateID = " + updateID + ", runtimeID = " + runtimeID);
			}
			
			setOngoingFlag(false);
		}
		
		
		return retcode;
		
	}

	@Override
	synchronized public int ackRCUpdate(long updateID) {
		
		int retcode = -1;
		long eTranxID = -1;
		
		try {
			
			eTranxID = persistDev.complete(updateID);
			
		} catch (NoSupportMethodException e) {
			e.printStackTrace();
		}
		
		//ack from dssDevice, do post actions
		if(eTranxID >= 0){
			
			if(pendingList != null && updateID == pendingList.getUpdateID()){
				
				//update id
				updateID(eTranxID);
				
				//clear rc lists
				clearPendingLists(pendingList);
				
				//signal to RCUpdater
				sigDSSAck();
			}
			
		}
		
		return retcode;
		
	}




	/**
	 * flush out all the remain works.
	 */
	@Override
	public boolean flushRCManager() {
		
		/*
		 * flush out lists by Long.MaxID() to flush all lists. 
		 * Of course, wait for edit log end all transaction commit.
		 */
		FlushUpdates(-1);
		
		return true;
	}


	@Override
	public int shutdownRCManager() {

		//flushing all rcs
		flushRCManager();
		
		//end the updater
		stopUpdater();
		
		//end persist devices
		persistDev.stopPersistDev();
		
		//check correctness
		if( rcec.size() != 0){
			
			LOG.error("something still in the container... maybe you put some elements" +
					" after you call flush. check it... rce container = " + rcec.toString());
			
		}
		
		return 0;
	}
	
	
	
	protected RCUArrayList getPendingList(){
		return this.pendingList;
		
	}
	
	
	/**
	 * set onging to true to indicate one request is on going and suspend
	 */
	protected void setOngoingFlag(boolean b){
		
		ongoing = b;
		
	}
	
	//******************************************************************************************//
	//																							//
	// 										RCUpdater											//
	//																							//
	//******************************************************************************************//
	

	protected Updater initUpdater(long updateInterval){
		
		if(updater == null){
			
			synchronized(this){
				
				if(updater == null){
					
					updater = new Updater(updateInterval);
					
				}
			}
		}
		
		return updater;
		
	}
	
	protected void startUpdater(){
		
		updater.start();
	}

	protected void stopUpdater() {
		
		updater.setAlive(false);
		
		try {
			//block caller and wait here...
			updater.join(DMSConstant.RCM_UPDATE_PERIODMS * 3 );
			
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		updater = null;
		
	}

	protected void suspendUpdater(){
		updater.pauseUpdate();
	}
	
	protected void resumeUpdater(){
		updater.continueUpdate();
	}
	
	protected void sigDSSAck(){
		
		if(ongoing){
			
			setOngoingFlag(false);
			
			if(updater != null && updater.getState() == Thread.State.TIMED_WAITING){
				
				//notify updater
				updater.notifyDssAck();
				
			}
		}
	}
	
	
	protected void FlushUpdates(long checkPointID){
		
		if( checkPointID < 0 ){
			
			/*
			 * get the tranxID of last element.
			 * if null means nothing left.
			 */
			RCElement e = getLastElement();
			
			if(e != null){
				
				checkPointID = e.getTranxID();
				
			}else{
				/*
				 * the container is empty.
				 */
				return;
			}
			
		}
		
//		if( checkPointID > elog.getMaxSeqCommittedID() ){
//
//			LOG.warn("Invalid argument, current max committed ID = " + elog.getMaxSeqCommittedID() 
//					+ ", your check point ID = " + checkPointID);
//			return;
//		}
		
		updater.doFlush(checkPointID);
		
	}
	

	
	/**
	 * updater checks the RCs every 1 second. if conditions sufficed, update RCs and wait for response 
	 * from remote within RCM_WAIT_ACK_TIMEOUT. If DSS doesn't response within RCM_WAIT_ACK_TIMEOUT, 
	 * updater will wake up and try to recover the pending list back to rcec and cancel this transaction.
	 * {@link #alive} is used to tell updater to stop after done all RCs updating ASAP.
	 * @param ut - update interval, default is 1 second.
	 * 
	 */
	class Updater extends Thread{
		
		// signal me after dss ack back
		protected Boolean ackSignal;
		
		// signal the flush callers
		protected Boolean flushFlag = false;
		
		//flag for notice thread to end
		protected boolean alive;
		
		//flag for notice thread to running/suspend
		protected AtomicBoolean running = new AtomicBoolean(true);
		
		private long updateInterval = DMSConstant.RCM_UPDATE_PERIODMS;
		
		private long lastCommittedID = 0;
		
		private long flushCheckPointID = DEFAULT_CPID;
		
		private int dssConsumeRate = DMSConstant.RCM_DSS_MAX_HBS;
		
		private int ackTimeout = DMSConstant.RCM_WAIT_ACK_TIMEOUT;
		
		
		private static final int DEFAULT_CPID = -2;
		
		public void setAlive(boolean alive) {
			this.alive = alive;
		}

		public Updater(long ut) {
			
			super("RCUpdater");
			this.alive = true;
			this.ackSignal = new Boolean(false);
			
			if(ut > 0){
				this.updateInterval = ut*1000;
			}
			
			this.flushCheckPointID = DEFAULT_CPID;
			
		}

		
		public void run()
		{
			
			//whlie alive flag set to true.
			while( alive ){
				
				if(running.get()){
					
					
					//no proceeding job
					if( !ongoing ){
						
						updateJob();
						
					} else {
						
						/*
						 * means the previous update has not ack back within ackTimeout period. 
						 * recover the pending lists.
						 */
						LOG.error("Because the previous request has NOT ack back. lists = "
								+ pendingList.toString());
						
						//cancel the editlog.
						abortUpdate(pendingList.getUpdateID(), pendingList.getRuntimeID());
					}
					
					//if someone call flush, check and notify it.
					notifyFlushDone();
					
					
				} else {
					
					sleepHandler();
					
				}
				
			}

		}

		
		
		private void updateJob() {

			long runtimeID = -1;

			//acquire lock
			persistDev.acquireLock();
			
			//get latest committed tranxID
			lastCommittedID = elog.getMaxSeqCommittedID();
			
			if( (runtimeID = setupUpdate(pendingList, lastCommittedID, dssConsumeRate)) >= 0){
				
				int retcode = -1;
				
				try{
					
					retcode = doUpdate(pendingList);
					
				} finally {
					
					if( retcode >= 0){
						
						//wait for response
						waitDssAck();
						
						//release lock after DSS ack back.
						persistDev.releaseLock();
						
					} else if( retcode < 0 ) {
						
						LOG.error("put NextUpdateList back to rcec because PersistDev retcode = "
								+ retcode + ", these lists: " + pendingList.toString());
						
						//cancel the editlog.
						abortUpdate(pendingList.getUpdateID(), runtimeID);
						
						//release lock first
						persistDev.releaseLock();
						
						//sleep
						sleepHandler();
					}

				}
				
			} else {
				
				/*
				 * no lists need to be updated.
				 */
				//release lock
				persistDev.releaseLock();
				
				sleepHandler();
				
			}
		
			
		}

		/**
		 * @param checkPointID, set check point
		 */
		protected void doFlush(long checkPointID) {
			
			if(flushFlag != true){
				flushFlag = true;
			}
			
			/*
			 * set current flush check point ID.
			 * multiple callers are allowed. if you curious
			 * why synchronized workable in this situation 
			 * you can visit here:
			 * http://tutorials.jenkov.com/java-concurrency/thread-signaling.html
			 */
			if(checkPointID > flushCheckPointID){
				flushCheckPointID = checkPointID;
			}
			
			
			synchronized(flushFlag){
				
				try {
					//wait for meet condition
					this.flushFlag.wait(ackTimeout*3);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
			LOG.info("RC Updater flush done!");
		}

		
		/**
		 * set update range
		 */
		private void notifyFlushDone() {
			
			/*
			 * why don't we set lastCommittedID to Long.MaxValue directly?
			 * Consider the edit log time out, we can't perform a big update at once.
			 * So we go for original design which do update in a reasonable dssConsumeRate
			 * to avoid long time update.
			 */
			if( flushCheckPointID > DEFAULT_CPID ){
				
				//if the eTranxID meets or over the check point, means flush done.
				if(flushCheckPointID <= rcmid.geteTranxID()){
					
					LOG.info("notify flush done!");
					
					synchronized(flushFlag){
						//notify callers that I have done this command.
						this.flushFlag.notifyAll();
					}
					
					this.flushFlag = false;
					
					this.flushCheckPointID = -1;
				}
			}
			
		}

		
		public void pauseUpdate(){
			running.set(false);
		}
		
		public void continueUpdate(){
			running.set(true);
		}
		
		
		@SuppressWarnings("static-access")
		protected void sleepHandler() {
			
			try {
				
				//if previous hasn't been acknowledged back.
				this.sleep(updateInterval);
				
			} catch (InterruptedException e) {
				
				e.printStackTrace();
			}
			
		}
		
		
		
		protected void waitDssAck() /*throws InterruptedException*/{
			
			synchronized(ackSignal){
				
				try {
					/*
					 * wait for DSS notify back until 
					 * RCM_WAIT_ACK_PERIODMS seconds.
					 */
					ackSignal.wait(ackTimeout);
					
				} catch (InterruptedException e) {
					
					e.printStackTrace();
				}
			}
			
		}
		
		
		
		protected void notifyDssAck(){
			
			synchronized(ackSignal){
				/*
				 * notify to updater. it is OK even updater 
				 * has been waked up due to timeout.
				 */
				ackSignal.notifyAll();
			}
		}
		
	} //end RCUpdater class

	
	
	
	
	
}
