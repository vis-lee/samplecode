package org.itri.ccma.server.namenode.rcmanager;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.itri.ccma.server.namenode.interfaces.IProtocolDSS4DMS;
import org.itri.ccma.server.namenode.rcmanager.exception.NoSupportMethodException;
import org.itri.ccma.server.namenode.rcmanager.interfaces.IRCUPersistance;

public class PersistToDSS implements IRCUPersistance {

	
	public final Log LOG = LogFactory.getLog(PersistToDSS.class.getName());
	
	private IProtocolDSS4DMS idss;
	
	
	//pendnig record
	private RCUArrayList dssPending;
	
	
	
	
	
	public PersistToDSS(IProtocolDSS4DMS idss, RCManager rcm) {
		super();
		this.idss = idss;
	}


	/**
	 * 
	 * @param txBuf
	 * @return total size. now we return this number but it means annul actually. 
	 * modify it if you want to use it.
	 * @throws IOException 
	 */
	private int setupTxBuf(RCUArrayList updateLists, RCUTxBuffer txBuf) throws IOException{
		
		int size = 0;
		RCElement e = null;
		
		try{
			
			Iterator<RCElement> iter = updateLists.iterator();
			 
			//go thru list and fill hbids into buf
			while( iter.hasNext() ){
				
				e = iter.next();
	
				size += e.fill2Buf(txBuf);
				
			}
			
		} catch (ArrayIndexOutOfBoundsException exp){
			
			LOG.error("!!ArrayIndexOutOfBoundsException!!");
			LOG.debug("pending list contents: " + updateLists.toString());
						
			return 0;
		}
		
		return size;
		
	}
	
	
	/**
	 * collect available RC elements for updating. the number of elements can't exceed the restrain or latest committed transaction ID.
	 * Journalizing a RCU log, then transmit increase and decrease RC elements to DSS separately. 
	 * @param restrain
	 * @return 	0	: means nothing to do.
	 * 			>0	: number of elements for updating 
	 * 			<0	: error code
	 * @throws IOException 
	 */
	synchronized protected int updateRCListToDSS(RCUArrayList updateLists) throws IOException{
		
		int retcode = 0;
		
		//tx buffer, size variation is high. because dedup will incurred many HB reference changes.
		RCUTxBuffer txBuf = new RCUTxBuffer(updateLists.getIncSize(), updateLists.getDecSize());
		
		retcode = setupTxBuf(updateLists, txBuf);
		
		long uID = updateLists.getUpdateID();
		
		if(retcode > 0){
			
			
			LOG.info("updateID = " + uID + " with num of transactions = " + updateLists.size() + " is sending to DSS...");
			
			//send out list with updateID
			retcode = idss.updateRC( uID, txBuf.getIncRCSize(), txBuf.getIncRCs().toArray(), txBuf.getDecRCSize(), txBuf.getDecRCs().toArray() );
			
			if(retcode == IProtocolDSS4DMS.DSS_RPC_OK){
				
				retcode = txBuf.getIncRCSize() + txBuf.getDecRCSize();
				
			}else{
				
				//set dss pending to null
				cleanData(uID);
			}
			
			LOG.info("sent to DSS, updateID = " + updateLists.getUpdateID() + " with txBuf.getIncRCSize() = " 
					+ txBuf.getIncRCSize() + ", txBuf.getDecRCSize() = " + txBuf.getDecRCSize());
			
		}
		
		
		return retcode;
		
	}
	


	
	
	@Override
	public int write(RCUArrayList lists) throws IOException {

		int retcode = -1;
		
		if(lists != null){
			
			prepareData(lists);
			retcode = updateRCListToDSS(lists);
			
		}
		
		return retcode;
	}

	@Override
	public RCUArrayList read() throws NoSupportMethodException {
		
		// no Query supported on DSS
		throw new NoSupportMethodException("No \"Query\" supported on DSS");
	}

	

	@Override
	public boolean prepareData(RCUArrayList lists) {
		dssPending = lists;
		return true;
	}
	
	
	@Override
	public boolean cleanData(long updateID){
		dssPending = null;
		return true;
	}

	
	/**
	 * after we persist to DSS, commit all the transactions.
	 */
	@Override
	public long complete(long updateID) throws NoSupportMethodException {

		long eTranxID = -1;
		
		if(dssPending == null || updateID != dssPending.getUpdateID()){
			return ID_NOT_FOUND;
		}
		
		LOG.info("update ID = " + updateID + " is acking from DSS...");
		
		eTranxID = dssPending.geteTranxID();
		
		cleanData(updateID);
		
		
		return eTranxID;

	}


	@Override
	public int checkState() {

		return idss.checkDssState();
	}


}
