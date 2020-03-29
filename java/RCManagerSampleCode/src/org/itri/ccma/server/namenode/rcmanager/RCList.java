/**
 * 
 */
package org.itri.ccma.server.namenode.rcmanager;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.itri.ccma.server.namenode.DMSConstant;
import org.itri.ccma.server.namenode.rcmanager.interfaces.IRCManager;

import java.util.concurrent.atomic.AtomicInteger;

import org.itri.ccma.editlog.interfaces.ITransactionManager;

/**
 * @author Vis Lee
 *
 *	this list contains a number of transactions that user specified. 
 *	Every transaction records in the rc element and indexed by it's transaction ID.
 *
 *	TODO: because the requirement has been changed, actually we don't need rc elemets this data structure.
 *		  all we need is to create two individual lists which contain increase rc and decrease rc update separately.
 *		  this is better for performance point of view.
 */
public class RCList implements Serializable{

	/**
	 * default ID
	 */
	private static final long serialVersionUID = 896468952555968153L;


	enum LSTATE	{
		INIT,
		USING,
		CLEANING,
		PENDING
	}
	
//	enum LTYPE {
//		ALL_INC,
//		ALL_DEC,
//		HETEROGENEITY
//	}
	
	public final Log LOG = LogFactory.getLog(RCList.class.getName());
	
	private ArrayList<RCElement> rclist;

	
	private LSTATE listState = LSTATE.INIT;
	
	/**
	 * count increase and decrease elements of list 
	 */
	private AtomicInteger incCount;
	private AtomicInteger decCount;
	
	private long headID;
	
	private long listID;
	
	private int listSize = DMSConstant.RCM_RCLIST_INIT_SIZE;
	
	private long sTranxID;
	private long eTranxID;
	
	

	
	/**
	 * @return the listID
	 */
	public long getListID() {
		return listID;
	}

	/**
	 * @param listID the listID to set
	 */
	public void setListID(long listID) {
		this.listID = listID;
	}

	/**
	 * @param sTranxID the sTranxID to set
	 */
	public void setHeadID(long sTranxID) {
		
		this.headID = sTranxID;
	}

	/**
	 * @return the sTranxID
	 */
	public long getHeadID() {
		return headID;
	}
	
	/**
	 * @return the listSize
	 */
	public int getListSize() {
		return listSize;
	}

	/**
	 * @return the incCount
	 */
	public int getIncCount() {
		return incCount.get();
	}

	/**
	 * @return the decCount
	 */
	public int getDecCount() {
		return decCount.get();
	}

	public int getTotalHBs(){
		return (incCount.get() + decCount.get());
	}

	public RCList(int size) {
		
		super();
		
		if(size > 0){
			listSize = size;
		}
		
		/*
		 * it will automatically increase the size even if exceed the init size.
		 */
		
		rclist = new ArrayList<RCElement>( listSize );
		
		preAllocElements(listSize);
		
		incCount = new AtomicInteger(0);
		decCount = new AtomicInteger(0);
		
		this.listState = LSTATE.USING;
		
	}
	
	private synchronized void preAllocElements(int listSize){
		
//		//extend to incSize
//		rclist.ensureCapacity(rclist.size() + incSize);
		
		/*
		 * we need to prepare all element first, or it will throw an outOfBoundary exception.
		 * don't care if the specific index has no element.
		 */
		
		while( listSize-- > 0){
			rclist.add(new RCElement(0));
		}
		
	}
	
	/**
	 * @return the listState
	 */
	public LSTATE getListState() {
		return listState;
	}

	/**
	 * @param listState the listState to set
	 */
	public void setListState(LSTATE listState) {
		this.listState = listState;
	}


	public boolean setupRCList(long listID, long headID){
		
		int i = 0;
		
		this.listID = listID;
		this.headID = headID;
		
		//setup RCElements
		for(RCElement e: this.rclist){
			
			if( e != null ) {
				e.setTranxID(headID+i++);
			}
		}
		
		
		return true;
	}
	
	public boolean addToRCList(long tranxID, List<Long> hbids, int size, int updateType){
		
		RCElement e = null;
		boolean retcode = false;
		
		if(listState == LSTATE.USING){
			
			try{
				//find corresponding RCElement
				e = getRCElement(tranxID);
				
				
				if( (retcode = e.addHBIDs(hbids, updateType)) ){
					
					switch(updateType){
					
					case IRCManager.RCU_TYPE_INC_RC:
						
						incCount.addAndGet(size);
						break;
						
					case IRCManager.RCU_TYPE_DEC_RC:
						
						decCount.addAndGet(size);
						break;
					
					default:
					
						LOG.error("no such type: " + updateType);
						
					}
				}
				
				
			}catch( IndexOutOfBoundsException excp ){
				
				//TODO should I throw the exception?
				retcode = false;
			}

		}

		return retcode;
		
	}
	
	public void resetRCList(){
		
		//go thru all elements and release HBIDs.
		listState = LSTATE.CLEANING;
		
		for(RCElement e:rclist){
			
			if(e != null){
				
				e.resetRCElement();
			}
		}
		
		headID = 0;
		listID = 0;
		
		incCount.set(0);
		decCount.set(0);
		
		listState = LSTATE.PENDING;
	}
	
	public RCElement getRCElement(long tranxID) throws IndexOutOfBoundsException{
		
		RCElement e = null;
		
		try{
			//find corresponding RCElement
			e = rclist.get( getIndex(tranxID) );
			
			//check id
			assert(e.getTranxID() == tranxID);
			
		}catch( IndexOutOfBoundsException excp){
			
			LOG.error("You add an out of range elements into me, don't do that! the tranxID = " + tranxID + excp.getMessage());
			throw excp;
		}
		
		return e;
	}
	
	
	public int fill2Buf(RCUTxBuffer txBuf) throws IOException{
		
		int esize = 0;

		for(RCElement e:rclist){
			
			if(e != null){
				
				try{
					esize = e.fill2Buf(txBuf);
				}catch (ArrayIndexOutOfBoundsException exp){
					LOG.error(this.toString());
					LOG.error("AllInc: " + this.collectHBIDsInList(IRCManager.RCU_TYPE_INC_RC).toString());
					LOG.error("AllDec: " + this.collectHBIDsInList(IRCManager.RCU_TYPE_DEC_RC));
					throw exp;
				}
				
				
				//the array growth size should be the same as list record.
				assert( (esize) == e.getTotalSize());
			}
		}

		return (this.incCount.get()+this.decCount.get());
		
	}
	
	
	private synchronized RCElement syncAddRCElement(long tranxID, int updateType){
		
		RCElement e;
		e = rclist.get( getIndex(tranxID) );
		
		if(e == null){
			e = new RCElement(tranxID);
			rclist.set( getIndex(e.getTranxID()), e);
		}
		
		return e;
	}
	

	/**
	 * no one else would access this list, basically.
	 * @return 
	 */
	public long closeAllElements(ITransactionManager elog, byte eventByte){
		
		long rID = 0;
		RCElement e = null;
		Iterator<RCElement> iter = rclist.iterator();
		
		while( iter.hasNext() ){
			
			e = iter.next();
			
			if(e !=null ){
				
				if( elog.notifyFlush( eventByte, e.getTranxID()) == false ){
					LOG.error( "close tranx fail! e = " + e.toString() );
				}
			}
		}
		
		//last one element;
		if( e!=null ){
			
			rID = e.getTranxID();
		}
		
		if(LOG.isDebugEnabled()){
			
			LOG.debug("last element TranxID = " + rID + ", of listID = " + this.listID);
		}
		
		return rID;
	}
	
	
	private int getIndex(long tranxID) {

		int index = (int) (tranxID - this.headID);
		
		assert(index < listSize);

		return index;
	}

	
	
	protected ArrayList<Long> collectHBIDsInList(int type){
		
		ArrayList<Long> hbids = new ArrayList<Long>();
		
		Iterator<RCElement> iter = rclist.iterator();
		
		RCElement e = null;
		
		while(iter.hasNext()){
			
			e = iter.next();
			
			if( e!=null ){
				
				if(type == IRCManager.RCU_TYPE_INC_RC){
					
					if( e.getIncHBIDsSize()!= 0 && hbids.addAll(e.getIncHBIDs()) ){
						LOG.error("collect INC_RC return false! tranxID = " + e.getTranxID() 
								+ "\ninc array = " + e.getIncHBIDs().toString() );
					}
					
				}else if(type == IRCManager.RCU_TYPE_DEC_RC){
					
					if( e.getDecHBIDsSize()!= 0 && hbids.addAll(e.getDecHBIDs()) ){
						LOG.error("collect DEC_RC return false! tranxID = " + e.getTranxID() 
								+ "\ninc array = " + e.getIncHBIDs().toString() );
					}
				}
			}
		}
		
		return hbids;
		
	}
	
	
	
	protected int countAgainInList(int type){
		
		int count = 0;
		
		Iterator<RCElement> iter = rclist.iterator();
		
		RCElement e = null;
		
		while(iter.hasNext()){
			
			e = iter.next();
			
			if( e!=null ){
				
				if(type == IRCManager.RCU_TYPE_INC_RC){
					count += e.getIncHBIDsSize();
				}else if(type == IRCManager.RCU_TYPE_DEC_RC){
					count += e.getDecHBIDsSize();
				}
			}
		}
		
		return count;
		
	}
	
	
	protected ArrayList<Long> getHBIDsInList(){
		
		ArrayList<Long> hbids = new ArrayList<Long>();
		
		Iterator<RCElement> iter = rclist.iterator();
		
		RCElement e = null;
		
		while(iter.hasNext()){
			
			e = iter.next();
			
			if( e!=null ){
				
				if( hbids.addAll(e.getIncHBIDs()) || hbids.addAll(e.getDecHBIDs()) ){
					LOG.error("addAll return false! tranxID = " + e.getTranxID() 
							+ "\ninc array = " + e.getIncHBIDs().toString()
							+ "\ndec array = " + e.getDecHBIDs().toString());

				}
			}
		}
		
		return hbids;
		
	}
	
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "RCList [listID=" + listID + ", listState=" + listState + ", incCount=" + incCount
				+ ", decCount=" + decCount + ", sTranxID=" + headID + " ]";
	}

	protected ArrayList getIncHBIDs(long tranxID) {
		
		RCElement e = getRCElement(tranxID);
		
		if(e!=null){
			return e.getIncHBIDs();
		}
		return null;
	}
	
	protected ArrayList getDecHBIDs(long tranxID) {
		
		RCElement e = getRCElement(tranxID);
		
		if(e!=null){
			return e.getDecHBIDs();
		}
		return null;
	}
	
	
}
