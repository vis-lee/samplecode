/**
 * 
 */
package org.itri.ccma.server.namenode.rcmanager;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.itri.ccma.server.namenode.DMSConstant;


/**
 * @author Vis Lee
 *
 * 1. Management list allocation and release
 * 2. find out corresponding list to add hb
 * 3. retrieve the lists to send to dss
 *
 *          assume:
 *              list element size = 100
 *              number of slots = 10
 *          
 *          
 *          latestCommitID: used to record last commitID of each slot for identifying the margion of useful lists.
 *          ------------------------------------------------------------------------------------------------------------
 *          |  ID 99   |  ID 156  |  ID 299  |  ID 399  |  ID 499  |  ID 586  |  ID 699  |  ID 799  |  ... |  ID n     |
 *          ------------------------------------------------------------------------------------------------------------
 *          
 *          slots contain lists that separate every 100 elements (transactions) into it's corresponding list.
 *          ------------------------------------------------------------------------------------------------------------
 *          |  slot 0  |  slot 1  |  slot 2  |  slot 3  |  slot 4  |  slot 5  |  slot 6  |  slot 7  |  ... |  ID n(9)  |
 *          ------------------------------------------------------------------------------------------------------------
 *             |            |                               |           |           |           |                  |    
 *             |            |                               |           |           |           |                  |    
 *          |------|     |------|                        |------|    |------|    |------|    |------|           |------|
 *          |e1000 |     |e1100 |                        |e1400 |    |e1500 |    |e1600 |    |e1700 |           |e1900 |
 *          |e1001 |     |e1101 |                        |e1402 |    |e1502 |    |e1601 |    |e1701 |           |e1901 |
 *          |  .   |     |  .   |                        |  .   |    |  .   |    |  .   |    |  .   |           |  .   |
 *          |  .   |     |  .   |                        |  .   |    |  .   |    |  .   |    |  .   |           |  .   |<---adding...
 *          |  .   |     |  .   |                        |  .   |    |  .   |    |  .   |    |  .   |           |  .   |
 *          |  .   |     |  .   |                        |  .   |    |  .   |    |  .   |    |  .   |           |  .   |
 *          |  .   |     |  .   |                        |  .   |    |  .   |    |  .   |    |  .   |           |  .   |<---adding...
 *          |  .   |     |  .   |                        |  .   |    |  .   |    |  .   |    |  .   |           |  .   |
 *          |e1050 |     |e1199 |                        |e1499 |    |e1599 |    |e1697 |    |e1750 |           |e1988 |<---adding...
 *          --------     --------                        --------    --------    --------    --------           --------
 *             |            |                                             
 *             |            |    
 *          |------|     |------|
 *  adding->|e2000 |     |e2100 |<---adding...
 *          |e2001 |     |e2101 |
 *  adding->|  .   |     |  .   |<---adding...
 *          |  .   |     |  .   |
 *  adding->|  .   |     |e2150 |
 *          |  .   |     |      |<---adding...
 *          |  .   |     |      |
 *          |  .   |     |      |
 *  adding->|e2099 |     |      |<---adding...
 *          --------     --------
 */
public class RCListsContainer {
	
	public final Log LOG = LogFactory.getLog(RCListsContainer.class.getName());
	
//	private RCListPool pool = new RCListPool();
//	
//	
//	private int slotSize = DMSConstant.RCM_DEFAULT_RCML_SLOT_SIZE;
//	
//	private LinkedList<RCList>[] rcLists;
//	private long[] latestCommitID;
//	
//	
//	//****kind of sliding window****//
//	/*
//	 * headID is atomic op, it
//	 */
//	private AtomicLong headID = new AtomicLong(0);
//	private AtomicLong tailID = new AtomicLong(0);
//
//
//	public RCListsContainer() {
//		super();
//		initManager(0);
//	}
//	
//	public RCListsContainer(int size) {
//		super();
//		initManager(size);
//	}
//	
//	private void initManager(int size) {
//		
//		if(size > 0){
//			slotSize = size;
//		}
//		
//		rcLists = new LinkedList[slotSize];
//		latestCommitID = new long[slotSize];
//		
//		for(int i = 0; i < slotSize; i++){
//			
//			rcLists[i] = new LinkedList<RCList>();
//			latestCommitID[i] = -1;
//			
//		}
//		
//	}
//	
//
//	/**
//	 * this method only can add the element after @headID.
//	 * checked in upper layer. don't check here due to performance. 
//	 * @param tranxID
//	 * @param updateType
//	 * @param hbi
//	 * @return	true - if add success
//	 * 			false - otherwise.
//	 */
//	public boolean addToList(long tranxID, List<Long> hbids, int size, int updateType){
//		
//		RCList rclist = null;
//		
//		rclist = getRCList(tranxID);
//		
//		if(rclist == null){
//			
//			long listid = getRCListID(tranxID);
//			int slotid = getSlotIndex(listid);
//			
//			//means this list haven't been there, create the list.
//			rclist = createRCList( slotid, listid );
//		}
//		
//		//add this tranx into the list
//		return rclist.addToRCList(tranxID, hbids, size, updateType);
//		
//	}
//	
//	protected RCList getRCList(long tranxID) {
//		
//		RCList rclist = null;
//		
//		long listid = getRCListID(tranxID);
//		int slotid = getSlotIndex(listid);
//		
//		rclist = iterateRCListFromLast(slotid, listid);
//		
//		return rclist;
//	}
//
//	/**
//	 * always create more than how many you need.
//	 */
//	synchronized private RCList createRCList(int slotID, long listID) {
//
//		RCList list = null;
//		
//		long lastID = latestCommitID[slotID];
//		
//		try{
//			//if someone else created the list.
//			list = iterateRCListFromLast(slotID, listID);
//			
//			if(list != null){
//				return list;
//			}
//			
//			list = rcLists[slotID].getLast();
//			
//			lastID = list.getListID();
//
//			
//		}catch(NoSuchElementException e){
//			//swallow
//		}
//		
//		if( getPriorListID(slotID, listID) > lastID){
//			
//			//recursive to create prior list, if this list id jump toooo far~
//			createRCList(slotID, getPriorListID(slotID, listID) );
//		}
//		
//		
//		list = pool.getRCList();
//		
//		list.setupRCList(listID, rcListIDToSTranxID(listID));
//		
//		rcLists[slotID].add(list);
//		
//		//set id
//		tailID.set(listID);
//		
//		return list;
//		
//	}
//	
//	private RCList iterateRCListFromLast(int slotID, long listID){
//
//		if( rcLists[slotID].isEmpty() == false ){
//			
//			RCList rclist = null;
//			
//			/*
//			 *  matched in first comparison should been the most cases.
//			 */
//			long curID = 0;
//			
//			//TODO add a read write lock of rcLists[slotid] here
//			//TODO ? can we iter without lock, because if remove a list only if this list abandoned which means I won't touch it anymore.
//			//iterate from last
//			try{
//				
//				Iterator<RCList> iter = rcLists[slotID].descendingIterator();
//				while( iter.hasNext() ){
//					
//					rclist = iter.next();
//					
//					//sometimes it can be null due to DssRpcAck back and remove the list of the index 0
//					if(rclist != null){
//						
//						curID = rclist.getListID();
//						
//						//this should be match in one loop in most cases.
//						if(curID == listID){
//							return rclist;
//						}
//						
//						//new list, break and create.
//						if(curID < listID){
//							break;
//						}
//					}
//				}
//				
//			}catch(ConcurrentModificationException e){
//				
//				LOG.info("catch ConcurrentModificationException! thread = " + Thread.currentThread().getName());
//				
//				//e.printStackTrace();
//				
//				//that's ok just iterate again
//				return iterateRCListFromLast(slotID, listID);
//				
//			}catch (NoSuchElementException e){
//				
//			}
//			
//			
//		}
//
//		return null;
//
//	}
//
//
//	private long getPriorListID(int slotID, long listID){
//		
//		long priorListID = (listID-slotSize);
//		
//		//pick up the bigger one.
//		return (latestCommitID[slotID] > priorListID) ? latestCommitID[slotID] : priorListID;
//	}
//	
//	private long getNextListID(long listID){
//		return (listID+slotSize);
//	}
//	
//	private long rcListIDToSTranxID(long listID) {
//		return ( listID * DMSConstant.RCM_RCLIST_INIT_SIZE );
//	}
//
//	private int getSlotIndex(long listid){
//		return (int)(listid % slotSize);
//	}
//	
//	private long getRCListID(long tranxID){
//		return (tranxID / DMSConstant.RCM_RCLIST_INIT_SIZE);
//	}
//
//	public void putUpdateLists(ArrayList<RCList> lists){
//		
//		if( lists != null && !lists.isEmpty() ){
//			
//			RCList rclist = lists.get(0);
//			
//			if(rclist.getListID() < headID.get()){
//				
//				//set head id
//				headID.set(rclist.getListID());
//				
//			}
//			
//		}
//		
//	}
//	
//	/**/
//	synchronized public void releaseRCLists(ArrayList<RCList> lists){
//		
//		if( lists != null ){
//			
//			for(RCList list:lists){
//
//				long listid = list.getListID();
//				int slotid = getSlotIndex(listid);
//				int index = 0;
//				
//				if( rcLists[slotid].isEmpty() == false){
//					
//					/*the first element isn't what I want. execute normal path.*/
//					if( rcLists[slotid].getFirst().getListID() != listid ){
//						
//						index = rcLists[slotid].indexOf(list);
//						
//						LOG.error("FATAL ERROR~! release the list that's not first one, the index = " + index);
//						
//					}else{
//						
//						//index = 0;
//					}
//
//				}else{
//					
//					index = -1;
//				}
//				
//				if(index >= 0){
//
//					if(LOG.isDebugEnabled()){
//						LOG.debug("release List, ListID = " + rcLists[slotid].get(index).getListID() 
//								+ ", in index = " + index + " of slotID = " + slotid);
//						
//					}
//					
//					//update latest commit id first, TODO: NOTE: if (index > 0), the commit ID will be out of sync. 
//					latestCommitID[slotid] = rcLists[slotid].get(index).getListID();
//					
//					//remove from rcLists
//					rcLists[slotid].remove(index);
//					
//					//put back to pool
//					pool.putRCList(list);
//					
//				}else{
//					
//					LOG.error("You want to release a list with listid = " + listid 
//							+ ", in slotid = "+ slotid + ", which I don't have.");
//					
//					//print the list content
//					if(LOG.isDebugEnabled()){
//						LOG.debug( list.toString() );
//					}
//				}
//			}
//			
//			//clean all to release the references.
//			lists.clear();
//			
//		}
//		
//	}
//	
//	
//	
//	/**
//	 * because optimization, we restrain the iterate range within DMSConstant.RCM_DEFAULT_RCML_SIZE
//	 * @return - number of HBs for updating
//	 */
//	public synchronized int getNextUpdateLists(RCUArrayList lists, long lastCommitID, int restrain){
//		
//		RCList rclist = null;
//		
//		long curID = headID.get();
//
//		//first undone list id
//		long maxid = getRCListID(lastCommitID+1);
//		
//		int slotID = 0;
//		int numHBs = 0;
//		
//		int nIndex = 0;
//		
//		while( curID <  maxid ){
//			
//			slotID = getSlotIndex(curID);
//			
//			try{
//				
//				rclist = rcLists[slotID].get( (nIndex)/slotSize );
//				
//				//get correct list by headID
//				if(rclist.getListID() != curID){
//					
//					if(LOG.isDebugEnabled()){
//						LOG.debug("this list ID isn't match what I want, go forward. rclist ID = " + rclist.getListID() 
//								+ ", I want ID = " + (curID-1) + ", slotID = " + slotID + ", index = " + ((nIndex-1)/slotSize) + ", maxID = " + maxid );
//					}
//					
//					continue;
//					
//				} 
//				
//				//add if haven't achieve the criterion.
//				if( (numHBs += rclist.getTotalHBs()) < restrain || lists.size() == 0 /* if this list size over the restrain we expect */ ){
//					
//					lists.add( rclist );
//					
//					//set head id, headID only moved here
//					headID.set(rclist.getListID()+1);
//					
//					//setup inc and dec sizes
//					lists.addIncSize(rclist.getIncCount());
//					lists.addDecSize(rclist.getDecCount());
//					
//				}else{
//					
//					numHBs -= rclist.getTotalHBs();
//					break;
//				}
//				
//			}catch(NoSuchElementException e){
//				
//				//swallow
//				continue;
//				
//			}catch(IndexOutOfBoundsException e){
//				
//				LOG.warn("this exception occurred because there is no lists in this index, so go forward" + e.getMessage());
//				
//			}finally{
//				
//				curID++;
//				nIndex++;
//			}
//		}
//		
//		
//		if(LOG.isDebugEnabled()){
//			
//			LOG.debug("total lists = " + lists.size() + ", HBs = " + numHBs);
//		}
//		
//		return numHBs;
//		
//	}
//	
//	
//	/**
//	 * put back what lists you have and keep them in RCListManager
//	 * @param lists
//	 */
//	synchronized public void putNextUpdateLists(ArrayList<RCList> lists){
//		
//		if(!lists.isEmpty()){
//			
//			RCList list = lists.get(0);
//			
//			if(list != null){
//				
//				//set headID back
//				headID.set(list.getListID());
//							
//			}
//		}
//		
//	}
//
//	
//	public int size(){
//		
//		int size = 0;
//		RCList list = null;
//		
//		for(int i = 0; i < slotSize; i++){
//			
//			Iterator<RCList> iter = rcLists[i].iterator();
//			
//			while(iter.hasNext()){
//				
//				list = iter.next();
//				
//				size += list.getTotalHBs();
//			}
//			
//		}
//		
//		return size;
//	}
//	
//	public boolean isEmpty(){
//		
//		for(int i = 0; i < slotSize; i++){
//			
//			if(rcLists[i].isEmpty() == false){
//				
//				return false;
//
//			}
//		}
//		
//		return true;
//	}
//
//	
//	public void dumpALL(){
//		
//		RCList list = null;
//		
//		String str = new String();
//		
//		ArrayList<Long> allhbids = new ArrayList<Long>();
//
//		for(int i = 0; i < slotSize; i++){
//			
//			str.concat("slotID = " + i + " : \n");
//			
//			Iterator<RCList> iter = rcLists[i].iterator();
//			
//			while(iter.hasNext()){
//				
//				list = iter.next();
//				
//				str.concat("\t" + list.toString() + " \n");
//				
//				allhbids.addAll(list.getHBIDsInList());
//				
//			}
//			
//			str.concat("\tHBIDs = " + allhbids.toString() + " \n");
//			
//			LOG.debug(str);
//		}
//	}
	
	
	
	
}
