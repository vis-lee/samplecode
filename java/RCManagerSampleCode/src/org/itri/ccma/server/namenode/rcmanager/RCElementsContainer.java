/**
 * 
 */
package org.itri.ccma.server.namenode.rcmanager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.itri.ccma.editlog.interfaces.ITransactionManager2;
import org.itri.ccma.editlog.manager2.TransactionManager;
import org.itri.ccma.server.namenode.DMSConstant;


/**
 * @author Vis Lee
 *
 * 1. Management 
 */
public class RCElementsContainer {
	
	public final Log LOG = LogFactory.getLog(RCElementsContainer.class.getName());
	
	private ITransactionManager2 elog;

	/*
	 *  expected average log(n) time cost for the containsKey, get, put and remove operations and their variants.
	 */
	private ConcurrentSkipListMap<Long, RCElement> rcMap;

	public RCElementsContainer() {
		this(0, null);
	}
	
	public RCElementsContainer(int size, ITransactionManager2 elog) {
		super();
		rcMap = new ConcurrentSkipListMap<Long, RCElement>();
		
		if(elog == null){
			elog = TransactionManager.getInstance();
		}
		
		setElog(elog);
		
	}
	

	/**
	 * @return the elog
	 */
	protected ITransactionManager2 getElog() {
		return elog;
	}

	/**
	 * @param elog the elog to set
	 */
	protected void setElog(ITransactionManager2 elog) {
		this.elog = elog;
	}

	/**
	 * add rc into corresponding element.
	 * @param tranxID
	 * @param hbids
	 * @param size
	 * @param updateType
	 * @return	true - if add success
	 * 			false - otherwise.
	 */
	public boolean addToContainer(long tranxID, List<Long> hbids, int size, int updateType){
		
		RCElement e = null, old = null;
		boolean retcode = false;
		Long tid = new Long(tranxID);
		
		//find corresponding RCElement
		if( (e = rcMap.get(tid)) == null ){
			
			//TODO build a memory pool for RCElement
			e = new RCElement(tranxID);

			//use putIfAbsent to synchronize
			if( (old = rcMap.putIfAbsent(tid, e)) == null){

				//setup the sequence id for new one
				e.setSeqID( elog.registerEvent(tranxID, DMSConstant.MODULE_ID_RCElement, tranxID) );
				
			} else {

				e = old;
			}
			
		}
		
		retcode = e.addHBIDs(hbids, updateType);
		
		return retcode;
	}
	
	
	/**
	 * remove the updated elements from the map
	 * @param list
	 */
	synchronized public void removeFromContainer(ArrayList<RCElement> list){
		
		if( list != null ){
			
			for(RCElement e : list){
				
				if( e != null ){
					
					//notify editlog
					if( elog.notifyFlush( DMSConstant.MODULE_ID_RCElement, e.getTranxID(), e.getSeqID()) == false ){
						
						LOG.error( "close tranx fail! e = " + e.toString() );
						
					} else {
						
						if(LOG.isDebugEnabled()){
							LOG.debug("close tranxID = " + e.getTranxID());
						}
					}
					
					if( rcMap.remove(e.getTranxID()) == null ){
						
						LOG.warn("The tranxID = " + e.getTranxID() + " is not exist in the Map");
					}
				}
				
			}
			
			//clean all to release the references.
			list.clear();
			
		}
		
	}
	
	
	
	/**
	 * because optimization, we restrain the iterate range within DMSConstant.RCM_DEFAULT_RCML_SIZE
	 * @return - number of HBs for updating
	 */
	public synchronized int getNextUpdateGroup(RCUArrayList list, long lastCommitID, int restrain){
		
		int numOfHBs = 0;
		long maxID = lastCommitID+1;

		try{
			//get the submap you interest on.
			ConcurrentNavigableMap<Long, RCElement> submap = rcMap.headMap(maxID);
			
			NavigableSet<Long> keys = submap.keySet();
			
			for(Long key : keys){
				
				RCElement e = submap.get(key);
				
				numOfHBs += e.getTotalSize();
				
				if(numOfHBs < restrain){
					
					if( !list.add(e) ){
						LOG.error("Add Fail! the rc element = " + e.toString());
					}
					
				} else {
					
					numOfHBs -= e.getTotalSize();
					break;
					
				}
				
			}
			
		} catch (IllegalArgumentException e){
			
			LOG.error("illlegal argument for headMap, key = " + maxID);
			
		}
		
		
		
		if(LOG.isDebugEnabled()){
			
			LOG.debug( "total lists = " + list.size() + ", numOfHBs = " + numOfHBs );
		}
		
		return numOfHBs;
		
	}
	
	
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

	
	public RCElement getRCElement(long id){
		return rcMap.get(id);
	}
	
	public RCElement getLastElement(){
		
		Entry<Long, RCElement> entry = rcMap.lastEntry();
		
		if(entry != null){
			return entry.getValue();
		}
		
		return null;
	}
	
	public int size(){
		
		int size = 0;
		
		size = rcMap.size();
		
		return size;
	}
	
	
	
	public boolean isEmpty(){
		
		return rcMap.isEmpty();
	}

	
	
	
	public void dumpALL(){
		
		LOG.debug( rcMap.toString() );
	}

	
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		
		String s = super.toString();
		
		s += "rcMap size = " + rcMap.size() + ", entries = ";
		
		NavigableSet<Long> keys = rcMap.keySet();
		
		for(Long key : keys){
			
			RCElement e = rcMap.get(key);
			
			s += e.toString() + "\n"; 
			
		}
		
		return s;
	}
	
	
	public void clear(){
		
		rcMap.clear();
		
	}
	
}
