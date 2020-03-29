package org.itri.ccma.server.namenode.rcmanager;

import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.itri.ccma.editlog.interfaces.ITransactionManager2;


public class ImitateEditlog implements ITransactionManager2 {

	public final Log LOG = LogFactory.getLog(ImitateEditlog.class.getName());
	
	
	private AtomicLong tranxid = new AtomicLong(0);
	
	private AtomicLong commitedID = new AtomicLong(0);
	
	private LinkedList<Long> pendingIDs;
	
	private ReadWriteLock lock;
	
	public ImitateEditlog() {
		
		super();

		this.pendingIDs = new LinkedList<Long>();

		lock = new ReentrantReadWriteLock();
		
	}


	@Override
	public boolean commit(long tranxID) {
		
		lock.writeLock().lock();
		
		//if I am the first one, record my ID
		Long id = new Long(tranxID);
		
		int index = pendingIDs.indexOf( id );
		
		try{
			//remove from list
			pendingIDs.remove(index);
			
			if(LOG.isDebugEnabled()){
				LOG.debug("removed commitTranxID = " + tranxID + " from pendingIDs~");
			}

			id = null;
			
			//which means there is no others before me
			if( index == 0 ){
				
				try{
					
					id = pendingIDs.getFirst();
					
					//get current index 0 and set committed id  to it-1;
					commitedID.set( id.longValue() - 1 );
					
					if(LOG.isDebugEnabled()){
						LOG.debug("set commitTranxID = " + commitedID.get());
					}
					
				}catch(NoSuchElementException e){
					
					//no element in the pending, so it should be the last one.
					commitedID.set( tranxid.get() -1 );
					
				}
				
				
			} else {
				
				
				
			}
			
		}catch(IndexOutOfBoundsException e){
			
			LOG.error("Out of boundary index = " + index + " \n" + e.getMessage());
			
		}finally{
			
			lock.writeLock().unlock();
		}

		
		return true;
	}

	@Override
	public long init() {
		
		//guarantee order by lock
		lock.writeLock().lock();
		
		long id = tranxid.getAndIncrement();
		
		//add to list, 
		pendingIDs.add( new Long(id) );
		
		lock.writeLock().unlock();
		
		return id;
	}


	@Override
	public long getMaxSeqCommittedID() {
		
		return commitedID.get();
	}

	
	public void reset(){
		
		lock.writeLock().lock();
		
		tranxid.set(0);
		commitedID.set(0);
		
		pendingIDs.clear();
		
		lock.writeLock().unlock();
	}


	public LinkedList<Long> getPendingIDs(){
		
		return (LinkedList<Long>) pendingIDs.clone();
		
	}

	@Override
	public long init(long timeoutSecondValue) {
		// TODO Auto-generated method stub
		return 0;
	}


	@Override
	public boolean abort(long transID) {
		// TODO Auto-generated method stub
		return true;
	}



	@Override
	public long getMaxSeqClosedID() {
		// TODO Auto-generated method stub
		return 0;
	}


	@Override
	public long registerEvent(long runtimeID, byte moduleType, long event) {
		// TODO Auto-generated method stub
		return 0;
	}


	@Override
	public long registerEvent(long runtimeID, byte moduleType, long[] event) {
		// TODO Auto-generated method stub
		return 0;
	}


	@Override
	public boolean log(long runtimeID, byte moduleType, byte[] data) {
		// TODO Auto-generated method stub
		return false;
	}


	@Override
	public boolean notifyFlush(byte moduleType, long event, long seqID) {
		// TODO Auto-generated method stub
		return true;
	}


	@Override
	public void shutdown() {
		// TODO Auto-generated method stub
		
	}
	
	
	
	
}
