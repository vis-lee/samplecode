package org.itri.ccma.server.namenode.rcmanager;

import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.itri.ccma.editlog.interfaces.ITransactionManager;
import org.itri.ccma.editlog.interfaces.ITransactionManager2;


public class FakeEditlog implements ITransactionManager2 {

	public final Log LOG = LogFactory.getLog(FakeEditlog.class.getName());
	
//	private RCManager rcm;
	
	private AtomicLong tranxid = new AtomicLong(0);
	
	private AtomicLong commitedID = new AtomicLong(0);
	
	private LinkedList<Long> pendingIDs;
	
	private ReadWriteLock lock;
	
	public FakeEditlog() {
		
		super();

		this.pendingIDs = new LinkedList<Long>();

		lock = new ReentrantReadWriteLock();
		
	}


	@Override
	public boolean commit(long tranxID) {
		
		lock.writeLock().lock();
		
		if(tranxID > commitedID.get()){
			
			commitedID.set(tranxID);
		}

		lock.writeLock().unlock();
		
		return true;
	}

	@Override
	public long init() {
		
		long id = tranxid.getAndIncrement();
		
		//commit directly
		commit(id);
		
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
