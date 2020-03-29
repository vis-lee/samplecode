package org.itri.ccma.server.namenode.rcmanager;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class RCUArrayList extends ArrayList<RCElement> implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3196886884184155923L;

	public final Log LOG = LogFactory.getLog(this.getClass().getName());
	
	
	private long updateID = -1;
	
	private AtomicInteger incSize = new AtomicInteger(0);
	private AtomicInteger decSize = new AtomicInteger(0);
	
	private long sTranxID = 0;
	private long eTranxID = 0;
	
	private long runtimeID = -1;


	/**
	 * @return the tranxID
	 */
	public long getUpdateID() {
		return this.updateID;
	}


	/**
	 * @param updateID the tranxID to set
	 */
	public void setUpdateID(long updateID) {
		this.updateID = updateID;
	}


	/**
	 * @return the numOfRCElements
	 */
	public int getIncSize() {
		return incSize.get();
	}
	
	public void addIncSize(int size){
		incSize.addAndGet(size);
	}
	
	public void subtractIncSize(int size){
		int expect = incSize.get();
		while( incSize.compareAndSet(expect, (expect-size)) != true );
	}
	

	/**
	 * @return the decSize
	 */
	public int getDecSize() {
		return decSize.get();
	}


	/**
	 * @param size the decSize to set
	 */
	public void addDecSize(int size) {
		this.decSize.addAndGet(size);
	}

	public void subtractDecSize(int size){
		int expect = decSize.get();
		while( decSize.compareAndSet(expect, (expect-size)) != true );
	}
	
	

	public long getsTranxID() {
		return sTranxID;
	}


	public void setsTranxID(long sTranxID) {
		this.sTranxID = sTranxID;
	}


	public long geteTranxID() {
		return eTranxID;
	}


	public void seteTranxID(long eTranxID) {
		this.eTranxID = eTranxID;
	}

	
	/**
	 * @return the runtimeID
	 */
	public long getRuntimeID() {
		return runtimeID;
	}


	/**
	 * @param runtimeID the runtimeID to set
	 */
	public void setRuntimeID(long runtimeID) {
		this.runtimeID = runtimeID;
	}

	
	
	private void subtractHBIDsSize(RCElement e){
		
		this.subtractIncSize(e.getIncHBIDsSize());
		this.subtractDecSize(e.getDecHBIDsSize());
	}
	
	private void addHBIDsSize(RCElement e){
		
		this.addIncSize(e.getIncHBIDsSize());
		this.addDecSize(e.getDecHBIDsSize());
	}

	/* (non-Javadoc)
	 * @see java.util.ArrayList#add(int, java.lang.Object)
	 */
	@Override
	public void add(int index, RCElement element) {

		super.add(index, element);
		addHBIDsSize(element);
		
	}


	/* (non-Javadoc)
	 * @see java.util.ArrayList#add(java.lang.Object)
	 */
	@Override
	public boolean add(RCElement e) {

		boolean retcode = super.add(e);
		
		if(retcode == true){
			addHBIDsSize(e);
		}
		
		return retcode;
	}


	/* (non-Javadoc)
	 * @see java.util.ArrayList#addAll(java.util.Collection)
	 */
	@Override
	public boolean addAll(Collection<? extends RCElement> c) {

		boolean retcode = super.addAll(c);
		
		if(retcode == true){
			
			for(RCElement e : c ){

				addHBIDsSize(e);
			}
		}

		return retcode;
	}


	/* (non-Javadoc)
	 * @see java.util.ArrayList#addAll(int, java.util.Collection)
	 */
	@Override
	public boolean addAll(int index, Collection<? extends RCElement> c) {
		
		boolean retcode = super.addAll(index, c);
		
		if(retcode == true){
			
			for(RCElement e : c ){

				addHBIDsSize(e);
			}
		}
		
		return retcode;
	}


	/* (non-Javadoc)
	 * @see java.util.ArrayList#remove(int)
	 */
	@Override
	public RCElement remove(int index) {
		
		RCElement e = super.remove(index);
		
		if(e != null){
			subtractHBIDsSize(e);
		}
		
		return e;
	}


	/* (non-Javadoc)
	 * @see java.util.ArrayList#remove(java.lang.Object)
	 */
	@Override
	public boolean remove(Object o) {
		
		boolean retcode = super.remove(o);
		
		if(retcode == true){
			
			RCElement e = (RCElement)o;
			
			subtractHBIDsSize(e);
		}
		
		return retcode;
	}


	/* (non-Javadoc)
	 * @see java.util.AbstractCollection#removeAll(java.util.Collection)
	 */
	@Override
	public boolean removeAll(Collection<?> c) {

		boolean retcode = super.removeAll(c);
		
		if(retcode == true && (c instanceof RCUArrayList) ){
			
			RCUArrayList ul = (RCUArrayList)c;
			
			for(RCElement e : ul){
				subtractHBIDsSize(e);
			}
		}

		return retcode;
	}



	/* (non-Javadoc)
	 * @see java.util.ArrayList#clear()
	 */
	@Override
	public void clear() {
		
		this.updateID = -1;
		this.incSize.set(0);
		this.decSize.set(0);
		
		this.sTranxID = 0;
		this.eTranxID = 0;
		
		this.runtimeID = -1;
		
		super.clear();
	}

	
	public byte[] getLogInfo() throws IOException{
		
		byte[] info = null;
		
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bos);
		
		try {
			
			dos.writeLong(this.updateID);
			dos.writeInt(incSize.get());
			dos.writeInt(decSize.get());
			dos.writeLong(sTranxID);
			dos.writeLong(eTranxID);
			//dos.writeLong(runtimeID); /* it's not useful for now */
			
			dos.flush();
			
		} catch (IOException e) {
			
			LOG.error("Log info write FAIL!");
			e.printStackTrace();
			throw e;
			
		} finally {
			
			info = bos.toByteArray();
			
			try {
				
				dos.close();
				bos.close();
				
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}

		return info;
		
	}
	
	public void setLogInfo(byte[] info) throws IOException{
		
		ByteArrayInputStream bis = new ByteArrayInputStream(info);
		
		DataInputStream dis = new DataInputStream(bis);
		
		try{
			
			this.updateID = dis.readLong();
			this.incSize.set(dis.readInt());
			this.decSize.set(dis.readInt());
			this.sTranxID = dis.readLong();
			this.eTranxID = dis.readLong();
			//this.runtimeID = dis.readLong();
			
		} catch (IOException e){
			
			LOG.error("Log info read FAIL!");
			throw e;
			
		} finally {
			
			dis.close();
			bis.close();
		}
		
	}
	
	
	

	/* (non-Javadoc)
	 * @see java.util.AbstractList#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object o) {

		if(o instanceof RCUArrayList){
			
			RCUArrayList cl = (RCUArrayList)o;
			
			if(this.getUpdateID() == cl.getUpdateID() 
					&& this.getsTranxID() == cl.getsTranxID()
					&& this.geteTranxID() == cl.geteTranxID()
					&& this.getIncSize() == cl.getIncSize()
					&& this.getDecSize() == cl.getDecSize() )
			{
				return true;
			}
			
		}else{
			
			return false;
		}
		
		return super.equals(o);
	}


	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		
		String s = "RCUArrayList [updateID = " + updateID + "], inc HBs = " + incSize.get() + ", dec HBs = " + decSize.get() 
					+ ", sTranxID = " + sTranxID + ", eTranxID = " + eTranxID + ", runtimeID = " + runtimeID + "\n";
		
		if(LOG.isDebugEnabled()){
			
			Iterator<RCElement> iter = this.iterator();
			RCElement e;
			
			while(iter.hasNext()){
				e = iter.next();
				if(e != null){
					s += e.toString() + "\n";
				}
			}
		}
		
		
		return s;
	}
	
	
	
	
	
}
