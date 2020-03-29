/**
 * 
 */
package org.itri.ccma.server.namenode.rcmanager;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Vis Lee
 * 
 * record update id information.
 *
 */
public class RCUpdateInfo implements Serializable{


	/**
	 * auto generated serialVersionUID
	 */
	private static final long serialVersionUID = -3666652339350322898L;


	private long updateID = -1;
	
	private AtomicInteger incSize = new AtomicInteger(0);
	private AtomicInteger decSize = new AtomicInteger(0);
	
	private long sTranxID;
	private long eTranxID;
	
	private long runtimeID;


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

	
	
	public void clear() {
		
		this.updateID = -1;
		this.incSize.set(0);
		this.decSize.set(0);
		
		this.sTranxID = 0;
		this.eTranxID = 0;
		
		this.runtimeID = 0;
		
	}
	
}
