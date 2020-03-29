/**
 * 
 */
package org.itri.ccma.server.namenode.rcmanager;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.itri.ccma.server.namenode.rcmanager.interfaces.RCMConstant;

/**
 * @author 980263
 *
 */
public class RCMID {

	public final Log LOG = LogFactory.getLog(RCMID.class.getName());
	
	
	//odd ID file
	transient private RandomAccessFile IDFile;
	
	//even ID file
	transient private RandomAccessFile evIDFile;
	
//	ReentrantReadWriteLock rwlock = new ReentrantReadWriteLock(false);

	final String fname = "RCMID.data";
	
	
	//editlog transaction id
	private long eTranxID = DEFAULT_ETRANXID;
	
	//RCM id. can't be reused, even restart.
	private AtomicLong updateID = new AtomicLong(DEFAULT_UPDATEID);

	static public final int DEFAULT_ETRANXID = -1;
	static public final int DEFAULT_UPDATEID = 0;
	
	
	public RCMID(String rcmpath) {
		
		super();
		
		initIDFile(rcmpath);
		
		//read IDs from file
		try {
			read();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	/**
	 * @return the eTranxID
	 */
	public long geteTranxID() {
		return this.eTranxID;
	}

	/**
	 * @param eTranxID the eTranxID to set
	 */
	public void seteTranxID(long eTranxID) {
		this.eTranxID = eTranxID;
	}

	/**
	 * @param rcmid the rCMID to set
	 */
	private void setUpdateID(long updateID) {
		this.updateID.set(updateID);
	}
	
	/**
	 * @return the rCMID
	 */
	public long getUpdateID() {
		return this.updateID.get();
	}

	/**
	 * we recorded the last updated  RCL ID with it's corresponding eTrnaxID.
	 * 
	 * @param eTranxID	- the lastest eTranxID from user
	 * @return RCMID	- ID for next rc list
	 * 
	 */
	public long updateID(long eTranxID) {
		
		long retID = 0;
		
		if(eTranxID >= this.eTranxID){
			
			try {
				
//				rwlock.writeLock().lock();
				
				//update 
				seteTranxID(eTranxID);
				
				write();
				
			} catch (IOException e) {

				e.printStackTrace();
				
			} finally {
				
//				rwlock.writeLock().unlock();
			}
			
			retID = updateID.incrementAndGet();
		
		}else{
			
			LOG.error("updating eTranxID = " + eTranxID + " error! which is less than RCMID.eTranxID = " + this.eTranxID);
		}
		
		
		return retID;
	}


	synchronized public void reset(){
		
		LOG.debug("the record eTranxID = " + eTranxID + ", updateID = " + updateID.get());
		
		try {
			
			//update 
			seteTranxID(DEFAULT_ETRANXID);
			updateID.set(0);
			
			write();
			
			read();
			
		} catch (IOException e) {

			e.printStackTrace();
			
		}

	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	private void initIDFile(String dir) {
		
		//TODO file HA could be achieved by writing to 2 files
		//or appending to the end of same file
		if(dir == null){
			dir = RCMConstant.rcm_defaultpath;
		}
		
		File file = new File(dir);
		
		//check dir existence.
		if(!file.exists()){
			file.mkdirs();
		}
		
		//check file existence
		try {
			
			createIDFile( dir+File.separator+fname );
			
		} catch (IOException e) {
			
			LOG.error("RCM ID file open exception: " + e.getMessage());
		}

		
	}

	synchronized private void createIDFile(String fpath) throws IOException
	{
		File file = new File(fpath);
		
		if (!file.exists())
		{
			IDFile = new RandomAccessFile(file, "rw");
			
			//write default value
			updateID(-1);
			
		}else{
			
			IDFile = new RandomAccessFile(file, "rw");
		}
	}
	
	
	synchronized private void write() throws IOException{
		
		
		//go to head
		IDFile.seek(0);
		//IDFile.length();
		
		//write data
		IDFile.writeLong(geteTranxID());
		IDFile.writeLong(getUpdateID());
		
		//actual flush to disk. ie, sync on FD of filesystem.
		IDFile.getFD().sync();
		
		
	}
	
	/**
	 * 
	 * @return next available rcl id.
	 * @throws IOException
	 */
	synchronized private long read() throws IOException{
		
		
		//go to head
		IDFile.seek(0);
		
		//read data
		seteTranxID( IDFile.readLong() );
		updateID.set( IDFile.readLong() );
		
		//increase rclid.
		return updateID.incrementAndGet();
		
	}

}
