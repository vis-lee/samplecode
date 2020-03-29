/**
 * 
 */
package org.itri.ccma.server.namenode.rcmanager;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.SyncFailedException;
import java.util.Comparator;
import java.util.Queue;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.itri.ccma.server.namenode.rcmanager.exception.NoSupportMethodException;
import org.itri.ccma.server.namenode.rcmanager.interfaces.IRCUPersistance;
import org.itri.ccma.server.namenode.rcmanager.interfaces.RCMConstant;

/**
 * @author Vis Lee
 *
 */
public class PersistToFile implements IRCUPersistance {

	
	public final Log LOG = LogFactory.getLog(PersistToFile.class.getName());
	
	
	private String defaultpath = RCMConstant.rcm_defaultpath;;
	
	static private final String prefix = "RCUpdateFile_";
	
	private File folder;
	
	private ConcurrentLinkedQueue<File> fq;
	
	
	private RCUArrayList filePending;
	
	
	
	public PersistToFile(String path) {
		super();
		
		if(path != null){
			this.defaultpath = path;
		}
		
		if(fq == null){
			initFileQueue();
		}
		
	}


	synchronized public File syncGetFolderHandler(){
		
		if(folder == null){
			
			folder = new File(defaultpath);
			
			if(!folder.exists()){
				
				folder.mkdirs();
			}

		}
		
		return folder;
		
	}
	

	public File getFolderHandler(){
		
		if(folder == null){
			
			folder = syncGetFolderHandler();
			
		}
		
		return folder;
		
	}
	
	
	public File getFileHandler(String fname){

		File f = null;
		
		if(fname != null){
			
			 f = new File(defaultpath + File.separator + fname);
				
			/*if(!f.exists()){
				return null;
			}*/
		}
		
		return f;
		
	}
	
	
	private Queue<File> syncGetFileQueue(){
		
		if(fq == null){
			
			fq = new ConcurrentLinkedQueue<File>();
			
			File folder = getFolderHandler();
			File[] files = folder.listFiles();
			
			TreeSet<File> ts = new TreeSet<File>(new RCMFileNameComparator());
			
			String fname;
			
			for(File f: files){
				
				fname = f.getName();
				
				if(fname.startsWith(prefix)){
					
					//put to tree
					ts.add(f);
				}
				
			}
			
			fq.addAll(ts);
			
		}
		
		return fq;
	}
	
	public long getUpdateIDFromFilename(File f){
		return Long.valueOf( f.getName().substring(prefix.length()) );
	}
	
	private Queue<File> initFileQueue(){
		
		if(fq == null){
			return syncGetFileQueue();
		}
		
		return fq;
	}
	
	
	public void putNewPendingFile(File f){
		
		if(fq != null){
			
			fq.add(f);
			
		}else{
			
			LOG.error("File Queue is NULL!");
		}
		
	}
	
	public File getNextReadFile(){
		
		if(fq != null){
			
			return fq.peek();
			
		}else{
			
			return null;
		}
		
	}

	public File removeFromFileQueue(long updateID){
		
		if(fq != null){
			
			File f = fq.peek();
			
			//spilit ID out
			long feid = getUpdateIDFromFilename(f);
			
			if(feid == updateID){
				
				if(LOG.isDebugEnabled()){
					LOG.debug("remove updateID = " + updateID + " from fp.");
				}
				
				//remove from Q
				return fq.remove();
				
			}else{
				
				LOG.error("completed UpdateID = " + updateID + " isn't match the ID = " + feid +" which is the first element in the FileQueue!");
			}
			
		}
		
		return null;
		
	}


	
	
	
	/**
	 * how we handle crash case while writing: the RCM recover RCElements, also HARCMU 
	 * re-build the update list and doUpdate() function. PersistToFile would return error
	 * code "DEV_WRITE_EXIST" to caller which would trigger a compare and error handling.
	 * In consequence, caller should retry it again and expect success.
	 * @param f
	 * @param lists
	 * @return
	 * @throws FileNotFoundException
	 * @throws SyncFailedException
	 * @throws IOException
	 */
	public  boolean writeToFile(File f, RCUArrayList lists) throws FileNotFoundException, 
																		 SyncFailedException, 
																		 IOException 
	{
		
		boolean retcode = false;
		RandomAccessFile raf = new RandomAccessFile(f, "rw");
		
		try{
			
			FileDescriptor fd = raf.getFD();
			FileOutputStream fos = new FileOutputStream(fd);
	        ObjectOutputStream oos = new ObjectOutputStream( fos );
			
	        try{
	        	
				oos.writeObject(lists);
				
				oos.flush();
				
				//sync to disk
				fd.sync();
				
				retcode = true;
				
	        }finally{
	        	
	        	//close stream
	        	oos.close();
	        }
			
			
		}finally{
			
			//close file handler
			raf.close();
		}
		
		return retcode;
		
	}
	
	
	
	public RCUArrayList readFromFile(File f) throws IOException, 
													ClassNotFoundException
	{
		
		RCUArrayList lists = null;
		
		RandomAccessFile raf = new RandomAccessFile(f, "rw");
		
		try{
			
			FileDescriptor fd = raf.getFD();
			
			FileInputStream fis = new FileInputStream(fd);
	        ObjectInputStream ois = new ObjectInputStream( fis );
			
	        try{
	        	
	        	lists = (RCUArrayList)ois.readObject();
	        	
	        }finally{
	        	
	        	//close stream
	        	ois.close();
	        }
			
		}finally{
			
			//close file handler
			raf.close();
		}
		
		
		return lists;
		
	}
	
	/* (non-Javadoc)
	 * @see org.itri.ccma.server.namenode.rcmanager.IRCUPersistance#write(org.itri.ccma.server.namenode.rcmanager.RCUArrayList)
	 */
	@Override
	public int write(RCUArrayList lists) throws IOException {

		String fname = prefix + lists.getUpdateID();
		
		File f = null;
		
		if((f = getFileHandler(fname)) != null){
			
			//check the file existence
			if( (f.exists() == false) ){
				
				//prepare to store to a file
				if( writeToFile(f, lists) ){
					
					/*
					 * record in the file pending Q.
					 * N.B. there is no race condition 
					 * between updater and FileCleaner 
					 * due to we use lock to ensure atomic operation.
					 */
					putNewPendingFile(f);
					
					if(LOG.isDebugEnabled()){
						
						LOG.debug("updateID = " + lists.getUpdateID() + " was stored to file" );
					}
					
					return DEV_WRITE_DONE;
				}
				
			} else {
				
				LOG.error("File exist! name = " + fname);
				return DEV_WRITE_EXIST;
			}
			
		}else{
			
			LOG.error("get File Handler Fail! name = " + fname);
		}
		
		return DEV_WRITE_FAIL;
	}

	/* (non-Javadoc)
	 * @see org.itri.ccma.server.namenode.rcmanager.IRCUPersistance#read()
	 */
	@Override
	public RCUArrayList read() throws IOException, ClassNotFoundException {
		
		File f = getNextReadFile();
		
		if(f != null){
			
			//check the file existence
			if( /*(f = getFileHandler(fname)) != null &&*/ (f.exists() == true) ){
				
				//prepare to read from a file
				filePending = readFromFile(f);
				
				return filePending;
				
			}else {
				
				LOG.error("File is NOT exist! name = " + f.getName());
				
				removeFromFileQueue( getUpdateIDFromFilename(f) );
			}
			
		}else{
			
			LOG.info("No more files~");
		}

		
		return null;
	}



	@Override
	public boolean prepareData(RCUArrayList lists) {
		
		LOG.info("You shouldn't set data in this module!");
		return true;
	}
	

	@Override
	public boolean cleanData(long updateID) {
		
		LOG.info("update ID = " + updateID + " is cleaning from file module...");
		
		//clean file pending
		filePending = null;
		
		//check the file existence
		File f = getFileHandler( (prefix + updateID) );
		
		//remove the file
		return f.delete();

	}
	
	
	
	
	
	
	
	
	/* (non-Javadoc)
	 * @see org.itri.ccma.server.namenode.rcmanager.IRCUPersistance#complete(org.itri.ccma.server.namenode.rcmanager.RCUArrayList)
	 */
	/**
	 * @return 	ID_NOT_FOUND
	 * 			-1: file not found
	 */
	@Override
	public long complete(long updateID) throws NoSupportMethodException {
		
		//if not my business
		if(filePending == null || filePending.getUpdateID() != updateID){
			return ID_NOT_FOUND;
		}
		
		
		LOG.info("update ID = " + updateID + " is acking to File...");
		
		File f = removeFromFileQueue(updateID);
		
		//check the file existence
		if( cleanData(updateID) ){
			
			return 0;
		}
		
		return -1;

	}


	/**
	 * File module status
	 * @return the number of files that wait for transmitting
	 */
	@Override
	public int checkState() {

		return fq.size()/*(nameMap.size()>0 ? DEV_RUNNING:0)*/;
	}
	
	
	protected void cleanPersistDev(){
		
		File f;
		
		//get from Q
		while( (f = getNextReadFile()) != null){
			
			//get id
			long uid = getUpdateIDFromFilename(f);
			
			//remove from fq
			f = removeFromFileQueue(uid);
			
			cleanData(uid);
		}
		
		
	}
	
	
	protected RCUArrayList read(long updateID) throws IOException, ClassNotFoundException {
		
		String fname = prefix + updateID;
		
		File f = null;
		
		if((f = getFileHandler(fname)) != null){
			
			//check the file existence
			if( (f.exists() == true) ){
				
				//prepare to read from a file
				return readFromFile(f);
				
			}else {
				
				LOG.error("File is NOT exist! name = " + f.getName());
				
			}
			
		}

		return null;
	}
	
	
	
	
	
	
	
	
	
	
	
	//file name comparator
	class RCMFileNameComparator implements Comparator<File>{

		@Override
		public int compare(File o1, File o2) {

			long o1id = getUpdateIDFromFilename(o1); 
			long o2id = getUpdateIDFromFilename(o2); 
			
			if(o1id == o2id){
				return 0;
			} else if(o1id > o2id){
				return 1;
			} else {
				return -1;
			}
			
		}

		
	}




	
	
	
	
	
	

}
