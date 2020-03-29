package org.itri.ccma.server.namenode.rcmanager.interfaces;

import java.io.IOException;

import org.itri.ccma.server.namenode.rcmanager.RCUArrayList;
import org.itri.ccma.server.namenode.rcmanager.exception.NoSupportMethodException;

public interface IRCUPersistance {

	/**
	 * write to persistence device
	 * @param lists
	 * @return	0	: means nothing to do.
	 * 			>0	: number of elements for updating 
	 * 			<0	: error code
	 * @throws IOException
	 */
	public int write(RCUArrayList lists) throws IOException;
	
	public static final int DEV_WRITE_DONE = 1;
	public static final int DEV_WRITE_FAIL = -256;
	public static final int DEV_WRITE_EXIST = -257;
	
	/**
	 * read operation
	 * @return RCUArrayList; null if there is no element.
	 * @throws NoSupportMethodException; if not implement, for example, DssRpcCall doesn't provide Query operation.
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 */
	public RCUArrayList read() throws NoSupportMethodException, IOException, ClassNotFoundException;
	
	public static final int DEV_READ_DONE = DEV_WRITE_DONE;
	public static final int DEV_READ_FAIL = DEV_WRITE_FAIL;
	public static final int DEV_READ_EXIST = DEV_WRITE_EXIST;
	
	/**
	 * prepare and clean the needed data for acknowledging from outside. 
	 * Because we need to call complete function to simulate remote device 
	 * reply to finish whole remote procedure call while remote device down.
	 * @param lists
	 * @return
	 */
	public boolean prepareData(RCUArrayList lists);

	public boolean cleanData(long updateID);
	
	
	/**
	 * this RCUArrayList has completed, do clean stuffs.
	 * @param lists
	 * @return user defined return code
	 * @throws NoSupportMethodException
	 */
	public long complete(long updateID) throws NoSupportMethodException;
	
	public static final int ID_NOT_FOUND = -128;
	
	/**
	 * check persistence device status.
	 * @return true, if device is available, or you can define yours.
	 */
	public int checkState();
	
	public static final int DEV_RUNNING = 1;
	public static final int DEV_DOWN = -1;

	
}
