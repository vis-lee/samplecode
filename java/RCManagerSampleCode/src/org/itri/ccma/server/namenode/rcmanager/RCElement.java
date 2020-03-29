/**
 * 
 */
package org.itri.ccma.server.namenode.rcmanager;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.itri.ccma.server.namenode.DMSConstant;
import org.itri.ccma.server.namenode.rcmanager.interfaces.IRCManager;

/**
 * @author Vis Lee
 *
 */
public class RCElement implements Externalizable{

	private long tranxID;
	
	private ArrayList<Long> incHBIDs;
	private ArrayList<Long> decHBIDs;
	
	transient private long seqID;

	transient public final Log LOG = LogFactory.getLog(RCElement.class.getName());

	
	//protected ReentrantWriterPreferenceReadWriteLock rceLock = new ReentrantWriterPreferenceReadWriteLock();
	
	public RCElement() {
		this(-1, null, null);
	}
	
	public RCElement(long tranxID) {
		this(tranxID, null, null);

	}

	public RCElement(ArrayList<Long> incHBIDs, ArrayList<Long> decHBIDs) {
		this(-1, incHBIDs, decHBIDs);

	}

	public RCElement(long tranxID, ArrayList<Long> incHBIDs, ArrayList<Long> decHBIDs) {
		
		super();
		
		this.tranxID = tranxID;
		
		if(incHBIDs == null){
			incHBIDs = new ArrayList<Long>(DMSConstant.MAX_NR_HBS_IN_REQUEST);
		}
		
		this.incHBIDs = incHBIDs;
		
		if(decHBIDs == null){
			decHBIDs = new ArrayList<Long>(DMSConstant.MAX_NR_HBS_IN_REQUEST);
		}
		
		this.decHBIDs = decHBIDs;
		
		this.seqID = 0;

	}
	
	
	public void resetRCElement(){
		
		synchronized(incHBIDs){
			this.incHBIDs.clear();
		}
		
		synchronized(decHBIDs){
			this.decHBIDs.clear();
		}
		
		this.tranxID = 0;
	}

	/**
	 * @return the traxID
	 */
	public long getTranxID() {
		return tranxID;
	}

	/**
	 * @param tranxID the tranxID to set
	 */
	protected void setTranxID(long tranxID) {
		this.tranxID = tranxID;
	}

	
	/**
	 * @return the incHBIDs
	 */
	public ArrayList<Long> getIncHBIDs() {
		return incHBIDs;
	}


	/**
	 * @return the decHBIDs
	 */
	public ArrayList<Long> getDecHBIDs() {
		return decHBIDs;
	}


	/**
	 * @return the seqID
	 */
	public long getSeqID() {
		return seqID;
	}

	/**
	 * @param seqID the seqID to set
	 */
	synchronized public void setSeqID(long seqID) {
		
		if(seqID > this.seqID){
			this.seqID = seqID;
		}
		
	}
	
	
	
	

	/**
	 * add HBIDs into RCElement.
	 * @param uhbids
	 * @param size
	 * @return false if one failed.
	 */
	public boolean addHBIDs(List<Long> uhbids, int updateType){
		
		boolean result = false;
		
		switch(updateType){
		
		case IRCManager.RCU_TYPE_INC_RC:
			
			synchronized(this.incHBIDs){
				result = this.incHBIDs.addAll(uhbids);
			}
			
			break;
			
		case IRCManager.RCU_TYPE_DEC_RC:
			
			synchronized(this.decHBIDs){
				result = this.decHBIDs.addAll(uhbids);
			}
			
			break;
		
		default:
		
			LOG.error("FATAL ERROR!, You panic me..., which type you want exactly: " + updateType);
			
		}
		
		return result;
		
	}

	
	/**
	 * @return the size of incHBIDs
	 */
	public int getIncHBIDsSize() {
		return incHBIDs.size();
	}


	/**
	 * @return the size of decHBIDs
	 */
	public int getDecHBIDsSize() {
		return decHBIDs.size();
	}
	
	
	public int getTotalSize(){
		
		return (this.incHBIDs.size() + this.decHBIDs.size());
	}
	
	
	
	/**
	 * fill the primitive type (long) of the elements of hbids set into the buf
	 * @param txBuf
	 * @return
	 * @throws IOException
	 */
	public int fill2Buf(RCUTxBuffer txBuf) throws IOException{
		
		int size = 0;
		
		try{
			size += writeHBIDsToOStream(txBuf.getIncRCs(), incHBIDs);
		}catch (ArrayIndexOutOfBoundsException e){
			LOG.error(txBuf.toString());
			LOG.error("incHBIDs Size = " + incHBIDs.size() + ", incHBIDs = " + incHBIDs.toString() );
			throw e;
		}
		
		try{
			size += writeHBIDsToOStream(txBuf.getDecRCs(), decHBIDs);
		}catch (ArrayIndexOutOfBoundsException e){
			LOG.error(txBuf.toString());
			LOG.error("decHBIDs Size = " + decHBIDs.size() + ", decHBIDs = " + decHBIDs.toString() );
			throw e;
		}
		
		
		return size;
		
	}
	
	

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "RCElement [traxID = " + tranxID + ", incHBIDs = " + incHBIDs.toString() +  ", decHBIDs = " + decHBIDs.toString() + "]";
	}

	
	
	/**
	 * fill HBIDs into array list from input stream.
	 * @param in
	 * @param array
	 * @param len
	 * @throws IOException
	 */
	protected int fillHBIDsFromIStream(ObjectInput in, ArrayList<Long>array, int len) throws IOException{
		
		int i = 0;
		
		for(i = 0; i < len; i++){
			
			array.add( new Long(in.readLong()) );
		}
		
		return i;
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		
		this.tranxID = in.readLong();
		int incSize = in.readInt();
		int decSize = in.readInt();
		
		fillHBIDsFromIStream(in, incHBIDs, incSize);
		fillHBIDsFromIStream(in, decHBIDs, decSize);
		
	}

	/**
	 * absorb HBIDs from array list and write to output stream.
	 * @param out
	 * @param array
	 * @throws IOException
	 */
	protected int writeHBIDsToOStream(ObjectOutput out, ArrayList<Long>array) throws IOException{
		
		int i = 0;
		
		for( Long hbid: array){
			
			if(hbid != null){
				out.writeLong( hbid.longValue() );
				i++;
			}
		}
		
		return i;
	}
	
	
	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		
		out.writeLong(this.tranxID);
		out.writeInt(getIncHBIDsSize());
		out.writeInt(getDecHBIDsSize());
		
		try{
			writeHBIDsToOStream(out, incHBIDs);
		}catch (ArrayIndexOutOfBoundsException e){
			LOG.error("incHBIDs Size = " + incHBIDs.size() + ", incHBIDs = " + incHBIDs.toString() );
			throw e;
		}
		
		try{
			writeHBIDsToOStream(out, decHBIDs);
		}catch (ArrayIndexOutOfBoundsException e){
			LOG.error("decHBIDs Size = " + decHBIDs.size() + ", decHBIDs = " + decHBIDs.toString() );
			throw e;
		}
		
		
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {

		if( !(obj instanceof RCElement) ){
			return false;
		}
		
		RCElement b = (RCElement) obj;
		
		return ( this.incHBIDs.equals(b.getIncHBIDs()) && this.decHBIDs.equals(b.getDecHBIDs()) );
	}
	
	
}


