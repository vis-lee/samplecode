/**
 * 
 */
package org.itri.ccma.server.namenode.rcmanager;

import java.io.IOException;
import java.io.ObjectOutput;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.itri.ccma.server.namenode.DMSConstant;
import org.itri.ccma.server.namenode.rcmanager.exception.NoSupportMethodException;


/**
 * @author Vis Lee
 * RCLongBuffer used to be a buffer for holding translation data which translate from RCElement to long[].
 * N.B. This class is only used here!
 */
class RCULongBuffer implements ObjectOutput{

	public final Log LOG = LogFactory.getLog(RCElement.class.getName());
	
	private int size = DMSConstant.RCM_DSS_MAX_HBS;

	
//	private ByteBuffer bb = null;
//	private LongBuffer rcs = null;
	
	private long[] hbs = null;
	private int index = 0;
	
	

	public RCULongBuffer(){
		this(-1);
	}
	
	public RCULongBuffer(int size) {
		
		super();
		
		if(size >= 0){
			this.size = size;
		}
		
//		bb = ByteBuffer.allocate(this.size*(Long.SIZE/Byte.SIZE));
//		rcs = bb.asLongBuffer();
		
		hbs = new long[this.size];
		
	}
	
	/**
	 * @return the incRCs
	 */
	public long[] getRCs() {
		return hbs;
	}
	
	/**
	 * @return the total size
	 */
	public int getSize() {
		return size;
	}

//	/**
//	 * @return the current index
//	 */
//	public int getIndex() {
//		return index;
//	}
	
	
	public long[] toArray(){
		//return this.rcs.array();

		return hbs;
	}
	
	
	
	
	
	
	
	
	
	

	@Override
	public void writeLong(long v) throws IOException {
		//this.rcs.putLong(v);
		try{
			hbs[this.index++] = v;
		}catch (ArrayIndexOutOfBoundsException e){
			LOG.error("array capacity = " + size + ", index = " + (index-1));
			throw e;
		}
		
	}
	
	
	
	@Override
	public void write(int b) throws IOException {
		
		throw new NoSupportMethodException("Not supported");
	}

	@Override
	public void write(byte[] b) throws IOException {
		// TODO Auto-generated method stub
		throw new NoSupportMethodException("Not supported");
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		// TODO Auto-generated method stub
		throw new NoSupportMethodException("Not supported");
	}

	@Override
	public void writeBoolean(boolean v) throws IOException {
		// TODO Auto-generated method stub
		throw new NoSupportMethodException("Not supported");
	}

	@Override
	public void writeByte(int v) throws IOException {
		// TODO Auto-generated method stub
		throw new NoSupportMethodException("Not supported");
	}

	@Override
	public void writeBytes(String s) throws IOException {
		// TODO Auto-generated method stub
		throw new NoSupportMethodException("Not supported");
	}

	@Override
	public void writeChar(int v) throws IOException {
		// TODO Auto-generated method stub
		throw new NoSupportMethodException("Not supported");
	}

	@Override
	public void writeChars(String s) throws IOException {
		// TODO Auto-generated method stub
		throw new NoSupportMethodException("Not supported");
	}

	@Override
	public void writeDouble(double v) throws IOException {
		// TODO Auto-generated method stub
		throw new NoSupportMethodException("Not supported");
	}

	@Override
	public void writeFloat(float v) throws IOException {
		// TODO Auto-generated method stub
		throw new NoSupportMethodException("Not supported");
	}

	@Override
	public void writeInt(int v) throws IOException {
		// TODO Auto-generated method stub
		throw new NoSupportMethodException("Not supported");
	}

	@Override
	public void writeShort(int v) throws IOException {
		// TODO Auto-generated method stub
		throw new NoSupportMethodException("Not supported");
	}

	@Override
	public void writeUTF(String s) throws IOException {
		// TODO Auto-generated method stub
		throw new NoSupportMethodException("Not supported");
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		throw new NoSupportMethodException("Not supported");
	}

	@Override
	public void flush() throws IOException {
		// TODO Auto-generated method stub
		throw new NoSupportMethodException("Not supported");
	}

	@Override
	public void writeObject(Object arg0) throws IOException {
		// TODO Auto-generated method stub
		throw new NoSupportMethodException("Not supported");
	}

	
	
}


