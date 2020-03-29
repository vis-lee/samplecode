/**
 * 
 */
package org.itri.ccma.server.namenode.rcmanager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * @author Vis Lee
 * RCUTxBuffer used to be a buffer for holding translation data 
 * which translate from RCElement
 */
class RCUTxBuffer {

	public final Log LOG = LogFactory.getLog(RCElement.class.getName());
	
	
	private RCULongBuffer incRCs = null;
	private RCULongBuffer decRCs = null;


	
	public RCUTxBuffer(){
		this(-1, -1);
	}
	
	public RCUTxBuffer(int incSize, int decSize) {
		
		super();
		
		incRCs = new RCULongBuffer(incSize);
		decRCs = new RCULongBuffer(decSize);
		
	}
	
	/**
	 * @return the incRCs
	 */
	public RCULongBuffer getIncRCs() {
		return incRCs;
	}

	/**
	 * @return the decRCs
	 */
	public RCULongBuffer getDecRCs() {
		return decRCs;
	}

	/**
	 * @return the buffer size
	 */
	public int getIncRCSize() {
		return incRCs.getSize();
	}

	/**
	 * @return the buffer size
	 */
	public int getDecRCSize() {
		return decRCs.getSize();
	}

	@Override
	public String toString() {
		
		String s = "txBuf incRC size = " + this.getIncRCSize() + ", txBuf decRC size = " + this.getDecRCSize() 
		+ ", total space = " + (this.getIncRCSize() + this.getDecRCSize());
		
		return s;
	}

	
	
	
	
	
	
}


