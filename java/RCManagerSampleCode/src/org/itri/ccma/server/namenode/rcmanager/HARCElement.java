/**
 * 
 */
package org.itri.ccma.server.namenode.rcmanager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.itri.ccma.editlog.interfaces.IEditlogModule;
import org.itri.ccma.editlog.manager2.EditlogEngine;
import org.itri.ccma.server.namenode.DMSConstant;
import org.itri.ccma.server.namenode.DMSFlags;

/**
 * @author Vis Lee
 *
 */
public class HARCElement implements IEditlogModule {


	public final Log LOG = LogFactory.getLog(this.getClass().getName());
	
	private RCManager rcm = null;
	
	public HARCElement(RCManager rcm) {
		
		if(DMSFlags.ENABLE_EDITLOG){
			
			//register module to elog
			EditlogEngine.registerModule(DMSConstant.MODULE_ID_RCElement, this, this.getClass().getName());
		}

		this.rcm = rcm;
	}
	

	@Override
	public void undo(byte[] data) throws Exception {
		// do nothing, because caller (ex: BM) will take care of this.

	}

	@Override
	public void redo(byte[] data) throws Exception {
		// do nothing, because caller (ex: BM) will take care of this.

	}

	@Override
	public boolean flushTimeout(long flushElement) {
		
		LOG.warn("we shouldn't meet this situation! edit log timeout callback function, so I will change to FileMode");
		
		//try to simply switch to file mode
		RCPersistance pdev = rcm.getPersistDev();
		
		pdev.acquireLock();
		pdev.switchToFileMode();
		pdev.releaseLock();
		
		//wait for flush
		rcm.flushRCManager();
		
		return false;
	}

}
