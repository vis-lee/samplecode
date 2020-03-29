/**
 * 
 */
package org.itri.ccma.server.namenode.rcmanager;

import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.itri.ccma.server.namenode.DMSConstant;
import org.itri.ccma.server.namenode.rcmanager.RCList.LSTATE;

/**
 * @author 980263
 *
 */
public class RCListPool {

	public final Log LOG = LogFactory.getLog(RCListPool.class.getName());
	
	private LinkedList<RCList> rclPool;
	
	private int poolSize = DMSConstant.RCM_DEFAULT_RCLPOOL_SIZE;
	
	private long calInterval = 5*60*1000;		//5 mins
	private long lastUpdateTime = 0;
	
	public RCListPool() {
		super();
		
		rclPool = new LinkedList<RCList>();
		
		for(int i = 0; i < poolSize; i++){
			
			rclPool.add( new RCList(0) );
			
		}
		
		lastUpdateTime = System.currentTimeMillis();
		
	}
	
	

	public RCList getRCList(){
		
		RCList rclist = null;
		
		synchronized (rclPool){
			
			rclist = rclPool.pollFirst();
			
			if(rclist != null){
				
				rclist.setListState(LSTATE.USING);
				
			}else{
				
				//extend
				rclist = new RCList(0);
				poolSize++;
			}
		}
		
		return rclist;
	}
	
	public void putRCList(RCList rclist){
		
		rclist.setListState(LSTATE.PENDING);
		
		rclist.resetRCList();
		
		
		synchronized (rclPool){
			
			//put back to pool
			rclPool.addLast(rclist);
		}
		
		//check pool size every 5 mins
		if( (lastUpdateTime+calInterval) < System.currentTimeMillis() ){
			trimPool();
		}
	}
	
	/**
	 * trim some list if the pool size is too big.
	 */
	public void trimPool(){
		
		RCList rclist = null;
		int inPool = rclPool.size();
		int onUsing = 0;
		int trimTo = 0;
		
		onUsing = poolSize - inPool;
		
		trimTo = (int) (onUsing * 1.2);
		
		if(LOG.isDebugEnabled()){
			LOG.debug("poolSize = " + poolSize + ", in pool = " + inPool + ", onUsing = " + onUsing + ", trimTo = " + trimTo);
		}
		
		synchronized (rclPool){
			
			while(poolSize > trimTo){
				
				//get list
				rclist = rclPool.pollFirst();
				
				if(rclist != null){
					
					//release it
					rclist = null;
					
					poolSize--;
					
				} else {
					
					//no more inPool lists
					break;
				}
				
			}
			
			//update time
			lastUpdateTime = System.currentTimeMillis();
			
		}
		
		if(LOG.isDebugEnabled()){
			LOG.debug("after trimed, the pool size = " + rclPool.size());
		}
		
	}
	
	
	
	
	
}
