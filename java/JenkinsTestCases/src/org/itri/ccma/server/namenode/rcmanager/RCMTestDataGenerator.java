package org.itri.ccma.server.namenode.rcmanager;

import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.itri.ccma.server.namenode.DMSConstant;

public class RCMTestDataGenerator {

	public final static Log LOG = LogFactory.getLog(RCMTestDataGenerator.class.getName());
	
	
	protected static int nr_data = 100*1000;
	protected static ArrayList< RCElement > dataset;
	
	protected static ConcurrentHashMap<Long, RCElement> cache;
	
	protected static final int MAX_CACHE_ENTRY = 1*1000*1000;

	private static Random rand = new Random();
	
	
	public RCMTestDataGenerator( ) {

		initDataSet();
	}

	/*
	 * generate one HBlockInfoJCS with random HBID
	 */
	protected static ArrayList<Long> generateHBlock(int numHBs){
		
		ArrayList<Long> hbids = new ArrayList<Long>(numHBs);
		
		for(int i = 0; i < numHBs; i++) {
			
			//prepare HBlockInfoJCS object
			hbids.add( new Long(rand.nextLong()) );
		}
		
		return hbids;
	}
	
	static public void initDataSet(){
		
		int num = RCMTestDataGenerator.nr_data;
		
		dataset = new ArrayList< RCElement >(RCMTestDataGenerator.nr_data);
		
		cache = new ConcurrentHashMap<Long, RCElement>();
		
		while( num-- != 0 ){
			dataset.add( new RCElement( 
								generateHBlock(rand.nextInt(DMSConstant.MAX_NR_HBS_IN_REQUEST)),
								generateHBlock(rand.nextInt(DMSConstant.MAX_NR_HBS_IN_REQUEST))
								) );
		}
		
	}
	
	
	static protected RCElement getRecordData(Long tranxID){
		
		RCElement old = null;
		RCElement data = null;
		
		try{
			data = cache.get(tranxID);
			
			if(data == null ){
				
				while(cache.size() > MAX_CACHE_ENTRY){
					
					try {
						Thread.sleep(500);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
				data = getDisposableData();
				old = cache.putIfAbsent(tranxID, data);
				
				if( old != null ){
					LOG.error("Error! there is old one!!");
					data = old;
				}
			}
			
		}catch(NullPointerException e){
			throw e;
		}
		
		return data;
		
	}
	
	
	static protected void printMap(){
		
		Set< Map.Entry<Long, RCElement> > set = cache.entrySet();
		
		for(Map.Entry<Long, RCElement> e: set){
			LOG.info( "entry key = " + e.getKey() + ", entry vavlue = " + e.getValue().toString() );
		}
		
	}
	
	
	
	static protected void removeRecordData(Long tranxID){
		
		RCElement rce;
		
		try{
			
			rce = cache.remove(tranxID);
			
			if(rce == null){
				LOG.error("remove fail! tranxID = " + tranxID);
			}
			
			rce = null;
			
			if( (tranxID.longValue() % 100) == 0){
				LOG.debug("map size = " + cache.size());
				//printMap();
			}
			
		}catch(NullPointerException e){
			throw e;
		}
		
	}
	
	
	
	protected static RCElement getDisposableData(){
		
		RCElement data = null;
		
		data = dataset.get( rand.nextInt(nr_data) );
		
		return data;
		
	}
	
	static protected long getSize(){
		
		return cache.size();
		
	}
	
	
	//******************************************************************************************//
	//																							//
	// 											END												//
	//																							//
	//******************************************************************************************//
	
}
