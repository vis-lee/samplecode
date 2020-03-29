package org.itri.ccma.server.namenode;

import java.io.File;



public interface DMSConstant {
	
	
	//**************************************************//
	//													//
	//		           Name Node              			//
	//													//
	//**************************************************//

	/**
	 *	Module ID 
	 */
	//space manager
	public static final byte MODULE_ID_SM				= 1;
	public static final byte MODULE_ID_SM_LUNSPACEMNGR 	= 2;
	public static final byte MODULE_ID_SM_RBSPACEMNGR 	= 3;
	
	//hbid manager
	public static final byte MODULE_ID_HBID	  = 9;
	
	//block manager
	public static final byte MODULE_ID_BM        = 10;
	public static final byte MODULE_ID_BM_LCACHE = 11;
	public static final byte MoDULE_ID_BM_HCACHE = 12;
	
	//rb manager
	public static final byte MODULE_ID_RM		= 17;
	
	//volume manager
	public static final byte MODULE_ID_PAGE_CACHE_CORE 	= 41;
	public static final byte MODULE_ID_DMS_VOLUME 		= 42;
	public static final byte MODULE_ID_USER_VOLUME 		= 43;
	public static final byte MODULE_ID_DELTALIST 		= 44;

	//rc manager
	public static final byte MODULE_ID_RCUpdate	 		= 60;
	public static final byte MODULE_ID_RCElement		= 61;
		
	//dms task manager
	public static final byte MODULE_ID_TM = 66;
	
	//rs module
	public static final byte MODULE_ID_RS = 67;
	
	//dss module
	public static final byte MODULE_ID_DSS = 68;
	
	//datanode manager
	public static final byte MODULE_ID_DM = 70;
	
	


	
	/**
	 * 	LBlock
	 */
	public static final int LBlockSize_default	= (int)Math.pow(2, 12); 								//4 KB
	public static final int LBBlockSize			= LBlockSize_default;
	public static final int LBBlockSize_KB		= LBBlockSize/1024;
	public static final int NumOfLBBlockPerMB	= 1024/LBBlockSize_KB;
	
	
	/**
	 * 	HBlock
	 */
	public static final int HBlockSize_default		= (int) Math.pow(2, 12); 							//4 KB
	public static final int HBBlockSize				= HBlockSize_default;
//	public static final long HBIDForInvalidatedHB 	= -1;				//111111.....1 (-1)
	public static final long defaultMinHBID 		= 1;				//000000.....1 (1)	
	public static final long defaultMaxHBID 		= Long.MAX_VALUE;	//011111.....1 (9223372036854775807)

	public static final int HARD_SECTOR_SIEZ_IN_CLIENT 		= 512;
	public static final int MAX_BATCH_SECTORS_IN_CLIENT 	= 128;
	public static final int MAX_NR_LBS_IN_REQUEST 			= (HARD_SECTOR_SIEZ_IN_CLIENT * MAX_BATCH_SECTORS_IN_CLIENT) / LBBlockSize;
	public static final int MAX_NR_HBS_IN_REQUEST 			= (HARD_SECTOR_SIEZ_IN_CLIENT * MAX_BATCH_SECTORS_IN_CLIENT) / HBBlockSize;

	/**
	 * RC Update default consuming rate: 16*1000 hbs / 1 secs
	 */
	public static final int RCM_RCLIST_INIT_SIZE 			= 200;			//transactions in a list
	public static final int RCM_DSS_MAX_HBS 				= 16*1000;		//max HBs DSS consume for one update.
	public static final int RCM_DEFAULT_RCLPOOL_SIZE 		= (2000/RCM_RCLIST_INIT_SIZE);	//initial size: assume average IOPS is 2000 transactions
	public static final int RCM_DEFAULT_RCML_SLOT_SIZE 		= 50;
	public static final String RCM_DIR 						= null;//DMSFlags.checkDMSDefaultDir(conf) + File.separator + "RCM";
	public static final int RCM_UPDATE_PERIOD 				= 1;	 		//1 sec
	public static final int RCM_UPDATE_PERIODMS 			= 1*1000;
	public static final int RCM_WAIT_ACK_TIMEOUT 			= 1*60*1000;	//1 mins
	public static final int RCM_MAX_RETRY 					= (RCM_WAIT_ACK_TIMEOUT/RCM_UPDATE_PERIODMS)/2; 		//1 mins
	public static final boolean RCM_ENABLE					= true; //conf.getBoolean("RCManager.enable", true);		//default is ON
	
	
	
}
