/*
 * Namenode_Protocol.c
 *
 *  Created on: May 3, 2012
 *      Author: Vis Lee
 *              Lego Lin
 *
 */

#include "Namenode_Protocol.h"
#include "Metadata.h"


/********************************************************************************/
/*																				*/
/*							Global variables 									*/
/*																				*/
/********************************************************************************/

char *NNP_MOD = "NNP: ";

static atomic64_t nnph_id_gen = ATOMIC64_INIT(1);

/********************************************************************************/
/*																				*/
/*								DEFINITIONS										*/
/*																				*/
/********************************************************************************/





/********************************************************************************/
/*																				*/
/*							FUNCS Implementations								*/
/*																				*/
/********************************************************************************/

char * __NN_Type_ntoc(int ttype, int type) {

	switch(ttype){

		case TSENDER:
		{
//			switch(type){
//
//				/*
//				 * DMS_Sender_Type
//				 */
//				case CLIENTNODE:
//					return "CLIENTNODE";
//				case DATANODE:
//					return "DATANODE";
//				case NAMENODE:
//					return "NAMENODE";
//
//				default:
//					return "unknow service type";
//			}
//
//			break;

			return __TSender_ntoc(type);
		}

		case TSERVICE:
		{
			switch(type){

				/*
				 * DMS_Service_Type
				 */
				case QUERY_FOR_READ:
					return "QUERY_FOR_READ";
				case QUERY_FOR_WRITE:
					return "QUERY_FOR_WRITE";
				case QUERY_FOR_OVERWRITE:
					return "QUERY_FOR_OVERWRITE";
				case COMMIT_ALLOCATION:
					return "COMMIT_ALLOCATION";
				case REPORT_READ_FAILURE:
					return "REPORT_READ_FAILURE";
				case COMMIT_METADATA:
					return "COMMIT_METADATA";
				case QUERY_VOLUME_INFO:
					return "QUERY_VOLUME_CAPACITY";
				case CREATE_VOLUME:
					return "CREATE_VOLUME";

				default:
					return "unknow service type";
			}

			break;
		}

		case TSUB:
		{
			switch(type){

				/*
				 * DMS_Sub_Type
				 */
				case UNDEFINED:
					return "UNDEFINED";
				case TIMEOUT:
					return "TIMEOUT";
				case EXCEPTION:
					return "EXCEPTION";

				case SERVICE:
					return "SERVICE";
				case RESPONSE:
					return "RESPONSE";

				default:
					return "unknow sub type";

			}

			break;
		}

		default:
			return "unknow dms type";
	}


};


int Parse_Datanode_Locations(struct Located_Request *lr, char *buf){

	int i = 0, j = 0, buf_offset = 0;
	struct Datanode_Location **dn_locs = NULL;
	struct Datanode_Location *dn_loc = NULL;

	if( IS_LEGAL(NNP_MOD, lr) &&
			IS_LEGAL(NNP_MOD, buf) )
	{

		//create dn_locations first
		dn_locs = Create_Datanode_Locations(lr->nr_dn_locs);

		write_lock(&lr->lock);

		lr->dn_locs = dn_locs;

		/* dnLocations[i]: */
		for(i = 0; i < lr->nr_dn_locs; i++){

			dn_loc = lr->dn_locs[i];

			if( CHECK_PTR(NNP_MOD, dn_loc) ){

				//datanode ip
				dn_loc->ssk.ip = READ_NET_INT(buf, buf_offset);

				//datanode port
				dn_loc->ssk.port = (int)READ_NET_SHORT(buf, buf_offset);

				//nr_physical_locations
				dn_loc->nr_triplets = READ_NET_INT(buf, buf_offset);

				/* parse physical locations */
				for(j = 0; j < dn_loc->nr_triplets; j++){

					//triplets
					dn_loc->triplets[j] = READ_NET_LONG(buf, buf_offset);
				}

			}

		}

		write_unlock(&lr->lock);

	}


	DMS_PRINTK(META_DBG, NNP_MOD, "end~!, retcode = %d \n", buf_offset);

	return buf_offset;

}


int Parse_Located_Request(struct Located_Request *lr, char *buf, ulong64 slbid){

	int retcode = -DMS_FAIL;
	int i = 0, buf_offset = 0;

	if( IS_LEGAL(NNP_MOD, lr) &&
			IS_LEGAL(NNP_MOD, buf) )
	{

		write_lock(&lr->lock);

		lr->slbid = slbid;

		//located request state
		lr->lr_state = READ_NET_SHORT(buf, buf_offset);

		//nr_HBIDs: short
		lr->nr_hbids = READ_NET_SHORT(buf, buf_offset);
		lr->nr_lbids = lr->nr_hbids;

		for(i = 0; i < lr->nr_hbids; i++){

			//HBID[nr_HBIDs]: ulong array
			lr->HBIDs[i] = READ_NET_LONG(buf, buf_offset);
		}

		//nr_dn_locations: short
		lr->nr_dn_locs = READ_NET_SHORT(buf, buf_offset);

		write_unlock(&lr->lock);

		//parse datanode locations
		retcode = Parse_Datanode_Locations(lr, buf+buf_offset);

		if(retcode > 0){

			//sum buffer offset
			retcode += buf_offset;

		}

	}

	DMS_PRINTK(META_DBG, NNP_MOD, "end~!, retcode = %d \n", retcode);

	return retcode;

}


int Parse_Located_Requests(struct Located_Request **lrs, int nr_lrs, ulong64 slbid, char *buf){

	int retcode = -DMS_FAIL;
	int i = 0, buf_offset = 0;

	short lbid_offset = 0;

	if( IS_LEGAL(NNP_MOD, lrs) &&
			IS_LEGAL(NNP_MOD, buf) )
	{

		for(i = 0; i < nr_lrs; i++){

			if( IS_LEGAL(NNP_MOD, lrs[i]) ){

				retcode = Parse_Located_Request(lrs[i], buf+buf_offset, slbid+lbid_offset);

				if(retcode > 0){

					//forward buf offset
					buf_offset += retcode;

					//forward lbid offset
					lbid_offset += lrs[i]->nr_lbids;

				} else {

					goto PARSE_OUT;
				}

			}else{

				retcode = -DMS_FAIL;
				goto PARSE_OUT;
			}
		}

		retcode = buf_offset;

	}


PARSE_OUT:

	DMS_PRINTK(META_DBG, NNP_MOD, "end~! retcode = %d \n", retcode);

	return retcode;

}


int Parse_Metadata_Response(struct DMS_Protocol_Header *header, char *buf, union NN_Protocol_Body *body){

	int retcode = -DMS_FAIL;
	int buf_offset = 0;

	if( IS_LEGAL(NNP_MOD, header) &&
			IS_LEGAL(NNP_MOD, buf) &&
			IS_LEGAL(NNP_MOD, body) )
	{
		/* Request class */
		//volumeID: long
		body->qmeta_res.volumeID = READ_NET_LONG(buf, buf_offset);

		//start_LBID: ulong
		body->qmeta_res.start_LBID = READ_NET_LONG(buf, buf_offset);

		//numOfLBs: short
		body->qmeta_res.nr_LBIDs = READ_NET_SHORT(buf, buf_offset);

		//commit_ID: ulong
		body->qmeta_res.commit_ID = READ_NET_LONG(buf, buf_offset);

		//nr_lrs: short
		body->qmeta_res.nr_lrs = READ_NET_SHORT(buf, buf_offset);

		body->qmeta_res.lrs = Create_Located_Requests(body->qmeta_res.nr_lrs);

		if(body->qmeta_res.lrs){

			retcode = Parse_Located_Requests(body->qmeta_res.lrs,
												body->qmeta_res.nr_lrs,
												body->qmeta_res.start_LBID,
												buf+buf_offset);
		}

		if(retcode > 0){
			retcode += buf_offset;
		}

	}

	return retcode;

}


int Parse_NN_Protocol_Body(struct DMS_Protocol_Header *header, char *buf, union NN_Protocol_Body *body){

	int retcode = -DMS_FAIL;


	if( IS_LEGAL(NNP_MOD, header) &&
			IS_LEGAL(NNP_MOD, buf) &&
			IS_LEGAL(NNP_MOD, body) )
	{

		if( header->sub_type != RESPONSE ){

			switch(header->service_type){

				case QUERY_FOR_READ:

				case QUERY_FOR_WRITE:
				case QUERY_FOR_OVERWRITE:

					retcode = Parse_Metadata_Response(header, buf, body);

					if(retcode < 0){

						DSTR_PRINT(ALWAYS, NNP_MOD,
								SPrint_Located_Requests(DSTR_NAME, DSTR_LIMIT, body->qmeta_res.lrs, body->qmeta_res.nr_lrs),"");
					}

					break;

				case COMMIT_ALLOCATION:
				case REPORT_READ_FAILURE:
				case COMMIT_METADATA:

					break;

				default:
					eprintk(NNP_MOD, "unknow service type = %d, msg = %s \n", header->service_type,
							__NN_Type_ntoc(TSERVICE, header->service_type));

			}


		} else {

			DMS_PRINTK(META_DBG, NNP_MOD, "FATAL ERROR~! should be RESPONSE, but subType = %s \n",
					__NN_Type_ntoc(TSUB, header->sub_type));
		}

	}

	DMS_PRINTK(META_DBG, NNP_MOD, "retcode = %d \n", retcode);

	return retcode;

}




/********************************************************************************/
/*																				*/
/*								Generate Protocol								*/
/*																				*/
/********************************************************************************/

int Pack_Datanode_Locations(struct Located_Request *lr, char *buf){

	int i = 0, j = 0, buf_offset = 0;
	//struct Datanode_Location **dn_locs = NULL;
	struct Datanode_Location *dn_loc = NULL;

	if( IS_LEGAL(NNP_MOD, lr) &&
			IS_LEGAL(NNP_MOD, buf) )
	{

		short nr_dn_locs = 0, nr_triplets = 0;
		int node_ip = 0;
		short port = 0;
		long64 triplet = 0;


		nr_dn_locs = lr->nr_dn_locs;
		WRITE_NET_SHORT(nr_dn_locs, buf, buf_offset);

		write_lock(&lr->lock);

		/* dnLocations[i]: */
		for(i = 0; i < lr->nr_dn_locs; i++){

			dn_loc = lr->dn_locs[i];

			if( CHECK_PTR(NNP_MOD, dn_loc) ){

				//datanode ip
				node_ip = dn_loc->ssk.ip;
				WRITE_NET_INT(node_ip, buf, buf_offset);

				//datanode port
				port = (short)dn_loc->ssk.port;
				WRITE_NET_SHORT(port, buf, buf_offset);

				//nr_physical_locations
				nr_triplets = dn_loc->nr_triplets;
				WRITE_NET_INT(nr_triplets, buf, buf_offset);

				/* Pack physical locations */
				for(j = 0; j < dn_loc->nr_triplets; j++){

					//triplets
					triplet = dn_loc->triplets[j];
					WRITE_NET_LONG(triplet, buf, buf_offset);
				}

			}

		}

		write_unlock(&lr->lock);

	}


	DMS_PRINTK(META_DBG, NNP_MOD, "end~!, retcode = %d \n", buf_offset);

	return buf_offset;

}


int Pack_Located_Request(struct Located_Request *lr, char *buf, ulong64 slbid){

	int retcode = -DMS_FAIL;
	int i = 0, buf_offset = 0;

	if( IS_LEGAL(NNP_MOD, lr) &&
			IS_LEGAL(NNP_MOD, buf) )
	{

		short lr_state = 0, nr_hbids = 0, nr_dn_locs = 0;
		ulong64 HBID = 0;

		write_lock(&lr->lock);

		//located request state
		lr_state = lr->lr_state;
		WRITE_NET_SHORT(lr_state, buf, buf_offset);

		//nr_HBIDs: short
		nr_hbids = lr->nr_hbids;
		WRITE_NET_SHORT(nr_hbids, buf, buf_offset);

		for(i = 0; i < lr->nr_hbids; i++){

			//HBID[nr_HBIDs]: ulong array
			HBID = lr->HBIDs[i];
			WRITE_NET_LONG(HBID, buf, buf_offset);
		}

		//nr_dn_locations: short
		nr_dn_locs = lr->nr_dn_locs;
		WRITE_NET_SHORT(nr_dn_locs, buf, buf_offset);

		write_unlock(&lr->lock);

		//Pack datanode locations
		retcode = Pack_Datanode_Locations(lr, buf+buf_offset);

		if(retcode > 0){

			//sum buffer offset
			retcode += buf_offset;

		}

	}

	DMS_PRINTK(META_DBG, NNP_MOD, "end~!, retcode = %d \n", retcode);

	return retcode;

}


int Pack_Located_Requests(struct Located_Request **lrs, int nr_lrs, ulong64 slbid, char *buf){

	int retcode = -DMS_FAIL;
	int i = 0, buf_offset = 0;

	short lbid_offset = 0;

	if( IS_LEGAL(NNP_MOD, lrs) &&
			IS_LEGAL(NNP_MOD, buf) )
	{

		for(i = 0; i < nr_lrs; i++){

			if( IS_LEGAL(NNP_MOD, lrs[i]) ){

				retcode = Pack_Located_Request(lrs[i], buf+buf_offset, slbid+lbid_offset);

				if(retcode > 0){

					//forward buf offset
					buf_offset += retcode;

					//forward lbid offset
					lbid_offset += lrs[i]->nr_lbids;

				} else {

					goto PACK_LR_OUT;
				}

			}else{

				retcode = -DMS_FAIL;
				goto PACK_LR_OUT;
			}
		}

		retcode = buf_offset;

	}


PACK_LR_OUT:

	DMS_PRINTK(META_DBG, NNP_MOD, "end~! retcode = %d \n", retcode);

	return retcode;

}


int Pack_Metadata_Response(struct DMS_Protocol_Header *header, char *buf, union NN_Protocol_Body *body){

	int retcode = -DMS_FAIL;
	int buf_offset = 0;

	if( IS_LEGAL(NNP_MOD, header) &&
			IS_LEGAL(NNP_MOD, buf) &&
			IS_LEGAL(NNP_MOD, body) )
	{
		ulong64 volumeID = 0, start_LBID = 0, commit_ID = -1;
		short nr_LBIDs = 0, nr_lrs = 0;

		/* Request class */
		//volumeID: long
		volumeID = body->qmeta_res.volumeID;
		WRITE_NET_LONG(volumeID, buf, buf_offset);

		//start_LBID: ulong
		start_LBID = body->qmeta_res.start_LBID;
		WRITE_NET_LONG(start_LBID, buf, buf_offset);

		//numOfLBs: short
		nr_LBIDs = body->qmeta_res.nr_LBIDs;
		WRITE_NET_SHORT(nr_LBIDs, buf, buf_offset);

		//commit_ID: ulong
		commit_ID = body->qmeta_res.commit_ID;
		WRITE_NET_LONG(commit_ID, buf, buf_offset);

		//nr_lrs: short
		nr_lrs = body->qmeta_res.nr_lrs;
		WRITE_NET_SHORT(nr_lrs, buf, buf_offset);

		if(body->qmeta_res.lrs){

			retcode = Pack_Located_Requests(body->qmeta_res.lrs,
												body->qmeta_res.nr_lrs,
												body->qmeta_res.start_LBID,
												buf+buf_offset);
		}

		if(retcode > 0){
			retcode += buf_offset;
		}

	}

	return retcode;

}



/**
 * Generate_NN_Protocol_Body
 *
 * @param body
 * @param volumeID
 * @param startLBID
 * @param nr_LBIDs
 * @return	0 :		OK
 * 			-1:		NULL ptr
 */
inline int Generate_NN_Protocol_Body(union NN_Protocol_Body *body, long64 volumeID, ulong64 start_LBID, ulong64 nr_LBIDs){

	int retcode = -DMS_FAIL;

	if( IS_LEGAL(NNP_MOD, body)){

		memset(body, 0, sizeof(union NN_Protocol_Body));

		body->qmeta_req.volumeID = htonll(volumeID);
		body->qmeta_req.start_LBID = htonll(start_LBID);
		body->qmeta_req.nr_LBIDs = htons((short)nr_LBIDs);

		retcode = sizeof(struct Query_Metadata_Request);
	}

	return retcode;
}







int Parse_NN_Protocol_Header(struct DMS_Protocol_Header *header){

	int retcode = -DMS_FAIL;

	if( IS_LEGAL(NNP_MOD, header)){

		retcode = Parse_DMS_Protocol_Header(header);

		if( header->magicNumber != MAGIC_NUM ){

			retcode = -DMS_FAIL;
			DMS_PRINTK(META_DBG, NNP_MOD, "FATAL ERROR~! magic_number miss-match = %x \n", header->magicNumber);

		}
	}

	DMS_PRINTK(META_DBG, NNP_MOD, "end~!, retcode = %d \n", retcode);

	return retcode;

}




/**
 * Generate_NN_Protocol_Header
 *
 * @param header
 * @param version
 * @param service_type
 * @param length
 * @return	>0 :	OK, the size of header
 * 			-1:		NULL ptr
 */
int Generate_NN_Protocol_Header(struct DMS_Protocol_Header *header, char retry,
									short service_type, int body_length, ulong64 pdata){

	int retcode = -DMS_FAIL;

	if( IS_LEGAL(NNP_MOD, header)){

		retcode = Generate_DMS_Protocol_Header(header, retry, service_type, SERVICE, body_length, pdata, atomic64_inc_return(&nnph_id_gen));

	}

	DMS_PRINTK(META_DBG, NNP_MOD, "end~!, retcode = %d \n", retcode);

	return retcode;
}




















