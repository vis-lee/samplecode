/*
 * SSockets_Manager.c
 *
 *  Created on: Apr 12, 2012
 *      Author: Vis Lee
 *              Lego Lin
 *
 */

#include "SSocket.h"


/********************************************************************************/
/*																				*/
/*							Global variables 									*/
/*																				*/
/********************************************************************************/

static int DMS_MAX_NODES = 255;
static atomic_t cur_max = ATOMIC_INIT(0);

static struct DMS_Node_Container **dms_nodes;

static rwlock_t dms_nodes_lock = RW_LOCK_UNLOCKED;

//number of occupied flag chunks.
//const int nr_ofchunk = ALIGN(DMS_MAX_NODES, BITS_PER_LONG);

//ulong64 occupied_flags[ nr_ofchunk ];

char *DNCMGR_MOD = "DNCMGR: ";


/********************************************************************************/
/*																				*/
/*								DEFINITIONS										*/
/*																				*/
/********************************************************************************/

#define DNC_IN_RANGE(i) IS_IN_RANGE(i, 0, DMS_MAX_NODES-1)

extern int _inet_pton(char *cp,void *dst);




/********************************************************************************/
/*																				*/
/*								FUNC IMPLEMENT									*/
/*																				*/
/********************************************************************************/



struct DMS_Node_Container * __DNC_Check_Duplicate(struct DMS_Node_Container *dnc){

	int i = 0;
	struct DMS_Node_Container *dup_dnc = NULL;

	for( i = 0; i < DMS_MAX_NODES; i++){

		if( dms_nodes[i] != NULL ){

			if(/*strncmp(&dnc->ip_str, &dms_nodes[i]->ip_str, DMS_IP_STR_LEN) == 0*/
					dnc->ssk.ip == dms_nodes[i]->ssk.ip &&
					dnc->ssk.port == dms_nodes[i]->ssk.port)
			{
				dup_dnc = dms_nodes[i];
				break;
			}
		}
	}

	//Am I duplicate?
	if( dup_dnc ) {

		DMS_PRINTK(DNC_DBG, DNCMGR_MOD, "FATAL ERROR~! found dup_dnc = %p, ip = %s \n",
				dup_dnc, dup_dnc->ip_str);

	}else{

		DMS_PRINTK(DNC_DBG, DNCMGR_MOD, "No duplicate dnc \n");
	}

	return dup_dnc;
}


int __Occupy_One_Index(struct DMS_Node_Container *dnc){

	int index = -1;
	int i = 0;
	struct DMS_Node_Container *dup_dnc = NULL;

	//get write lock
	write_lock(&dms_nodes_lock);

	dup_dnc = __DNC_Check_Duplicate(dnc);

	if(!dup_dnc){

		for( i = 0; i < DMS_MAX_NODES; i++){

			//found
			if( dms_nodes[i] == NULL ){

				dms_nodes[i] = dnc;
				index = i;
				break;
			}
		}
	}

	write_unlock(&dms_nodes_lock);

#if 0
	//Am I within DMS_MAX_NODES range?
	if( DNC_IN_RANGE(index) ) {

		//TODO use this parameter to optimize search time.
		atomic_cmpxchg(&cur_max, old, index);
		DMS_PRINTK(DNC_DBG, DNCMGR_MOD, "the cur_max = %d!! \n", atomic_read(&cur_max));
	}
#endif

	DMS_PRINTK(DNC_DBG, DNCMGR_MOD, "ret index = %d \n", index);

	return index;
}


struct DMS_Node_Container * __Clean_One_Index( int node_index /*struct DMS_Node_Container *dnc*/ ){

	struct DMS_Node_Container *dnc = NULL;

	if( DNC_IN_RANGE(node_index) ){

		//get write lock
		write_lock(&dms_nodes_lock);

		if(IS_LEGAL(DNCMGR_MOD, dms_nodes[node_index])){

			dnc = dms_nodes[node_index];
			dms_nodes[node_index] = NULL;

		}

		write_unlock(&dms_nodes_lock);

	} else {

		DMS_PRINTK(DNC_DBG, DNCMGR_MOD, "WARN: eindex = %d out of range!! \n", node_index);
	}

	DMS_PRINTK(DNC_DBG, DNCMGR_MOD, "ret dnc ptr = %p \n", dnc);

	return dnc;
}


#if 0
int __Occupy_One_Index(){

	int retcode = -DMS_FAIL, index = -1;
	int i = 0;

	//get write lock
	write_lock(&dms_nodes_lock);

	for( i = 0; i < nr_ofchunk; i++){

		//find next 0 bit
		index = find_first_zero_bit( &occupied_flags[i], BITS_PER_LONG );

		//found
		if( index < BITS_PER_LONG ){

			retcode = index + i * BITS_PER_LONG;
			break;
		}
	}

	write_unlock(&dms_nodes_lock);

	//Am I within DMS_MAX_NODES range?
	if( !DNC_IN_RANGE(retcode) ) {

		DMS_PRINTK(DNC_DBG, DNCMGR_MOD, "WARN: eindex = %d out of range!! \n", retcode);
		retcode = -DMS_FAIL;
	}

	DMS_PRINTK(DNC_DBG, DNCMGR_MOD, "ret index = %d \n", retcode);

	return retcode;
}

int __Clean_One_Index(int index){

	if(){

	}
}
#endif


inline struct DMS_Node_Container * __Node_Index_to_DNC(int node_index){

	struct DMS_Node_Container *dnc = NULL;

	//check node index
	if(DNC_IN_RANGE(node_index)){

		//get dnc
		dnc = dms_nodes[node_index];

	} else {

		//TODO build node?
	}

	return dnc;

}

/**
 * Build_DMS_Node_Container
 * @param ip_str	:end point ip
 * @param port		:end point port
 * @param crecv_fn	:create receiver function pointer
 * @param rrecv_fn	:release receiver function pointer
 * @param data		:user data, pass to create and release receiver function.
 * @param nerrep_fn	:network error report function pointer
 * @param nstchg_fn	:network state change function pointer
 *
 * @return	0~DMS_MAX_NODES-1	:node index
 * 			-1					:NULL ptr or improper state
 * 			-1602				:ip format error
 */
int Build_DMS_Node_Container(int node_ip, short port,
		CreateRecv_Fn_t crecv_fn, ReleaseRecv_Fn_t rrecv_fn, void *data,
		NErRep_Fn_t nerrep_fn, NStChg_Fn_t nstchg_fn){

	int retcode = -DMS_FAIL, index = -1;
//	u32 ip_int = 0;
	struct DMS_Node_Container *dnc = NULL;

	//check ip_str correct or not. using _inet_pton, FIXME remove if useless.
//	if( _inet_pton(node_ip, &ip_int) > 0 ){

		//create dnc
		dnc = Create_DMS_Node_Container(node_ip, port, crecv_fn, rrecv_fn, data, nerrep_fn, nstchg_fn);

		if(dnc){

			//insert to DNCS array, and check duplicate~!
			index = __Occupy_One_Index(dnc);

			if( DNC_IN_RANGE(index) ){

				dnc->index = index;

				//schedule to building work queue
				if( Build_Connection(dnc) > 0 ){

					DMS_PRINTK(DNC_DBG, DNCMGR_MOD, "dnc = %p, node_index = %d, schedule building work ok~ \n",
							dnc, index);

					retcode = index;
				}
			}
		}

//	} else {
//
//		eprintk(DNCMGR_MOD, "ip format Error! ip = %s \n", node_ip);
//		retcode = -EDNC_FORMAT;
//	}

	DMS_PRINTK(DNC_DBG, DNCMGR_MOD, "end~ dnc = %p, retcode = %d \n", dnc, retcode);

	return retcode;
}


/**
 * Destroy_DMS_Node_Container
 * @param dnc
 */
void Destroy_DMS_Node_Container(int node_index){

	struct DMS_Node_Container *dnc = NULL;

	//clear ptr
	dnc = __Clean_One_Index(node_index);

	if( dnc ){

		DMS_PRINTK(DNC_DBG, DNCMGR_MOD, "dnc = %p, ip = %s \n", dnc, dnc->ip_str);

		//release dnc
		Release_DMS_Node_Container(dnc);
	}

	DMS_PRINTK(DNC_DBG, DNCMGR_MOD, "end~! dnc = %p \n", dnc);

}

/**
 *
 * @param node_index
 * @param buf
 * @param len
 * @return	>0:		sent length
 * 			-1:		NULL ptr
 * 			-1601:	connection error
 */
int Send_Msg(int node_index, char *buf, int len, int msg_flags){

	int retcode = -DMS_FAIL;

	//get dnc
	struct DMS_Node_Container *dnc = __Node_Index_to_DNC(node_index);

	//check dnc status
	if(IS_LEGAL(DNCMGR_MOD, dnc) ) {

		if(atomic_read(&dnc->status)  == DNODE_CONNECTED){

			//xmit
			retcode = sock_xmit(dnc->sock, true, buf, len, msg_flags);

		}else{
			//connection error
			retcode = -EDNC_CONNECT;
		}


	}

	return retcode;

}

/**
 * Recv_Msg : general recv function
 * @param node_index
 * @param buf
 * @param len
 * @return	>0:		recv length
 * 			-1:		NULL ptr
 * 			-32:	-EPIPE. pipe broken, or someone stop me.
 * 			-1601:	connection error
 */
int Recv_Msg(int node_index, char *buf, int len, int msg_flags){

	int retcode = -DMS_FAIL;

	//get dnc
	struct DMS_Node_Container *dnc = __Node_Index_to_DNC(node_index);

	//check dnc status
	if(IS_LEGAL(DNCMGR_MOD, dnc)) {

		if( atomic_read(&dnc->status) == DNODE_CONNECTED ){

			//xmit
			retcode = sock_xmit(dnc->sock, false, buf, len, msg_flags);

		} else {

			//connection error
			retcode = -EDNC_CONNECT;
		}

	}

	return retcode;

}



inline char * Get_IP_Str(int node_index){

	//get dnc
	struct DMS_Node_Container *dnc = __Node_Index_to_DNC(node_index);

	//check dnc status
	if(IS_LEGAL(DNCMGR_MOD, dnc)) {

		return dnc->ip_str;

	}

	return NULL;

}


inline int Get_IP_Int(int node_index){

	//get dnc
	struct DMS_Node_Container *dnc = __Node_Index_to_DNC(node_index);

	//check dnc status
	if(IS_LEGAL(DNCMGR_MOD, dnc)) {

		return dnc->ssk.ip;

	}

	return -1;

}


inline short Get_Port(int node_index){

	//get dnc
	struct DMS_Node_Container *dnc = __Node_Index_to_DNC(node_index);

	//check dnc status
	if(IS_LEGAL(DNCMGR_MOD, dnc)) {

		return dnc->ssk.port;

	}

	return -1;
}


int Get_Node_Index_by_IP_Port(int ip, short port){

	int retcode = -1;
	int i = 0;

	//get write lock
	write_lock(&dms_nodes_lock);

	for( i = 0; i < DMS_MAX_NODES; i++){

		if( dms_nodes[i] != NULL){

			if(dms_nodes[i]->ssk.ip == ip && dms_nodes[i]->ssk.port == port){
				retcode = i;
			}
		}
	}

	write_unlock(&dms_nodes_lock);


	return retcode;
}

int Get_Node_Index_by_SSocket(struct SSocket *ssk){

	int retcode = -1;
	int i = 0;

	//get write lock
	write_lock(&dms_nodes_lock);

	for( i = 0; i < DMS_MAX_NODES; i++){

		if( dms_nodes[i] != NULL){

			if(dms_nodes[i]->ssk.ip == ssk->ip && dms_nodes[i]->ssk.port == ssk->port){
				retcode = i;
			}
		}
	}

	write_unlock(&dms_nodes_lock);

	return retcode;
}


int Get_Size_of_DNCS(void){

	int retcode = 0;
	int i = 0;

	//get write lock
	write_lock(&dms_nodes_lock);

	for( i = 0; i < DMS_MAX_NODES; i++){

		if( dms_nodes[i] != NULL){

			retcode++;
		}
	}

	write_unlock(&dms_nodes_lock);


	return retcode;
}


int SPrint_DNCS_Manager(char *buf, int len_limit){

	int i = 0, len = 0;

	if( IS_LEGAL(DNCMGR_MOD, dms_nodes) )
	{

		if(len_limit > 0){

			len += sprintf(buf+len, "\t%s, all dncs = { \n", DNCMGR_MOD);

			for( i = 0; i < DMS_MAX_NODES; i++ ){

				if( (dms_nodes[i] != NULL) && (len_limit-len > 0) ){

					//print DNCs
					len += SPrint_DMS_Node_Containers(buf+len, len_limit-len, dms_nodes[i]);
				}
			}

			len += sprintf(buf+len, "\t} \n");
		}

	}

	return len;

}


int Init_DNCS_Manager(void){

	int retcode = -DMS_FAIL;

	rwlock_init(&dms_nodes_lock);

	//get write lock
	write_lock(&dms_nodes_lock);

	//init dms_nodes
	dms_nodes = (struct DMS_Node_Container **) kzalloc (sizeof(struct DMS_Node_Container *) * DMS_MAX_NODES, GFP_KERNEL);

	//memset(&occupied_flags, 0, sizeof(occupied_flags));

	write_unlock(&dms_nodes_lock);

	//init work queue
	retcode = Init_Node_Builder();

	DMS_PRINTK(DNC_DBG, DNCMGR_MOD, "done! size of dms_nodes = %d, retcode = %d \n", DMS_MAX_NODES, retcode);

	return retcode;
}


void Release_DNCS_Manager(void){

	int i = 0;

	//get write lock
	write_lock(&dms_nodes_lock);

	for( i = 0; i < DMS_MAX_NODES; i++){

		if( dms_nodes[i] != NULL){

			//release dnc
			Release_DMS_Node_Container(dms_nodes[i]);
			//dms_nodes[i] = NULL;
		}
	}

	write_unlock(&dms_nodes_lock);

	Release_Node_Builder();

	DMS_PRINTK(DNC_DBG, DNCMGR_MOD, "done! \n");
}









#ifdef DMS_UTEST
EXPORT_SYMBOL(Init_DNCS_Manager);
EXPORT_SYMBOL(Release_DNCS_Manager);
EXPORT_SYMBOL(Build_DMS_Node_Container);
EXPORT_SYMBOL(Destroy_DMS_Node_Container);
EXPORT_SYMBOL(Get_Size_of_DNCS);
EXPORT_SYMBOL(SPrint_DNCS_Manager);
EXPORT_SYMBOL(Send_Msg);
EXPORT_SYMBOL(Recv_Msg);
EXPORT_SYMBOL(__Node_Index_to_DNC);

void Reset_DNCS_Manager(void){

	int i = 0;

	//needn't get write lock
	for( i = 0; i < DMS_MAX_NODES; i++){

		if( dms_nodes[i] != NULL){

			//release dnc
			Destroy_DMS_Node_Container(i);

		}
	}


	DMS_PRINTK(DNC_DBG, DNCMGR_MOD, "done! \n");
}
EXPORT_SYMBOL(Reset_DNCS_Manager);

#endif






