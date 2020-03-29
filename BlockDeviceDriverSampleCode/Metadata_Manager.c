/*
 * Metadata_Manager.c
 *
 *  Created on: Apr 9, 2012
 *      Author: Vis Lee
 *              Lego Lin
 *
 */
#include <linux/tcp.h>
#include <linux/socket.h>
#include <linux/kthread.h>
#include <net/sock.h>

#include "DIO.h"
#include "Namenode_Protocol.h"
#include "Metadata_Manager.h"
#include "SSockets_Manager.h"

/********************************************************************************/
/*																				*/
/*							Global variables 									*/
/*																				*/
/********************************************************************************/

char *METAMGR_MOD = "METAMGR: ";

static int nn_node_index = 0;



/********************************************************************************/
/*																				*/
/*								DEFINITIONS										*/
/*																				*/
/********************************************************************************/





/********************************************************************************/
/*																				*/
/*								Metadata Handler								*/
/*																				*/
/********************************************************************************/

int Generate_Query_Request_of_Namenode(struct DMS_IO *dio, struct DMS_Protocol_Header *header, union NN_Protocol_Body *body){

	int retcode = DMS_OK;
//	int service_type = 0;

	if(IS_LEGAL(METAMGR_MOD, dio) &&
			IS_LEGAL(METAMGR_MOD, dio->volume) &&
			IS_LEGAL(METAMGR_MOD, header) &&
			IS_LEGAL(METAMGR_MOD, body) )
	{

		//generate body
		retcode = Generate_NN_Protocol_Body(body, dio->volume->volumeID, dio->sLBID, dio->nr_LBIDs);

		if(retcode < 0)
			goto GEN_QUERY_FAIL;

		//generate head
		retcode = Generate_NN_Protocol_Header(header, dio->retry_count++, dio->op, retcode, dio);

		if(retcode < 0)
			goto GEN_QUERY_FAIL;


	} else {

		retcode = -DMS_FAIL;
	}

GEN_QUERY_FAIL:

	DMS_PRINTK(META_DBG, METAMGR_MOD, "end~!, retcode = %d \n", retcode);

	return retcode;

}

int Send_Query_Request_to_Namenode(struct DMS_IO *dio){

	int retcode = DMS_OK;
	struct DMS_Protocol_Header header;
	union NN_Protocol_Body body;

	if(IS_LEGAL(METAMGR_MOD, dio) &&
			IS_LEGAL(METAMGR_MOD, dio->volume) )
	{

		//generate Query Request
		retcode = Generate_Query_Request_of_Namenode(dio, &header, &body);

		if(retcode < 0){
			goto SEND_FOUT;
		}

		//TODO ref FSM time stamp, ie, fsm.timeout - 1,
		//set waiting time
		//dio->dmeta->wait_time = GET_WAIT_TIME(DEFAULT_NN_REQ_WAIT_TIME);
		/*	struct timeval dtv;
			do_gettimeofday(&dtv);
			*/

		//TODO I think it should be a lock here for ordering the concurrency thread.
		//socket send head
		retcode = Send_Msg(nn_node_index, (char *)&header, sizeof(struct DMS_Protocol_Header), MSG_MORE);

		if(retcode < 0){
			goto SEND_FOUT;
		}

		//socket send body
		retcode = Send_Msg(nn_node_index, (char *)&body, header.body_length, 0);

		if(retcode < 0){
			goto SEND_FOUT;
		}


SEND_FOUT:
		DMS_PRINTK(META_DBG, METAMGR_MOD, "did = %llu, serviceID = %llu, body_len = %d, retcode = %d \n",
					dio->did, header.serviceID, header.body_length, retcode);

	} else {

		retcode = -DMS_FAIL;
	}


	return retcode;

}




/* re-entrance function */
int Metadata_Handler(struct DMS_IO *dio){

	int retcode = DMS_OK;
	struct DMS_Metadata *dmeta = NULL;

	if(IS_LEGAL(METAMGR_MOD, dio)){


		//TODO check cache

		//cache miss, go to namenode
		retcode = Send_Query_Request_to_Namenode(dio);


	} else {

		retcode = -DMS_FAIL;
	}

OUT:

	return retcode;

}


/********************************************************************************/
/*																				*/
/*								Commit to namenode								*/
/*																				*/
/********************************************************************************/


int Commit_to_Namenode(struct DMS_Metadata *dmeta){

	int retcode = -DMS_FAIL;

	int i = 0;
	ulong64 slbid = 0;

	if( IS_LEGAL(METAMGR_MOD, dmeta) )
	{
		//complete, commit to DMS_Metadata, if haven't commit
		//TODO if(!test_and_set_bit(COMMITTED_NN_BIT, &lr->commit_state)){

			//TODO commit to NN

			//TODO compare replica factor with nr_dn_locs, if less than report to namenode

			//TODO release DMS_Metadata
		//}

	}

	return retcode;
}



/********************************************************************************/
/*																				*/
/*								Namenode Receiver								*/
/*																				*/
/********************************************************************************/

int NN_Req_Sanity_Check(struct DMS_Protocol_Header *header){

	int retcode = -DMS_FAIL;

	if( IS_LEGAL(METAMGR_MOD, header) &&
			IS_LEGAL(METAMGR_MOD, header->extention) )
	{
		struct DMS_IO *dio = (struct DMS_IO *)header->extention;;

		struct DMS_Metadata *dmeta = dio->dmeta;

		if( IS_LEGAL(METAMGR_MOD, dmeta) )
		{

			//lock dio
			write_lock(&dio->lock);

			//TODO check dio state(in send_nn?), time stamp(within available time), did and magic_num of dio

			//TODO check timestamp

			//TODO check dio existence

			//TODO cancel fsm timer

			//TODO set fsm state

			//			DMS_PRINTK(META_DBG, METAMGR_MOD, "did = %d, serviceID = %d \n",
			//								dio->did, header->serviceID);


			//unlock dio
			write_unlock(&dio->lock);

		}
	}


	return retcode;

}



int __Grow_up_Buffer(char **old, int size){

	char *new = NULL;
	int buf_size = 0;

	if( IS_LEGAL(METAMGR_MOD, old) ){

		buf_size = size + PAGE_SIZE;

		new = (char *)kmalloc(buf_size, GFP_KERNEL);

		if( CHECK_PTR(METAMGR_MOD, new) ){

			//free old one.
			kfree(*old);

			*old = new;

		} else {

			buf_size = size;
		}

	}

	wprintk(METAMGR_MOD, "!!WARN: old_size = %d, new_size = %d \n",
				size, buf_size);

	return buf_size;

}


int Recv_Query_Response_from_Namenode(struct DMS_Protocol_Header *header, union NN_Protocol_Body *body, char **buf_ptr, int buf_size){

	int retcode = -DMS_FAIL;

	if(IS_LEGAL(METAMGR_MOD, header) &&
			IS_LEGAL(METAMGR_MOD, body) &&
			IS_LEGAL(METAMGR_MOD, buf_ptr) )
	{

		//receive header from socket
		retcode = Recv_Msg(nn_node_index, (char *)header, sizeof(struct DMS_Protocol_Header), MSG_WAITALL);

		//TODO set state of Metadata Manager? do some error handling?
		if(retcode < 0){

			goto OUT;
		}

		//parse header
		retcode = Parse_NN_Protocol_Header(header);

		if(retcode < 0){

			goto OUT;
		}

		if(header->body_length > buf_size){

			//grow up the buffer
			buf_size = __Grow_up_Buffer(buf_ptr, buf_size);

		}

		//receive body from socket
		retcode = Recv_Msg(nn_node_index, *buf_ptr, header->body_length, 0);

		if(retcode < 0){

			goto OUT;
		}

		//parse body protocol
		retcode = Parse_NN_Protocol_Body(header, *buf_ptr, body);

	}

OUT:

	return retcode;
}


int Process_Namenode_Response(struct DMS_Protocol_Header *header, union NN_Protocol_Body *body){

	int retcode = -DMS_FAIL;

	if( IS_LEGAL(METAMGR_MOD, header) &&
			IS_LEGAL(METAMGR_MOD, body) )
	{
		//get dio
		struct DMS_IO *dio = (struct DMS_IO *)header->extention;

		struct DMS_Metadata *dmeta = NULL;

		if(IS_LEGAL(METAMGR_MOD, dio)){


			//TODO check dio state(in send_nn?), time stamp(within available time), did and magic_num of dio
			//Check_Response_Availability( ); / Sanity_Check();

//			DMS_PRINTK(META_DBG, METAMGR_MOD, "did = %d, serviceID = %d \n",
//								dio->did, header->serviceID);
			NN_Req_Sanity_Check(header);


			dmeta = Create_DMS_Metadata(dio);

			if(!CHECK_PTR(METAMGR_MOD, dmeta)){
				goto OUT;
			}


			dmeta->nr_lrs = body->qmeta_res.nr_lrs;
			dmeta->lrs = body->qmeta_res.lrs;
			dmeta->commitID = body->qmeta_res.commit_ID;

			atomic_set(&dmeta->nr_waiting_lrs, body->qmeta_res.nr_lrs);

			//lock dio
			write_lock(&dio->lock);

			dio->dmeta = dmeta;

			//TODO reset retry count
			//INIT_DIO_RETRY_COUNT(&dio->retry_count);

			//TODO gen dn_protocol by nnp-body


			//unlock dio
			write_unlock(&dio->lock);

			retcode = DMS_OK;
		}

	}


OUT:
	return retcode;

}


/* !! N.B. the size of body buffer is times of 4KB */
int Namenode_Receiver(void *data){

	int retcode = 0;

	struct DMS_Protocol_Header *header = NULL;
	union NN_Protocol_Body *body = NULL;
	char * buf = NULL;
	int buf_size = PAGE_SIZE;

	header = (struct DMS_Protocol_Header *)kmalloc(sizeof(struct DMS_Protocol_Header), GFP_KERNEL);
	body = (union NN_Protocol_Body *)kmalloc(sizeof(union NN_Protocol_Body), GFP_KERNEL);
	buf = (char *)kmalloc(buf_size, GFP_KERNEL);


	while( !kthread_should_stop() /*retcode >= 0*/ ){

		//reset, FIXME remove after stable
		memset((char *)body, 0, sizeof(union NN_Protocol_Body));
		memset(buf, 0, buf_size);

		retcode = Recv_Query_Response_from_Namenode(header, body, &buf, buf_size);

		if(retcode > 0){

			retcode = Process_Namenode_Response(header, body);

		}else{

			//TODO go forward and find next magicNumber.
			continue;
		}

	}

	kfree(buf);
	kfree(body);
	kfree(header);

	DMS_PRINTK(META_DBG, METAMGR_MOD, " end, retcode = %d, msg = %s \n",
			retcode, __errorntostr(retcode));

	return retcode;

}

struct task_struct * Create_Namenode_Receiver(int node_index, void *data){

	struct task_struct *receiver = NULL;

	receiver = kthread_create(Namenode_Receiver, (void *)node_index, "nn_receiver");

	return receiver;
}

void Stop_Namenode_Receiver(void *data){

	struct task_struct *receiver = (struct task_struct *)data;

	if(pid_alive(receiver)){

		DMS_PRINTK(META_DBG, METAMGR_MOD, "stopping %s \n", (char *)&receiver->comm);

		kthread_stop(receiver);

		DMS_PRINTK(META_DBG, METAMGR_MOD, "stopped %s \n", (char *)&receiver->comm);

	}else{

		DMS_PRINTK(META_DBG, METAMGR_MOD, "thread has dead... dn_receiver = %p \n", receiver);
	}
}



static void NN_Socket_State_Change(struct sock *sk)
{

	switch (sk->sk_state) {

		case TCP_ESTABLISHED:
			iprintk(METAMGR_MOD, "TCP_ESTABLISHED, Set Driver to \"SERVICING\"\n");
			//TODO set driver to "servicing"
			break;

		default:
			break;
	}


}


static void NN_Socket_Error_Report(struct sock *sk)
{
	//TODO set driver to "PENDING"
}



int Init_Metadata_Manager(int nn_ip, short port){

	int retcode = DMS_OK;

	char *ip_c = (char *)&nn_ip;

//	if(IS_LEGAL(METAMGR_MOD, nn_ip)){

		DMS_PRINTK(META_DBG, METAMGR_MOD, "namenode_ip = %d.%d.%d.%d:%d \n",
				ip_c[3], ip_c[2], ip_c[1], ip_c[0], port);

		//build namenode connection
		nn_node_index = Build_DMS_Node_Container(nn_ip,
												 port,
												 Create_Namenode_Receiver,
												 Stop_Namenode_Receiver,
												 NULL,
												 NN_Socket_Error_Report,
												 NN_Socket_State_Change );

//	}else{
//
//		retcode = -DMS_FAIL;
//	}

	return retcode;
}


void Release_Metadata_Manager(void){

	//dis-connect
	Destroy_DMS_Node_Container(nn_node_index);

}

