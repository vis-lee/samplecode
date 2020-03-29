/*
 * SSocket.c
 *
 *  Created on: Apr 12, 2012
 *      Author: Vis Lee
 *              Lego Lin
 *
 */

#include <linux/socket.h> /* socket */
#include <linux/tcp.h>
#include <net/sock.h>
#include <linux/ctype.h>
#include <linux/in.h>
#include <linux/workqueue.h>
#include <linux/kallsyms.h>
#include <linux/kthread.h>

#include "DMS_Common.h"
#include "SSocket.h"


/********************************************************************************/
/*																				*/
/*							Global variables 									*/
/*																				*/
/********************************************************************************/

static unsigned long long CONNECTION_RETRY_INTERVAL_MAX_HZ = 0;



//create retry work queue
struct workqueue_struct * node_builder_workq = NULL;

char *DNC_MOD = "DNC: ";

static const int ONE_DAY_SECS = 60*60*24;


/********************************************************************************/
/*																				*/
/*								DEFINITIONS										*/
/*																				*/
/********************************************************************************/

//#define ONE_DAY_SECS					60*60*24
#define CONNECTION_RETRY_INTERVAL		5	//sec
#define CONNECTION_RETRY_INTERVAL_MAX	20	//sec
#define CONNECTION_RETRY_MAX_TRY		(ONE_DAY_SECS / CONNECTION_RETRY_INTERVAL_MAX)	//retry one day

#define SEND_TIMEO_RETRY_THRESHOLD		3  //multiply by 10 sec

#define REBUILD_DEFAULT_DELAY			(1*HZ)

static int __Schedule_to_Node_Builder_Workq(struct DMS_Node_Container *dnc);
static int __DNC_State_Transition(struct DMS_Node_Container *dnc, int tstate);
static void DNC_Socket_State_Change(struct sock *sk);
static void DNC_Socket_Error_Report(struct sock *sk);


/********************************************************************************/
/*																				*/
/*							FUNCS Implementations								*/
/*																				*/
/********************************************************************************/



/********************************************************************************/
/*																				*/
/*								Socket Operations								*/
/*																				*/
/********************************************************************************/
int
_inet_pton(char *cp,void *dst)
 {
	int value;
	int digit;
	int i;
	char temp;
	char bytes[4];
	char *end = bytes;
	static const int addr_class_max[4] = { 0xffffffff, 0xffffff, 0xffff, 0xff };

	for (i = 0; i < 4; i++) {
		bytes[i] = 0;
	}

	temp = *cp;

	while (1) {
		if (!isdigit(temp))
			return 0;

		value = 0;
		digit = 0;
		for (;;) {
			if (isascii(temp) && isdigit(temp)) {
				value = (value * 10) + temp - '0';
				temp = *++cp;
				digit = 1;
			} else
				break;
		}

		if (temp == '.') {
			if ((end > bytes + 2) || (value > 255))
				return 0;
			*end++ = value;
			temp = *++cp;
		} else if (temp == ':') {
			//                        cFYI(1,("IPv6 addresses not supported for CIFS mounts yet"));
			return -1;
		} else
			break;
	}

	/* check for last characters */
	if (temp != '\0' && (!isascii(temp) || !isspace(temp)))
		if (temp != '\\') {
			if (temp != '/')
				return 0;
			else
				(*cp = '\\'); /* switch the slash the expected way */
		}
	if (value > addr_class_max[end - bytes])
		return 0;

	*((__be32 *) dst) = *((__be32 *) bytes) | htonl(value);

	return 1; /* success */
}


int
_inet_str2n(char *cp)
 {
	int value;
	int digit;
	int i;
	char temp;
	char bytes[4];
	char *end = bytes;
	static const int addr_class_max[4] = { 0xffffffff, 0xffffff, 0xffff, 0xff };

	u32 dst;

	for (i = 0; i < 4; i++) {
		bytes[i] = 0;
	}

	temp = *cp;

	while (1) {
		if (!isdigit(temp))
			return 0;

		value = 0;
		digit = 0;
		for (;;) {
			if (isascii(temp) && isdigit(temp)) {
				value = (value * 10) + temp - '0';
				temp = *++cp;
				digit = 1;
			} else
				break;
		}

		if (temp == '.') {
			if ((end > bytes + 2) || (value > 255))
				return 0;
			*end++ = value;
			temp = *++cp;
		} else if (temp == ':') {
			//                        cFYI(1,("IPv6 addresses not supported for CIFS mounts yet"));
			return -1;
		} else
			break;
	}

	/* check for last characters */
	if (temp != '\0' && (!isascii(temp) || !isspace(temp)))
		if (temp != '\\') {
			if (temp != '/')
				return 0;
			else
				(*cp = '\\'); /* switch the slash the expected way */
		}
	if (value > addr_class_max[end - bytes])
		return 0;

	dst = *((__be32 *) bytes) | htonl(value);

	dst = ntohl(dst);

	return dst; /* success */
}




#if 0
static int __Prepare_Servaddr_In(struct sockaddr_in * servaddr, const char *addr, int port){

	int retcode = DMS_OK;

	if( check_ptr_validation(CP_MOD, __func__, servaddr) )
	{
		memset(servaddr, 0, sizeof(struct sockaddr_in));

		servaddr->sin_family = AF_INET;
		servaddr->sin_port = htons(port);

		//translate string to inet_addr
		if ( _inet_pton(addr, &(servaddr->sin_addr)) >= 0 ) {

			//success
			retcode = DMS_OK;

		}else{

			//fail
			eprintk(DNC_MOD, "ip address translate Fail!: %s:%d.\n", addr, port);
		}

	} else {

		retcode = -DMS_FAIL;
	}

	return retcode;
}
#endif


/* enable alive */
static void __Set_TCP_Alive_Option(struct socket *sock) {

	int retcode = -1, flag = 0;

	flag = 1;
	//retval = s->ops->setsockopt(s, IPPROTO_TCP, SO_KEEPALIVE, (char __user *)&flag, sizeof(flag));
	retcode = sock_setsockopt(sock, IPPROTO_TCP, SO_KEEPALIVE, (char __user *)&flag, sizeof(flag));

	if (retcode < 0) {
		eprintk(DNC_MOD, "Couldn't setsockopt(SO_KEEPALIVE), retcode: %d flag=%lx\n", retcode, sock->sk->sk_flags);
	}else{
		DMS_PRINTK(DNC_DBG, DNC_MOD, "setsockopt(SO_KEEPALIVE), retcode: %d flag=%lx\n", retcode, sock->sk->sk_flags);
	}

	flag = 5; // start send probing packet after idle for 5 seconds
   	retcode = sock->ops->setsockopt(sock, IPPROTO_TCP, TCP_KEEPIDLE, (char __user *)&flag, sizeof(flag));

	if (retcode < 0) {
		eprintk(DNC_MOD, "Couldn't setsockopt(TCP_KEEPIDLE), retcode: %d val=%ds\n", retcode,((struct tcp_sock*)sock->sk)->keepalive_time/HZ);
	}else{
		DMS_PRINTK(DNC_DBG, DNC_MOD, "setsockopt(TCP_KEEPALIVE), retcode: %d val=%ds\n", retcode,((struct tcp_sock*)sock->sk)->keepalive_time/HZ);
	}

	flag = 1; // probing per 1 second
	retcode = sock->ops->setsockopt(sock, IPPROTO_TCP, TCP_KEEPINTVL, (char __user *)&flag, sizeof(flag));

	if (retcode < 0) {
		eprintk(DNC_MOD, "Couldn't setsockopt(TCP_KEEPINTVL), retcode: %d val=%ds\n", retcode, ((struct tcp_sock*)sock->sk)->keepalive_intvl/HZ);
	}else{
		DMS_PRINTK(DNC_DBG, DNC_MOD, "setsockopt(TCP_KEEPINTVL), retcode: %d val=%ds\n", retcode,((struct tcp_sock*)sock->sk)->keepalive_intvl/HZ);
	}

	flag = 5; // if sequentiall probing faii for 5 times... then close connection
	retcode = sock->ops->setsockopt(sock, IPPROTO_TCP, TCP_KEEPCNT, (char __user *)&flag, sizeof(flag));

	if (retcode < 0) {
		eprintk(DNC_MOD, "Couldn't setsockopt(TCP_KEEPCNT), retcode: %d cnt=%d\n", retcode,((struct tcp_sock*)sock->sk)->keepalive_probes);
	}else{
		DMS_PRINTK(DNC_DBG, DNC_MOD, "setsockopt(TCP_KEEPCNT), retcode: %d cnt=%d\n", retcode,((struct tcp_sock*)sock->sk)->keepalive_probes);
	}

	return ;
}

static int __Set_DMS_TCP_Options(struct socket *sock){

	int retcode = -1, flag = 0;
	struct timeval tv_timeo;

	if(IS_LEGAL(DNC_MOD, sock)){

		mm_segment_t oldfs = {0};

		oldfs = get_fs();
		set_fs(KERNEL_DS);

		/*
		 * Dear unsuspecting programmer,
		 *
		 * Don't use sock_setsockopt() for SOL_TCP.  It doesn't check its level
		 * argument and assumes SOL_SOCKET so, say, your TCP_NODELAY will
		 * silently turn into SO_DEBUG.
		 *
		 * Yours,
		 * Keeper of hilariously fragile interfaces.
		 */
		/* Disable the Nagle (TCP No Delay) algorithm */
		flag = 1;
		retcode = sock->ops->setsockopt( sock, IPPROTO_TCP/*SOL_TCP*/, TCP_NODELAY, (char __user *)&flag, sizeof(flag) );

		if (retcode < 0) {

			eprintk(DNC_MOD, "Couldn't setsockopt(TCP_NODELAY), retcode: %d\n", retcode);

		}else{

			DMS_PRINTK(DNC_DBG, DNC_MOD, "setsockopt(TCP_NODELAY), retcode: %d\n", retcode);
		}

		/* set cork to 0*/
		flag = 0;
		retcode = sock->ops->setsockopt(sock, IPPROTO_TCP/*SOL_TCP*/, TCP_CORK, (char __user *)&flag, sizeof(flag));

		if (retcode < 0) {

			eprintk(DNC_MOD, "Couldn't setsockopt(TCP_CORK), retcode: %d\n", retcode);

		}else{

			DMS_PRINTK(DNC_DBG, DNC_MOD, "setsockopt(TCP_CORK), retcode: %d\n", retcode);
		}

		/* set send time out */
		tv_timeo.tv_sec = 10;
		tv_timeo.tv_usec = 0;
		sock_setsockopt(sock, IPPROTO_TCP, SO_SNDTIMEO, (char __user *)&tv_timeo, sizeof(struct timeval));

		/* set recv time out */
		tv_timeo.tv_sec = 10;
		tv_timeo.tv_usec = 0;
		sock_setsockopt(sock, IPPROTO_TCP, SO_RCVTIMEO, (char __user *)&tv_timeo, sizeof(struct timeval));

		__Set_TCP_Alive_Option(sock);

		set_fs(oldfs);

	}

	return retcode;
}

/**
 *
 * @param dnc
 * @return	0	: OK
 * 			-1	: NULL pointer
 */
static int __DNC_Create_Socket(struct DMS_Node_Container * dnc) {

    struct socket *sock = NULL;
	int retcode = -DMS_FAIL;

	if( IS_LEGAL(DNC_MOD, dnc) ) {

		DMS_PRINTK(DNC_DBG, DNC_MOD, "creating socket.. %s\n", (char *)&dnc->ip_str);

		write_lock(&dnc->lock);

		if(dnc->sock == NULL){

			retcode = sock_create(AF_INET, SOCK_STREAM, IPPROTO_TCP, &sock);

			if (retcode >= 0) {

				DMS_PRINTK(DNC_DBG, DNC_MOD, "creating socket ok! set options...\n");

				retcode = __Set_DMS_TCP_Options(sock);

				if(retcode == 0){

					DMS_PRINTK(DNC_DBG, DNC_MOD, "set options OK. \n");

					dnc->sock = sock;

				}else{

					write_unlock(&dnc->lock);
					//fail, release socket
					sock_release(sock);
					write_lock(&dnc->lock);

					DMS_PRINTK(DNC_DBG, DNC_MOD, "set options Fail!\n");
				}

			}else{

				eprintk(DNC_MOD, "create socket Error! retcode: %d\n", retcode);

			}

		}

		write_unlock(&dnc->lock);

	}

	return retcode;

}

static void __DNC_Release_Socket(struct DMS_Node_Container * dnc){

	struct socket *sock = NULL;
	struct task_struct *receiver = NULL;

	if( IS_LEGAL(DNC_MOD, dnc) &&
			IS_LEGAL(DNC_MOD, dnc->sock) )
	{

		/* This section need to be locked due to we want to modify dnc */
		write_lock(&dnc->lock);
		sock = dnc->sock;
		receiver = dnc->recver;
		dnc->sock = NULL;
		dnc->recver = NULL;
		write_unlock(&dnc->lock);

//		if (waitqueue_active(&sock->sk->sk_lock.wq))
//				wake_up(&sock->sk->sk_lock.wq);

		DMS_PRINTK(DNC_DBG, DNC_MOD, "wake up sleeping receivers... !\n");

		if (sock->sk && sock->sk->sk_sleep) {
			sock->sk->sk_err = EDNC_CONNECT;
			wake_up_interruptible(sock->sk->sk_sleep);
		}

		if( IS_LEGAL(DNC_MOD, dnc->rrecv_fn) && (receiver != NULL) ) {

			if(DNC_DBG) print_symbol("\t\t DMS_VDD: DNC: EXE RRECV: %s \n", (unsigned long)dnc->rrecv_fn);

			//release_receiver.
			if(dnc->rrecv_fn){
				dnc->rrecv_fn(receiver);
			}

		}

		if( sock != NULL ) {
			sock_release(sock);
		}

	}

	DMS_PRINTK(DNC_DBG, DNC_MOD, "done!\n");
}



#if 0
static int __Create_Receiver(struct DMS_Node_Container *dnc){

	int retcode = -DMS_FAIL;

	if(IS_LEGAL(DNC_MOD, dnc)){

		//setup to CONNECTED
		atomic_set(&dnc->status, DNODE_CONNECTED);

		//create receiver
		dnc->recver = dnc->crecv_fn();

	}

	return retcode;
}

static int __Release_Receiver(struct DMS_Node_Container *dnc){

	int retcode = -DMS_FAIL;

	if(IS_LEGAL(DNC_MOD, dnc)){

		struct socket *sock = dnc->sock;

		//set status to DISCONNECTED
		atomic_set(&dnc->status, DNODE_DISCONNECTED);

//		if (waitqueue_active(&sock->sk->sk_lock.wq))
//				wake_up(&sock->sk->sk_lock.wq);

		//wake up sleeping receivers
		if (sock->sk && sock->sk->sk_sleep) {
			sock->sk->sk_err = EIO;
			wake_up_interruptible(sock->sk->sk_sleep);
		}

		//release_receiver.
		dnc->rrecv_fn(dnc->recver);


	}

	return retcode;
}
#endif


#if 0
/**
 *
 * @param ip_str
 * @param port
 * @param sock
 * @return	0:		connect success
 * 			-1:		NULL ptr
 * 			-1602:	error ip format
 */
static int __Try_to_Connect_to_Server( char *ip_str, int port, struct socket *sock ){

	int retcode = -1;
	struct sockaddr_in servaddr;

	if(IS_LEGAL(DNC_MOD, ip_str) &&
			IS_LEGAL(DNC_MOD, sock) &&
			IS_LEGAL(DNC_MOD, sock->ops) )
	{

		DMS_PRINTK(DNC_DBG, DNC_MOD, "start~ socket = %p, ip = %s, port = %d\n", sock, ip_str, port);

		memset(&servaddr, 0, sizeof(struct sockaddr_in ));

		servaddr.sin_family = AF_INET;
		servaddr.sin_port = htons(port);

		//translate string to __u32 ip address
		if ( _inet_pton(ip_str, &(servaddr.sin_addr)) == true ) {

			retcode = sock->ops->connect(sock, (struct sockaddr *)&servaddr, sizeof (servaddr), 0);

		} else {

			eprintk(DNC_MOD, "translate ip address err: %s:%d.\n", ip_str, port);
			retcode = -EDNC_FORMAT;
		}

	}

	//-113: -EHOSTUNREACH, No route to host (physical network cable disconnect)
	DMS_PRINTK(DNC_DBG, DNC_MOD, "end~! retcode = %d\n", retcode);

	return retcode;

}
#endif


static int __Try_to_Connect_to_Server( int node_ip, short port, struct socket *sock ){

	int retcode = -1;
	struct sockaddr_in servaddr;

	if(	IS_LEGAL(DNC_MOD, sock) &&
			IS_LEGAL(DNC_MOD, sock->ops) )
	{

		char *ip_c = (char *)&node_ip;
		DMS_PRINTK(DNC_DBG, DNC_MOD, "start~ socket = %p, ip = %d.%d.%d.%d, port = %d\n",
				sock, ip_c[3], ip_c[2], ip_c[1], ip_c[0], port);

		memset(&servaddr, 0, sizeof(struct sockaddr_in ));

		servaddr.sin_family = AF_INET;
		servaddr.sin_addr.s_addr = (__force u32)htonl(node_ip);
		servaddr.sin_port = (__force u16)htons(port);

		//translate string to __u32 ip address
//		if ( _inet_pton(node_ip, &(servaddr.sin_addr)) == true ) {

			retcode = sock->ops->connect(sock, (struct sockaddr *)&servaddr, sizeof (servaddr), 0);

//		} else {
//
//			eprintk(DNC_MOD, "translate ip address err: %s:%d.\n", node_ip, port);
//			retcode = -EDNC_FORMAT;
//		}

	}

	//-113: -EHOSTUNREACH, No route to host (physical network cable disconnect)
	DMS_PRINTK(DNC_DBG, DNC_MOD, "end~! retcode = %d\n", retcode);

	return retcode;

}



/*
 * Node_Connection_Handler will try to build connection to server.
 * it checks the state of the socket status to know .
 * of course it needn't to re-connect if the socket is valid.
 */
static void Node_Connection_Handler(void *data){

	int retcode = -DMS_FAIL;
	struct DMS_Node_Container *dnc = (struct DMS_Node_Container *)data;

	if( IS_LEGAL(DNC_MOD, dnc) )
	{

		//this condition shouldn't happen actually.
		if(dnc->sock == NULL){

			wprintk(DNC_MOD, "socket is NULL, no~! some logic bug here. check it out, dnc ip = %s, status = %d \n",
					dnc->ip_str, atomic_read(&dnc->status));

			__DNC_Create_Socket(dnc);
		}

		if( IS_LEGAL(DNC_MOD, dnc->sock) && atomic_read(&dnc->status) == DNODE_BUILDING )
		{

			DMS_PRINTK(DNC_DBG, DNC_MOD, "dnc = %p, ip = %s\n",
									dnc, dnc->ip_str);

			write_lock(&dnc->lock);

			//try to re-connect
			retcode = __Try_to_Connect_to_Server(dnc->ssk.ip, dnc->ssk.port, dnc->sock);

			switch(retcode) {

				case DMS_OK:

					dnc->sock->sk->sk_user_data = dnc;
					dnc->sock->sk->sk_error_report = DNC_Socket_Error_Report;
					dnc->sock->sk->sk_state_change = DNC_Socket_State_Change;

					__DNC_State_Transition(dnc, DNODE_CONNECTED);

					break;

				case -EDNC_FORMAT:
					//if ip format error, we can't do anything, so release it
					goto ERROR_IP_FORMAT;
					break;

				case -EISCONN:
					break;

				case -EALREADY:
					//means previous operation still in progress (try to conn), this should never happen. break directly.
					break;

				default:
					DMS_PRINTK(DNC_DBG, DNC_MOD, "build connection fail, retcode = %d, %s; \
								re-schedule to workq~! sk->state = %d, dnc = %p, dnc->status = %d\n",
								retcode, __errorntostr(retcode), dnc->sock->sk->sk_state, dnc, atomic_read(&dnc->status));

					write_unlock(&dnc->lock);
					__Schedule_to_Node_Builder_Workq(dnc);
					write_lock(&dnc->lock);

			}

			write_unlock(&dnc->lock);

		}

	}else{

		//driver is unloading...
		//Free_and_Remove_from_Connection_Retry_Data(data);
	}

	DMS_PRINTK(DNC_DBG, DNC_MOD, "end~! dnc = %p \n", dnc);

	return;

ERROR_IP_FORMAT:

	write_unlock(&dnc->lock);
	DMS_PRINTK(DNC_DBG, DNC_MOD, "ERROR! dnc = %p, ERROR_IP_FORMAT! \n", dnc);
	Destroy_DMS_Node_Container(dnc->index);

}



/**
 * __Schedule_to_Node_Builder_Workq
 * @param dnc
 * @return 	1	: schedule success.
 * 			0	: scheduled already.
 * 			-1	: NULL pointer.
 * 			-1603: rebuild connection time out, release this node.
 *
 * DESCRIPTION:
 *		schedule DMS_Node_Container to Schedule_to_Node_Builder_workq.
 * 		give up retry, if retry_count over one day.
 */
static int __Schedule_to_Node_Builder_Workq(struct DMS_Node_Container *dnc){

	int delay_interval = 0, retcode = -DMS_FAIL;

	if(IS_LEGAL(DNC_MOD, dnc) &&
			IS_LEGAL(DNC_MOD, dnc->sock) )
	{

		//re-entrance, but only one can pass here.
		write_lock_irq(&dnc->lock);

		if( !delayed_work_pending(&dnc->bc_work) ){

			//check retry counter
			if(dnc->retry_count <= CONNECTION_RETRY_MAX_TRY){

				delay_interval = HZ * CONNECTION_RETRY_INTERVAL * dnc->retry_count++;

				//use MAX, if over the MAX
				delay_interval = ( delay_interval >= CONNECTION_RETRY_INTERVAL_MAX_HZ ) ?
						CONNECTION_RETRY_INTERVAL_MAX_HZ : delay_interval;

				DMS_PRINTK(DNC_DBG, DNC_MOD, "retry_count = %d, interval = %d \n",
						dnc->retry_count, delay_interval/HZ);

				INIT_WORK(&dnc->bc_work, Node_Connection_Handler, dnc);

				// The return value from is nonzero if the work_struct was actually added to queue
				// (otherwise, it may have already been there and will not be added a second time).
				retcode = queue_delayed_work(node_builder_workq, &dnc->bc_work, delay_interval);

				write_unlock_irq(&dnc->lock);

			}else{

				//unlock first
				write_unlock_irq(&dnc->lock);

				//this entry has been tried one day, give up this connection.
				Destroy_DMS_Node_Container(dnc->index);

				retcode = -EDNC_RC_OVTIME;

			}

		} else {

			write_unlock_irq(&dnc->lock);

			retcode = DMS_OK;
		}

	}

	DMS_PRINTK(DNC_DBG, DNC_MOD, "end~! retcode = %d\n", retcode);

	return retcode;
}

/**
 * __Cancel_from_Node_Builder_Workq
 *
 * @param dnc
 * @return	1	: cancel success.
 * 			0	: canceled already.
 * 			-1	: NULL pointer.
 */
static int __Cancel_from_Node_Builder_Workq(struct DMS_Node_Container *dnc){

	int retcode = -DMS_FAIL;

	if(IS_LEGAL(DNC_MOD, dnc) &&
			IS_LEGAL(DNC_MOD, dnc->sock) )
	{

		write_lock_irq(&dnc->lock);

		if(delayed_work_pending(&dnc->bc_work)){

			//cancel work. dnc manager will flush work queue
			if( (retcode = cancel_delayed_work(&dnc->bc_work)) == 0 ){

				eprintk(DNC_MOD, "cancel FAIL! dnc = %p, node_ip = %s, waiting for work done... \n",
						dnc, dnc->ip_str);

				write_unlock_irq(&dnc->lock);

				flush_workqueue(node_builder_workq);
				eprintk(DNC_MOD, "work done! dnc = %p, node_ip = %s \n",
										dnc, dnc->ip_str);

				write_lock_irq(&dnc->lock);
			}

		}else{

			//canceled already
			retcode = DMS_OK;
		}

		write_unlock_irq(&dnc->lock);

	}

	DMS_PRINTK(DNC_DBG, DNC_MOD, "end~! retcode = %d\n", retcode);

	return retcode;
}



/**
 * __DNC_State_Transition
 *
 * @param dnc
 * @param tstate
 * @return	0	:	OK
 * 			-1	:	NULL pointer
 *
 * 	BUILDING 1	: schedule success.
 * 			 0	: scheduled already.
 */
static int __DNC_State_Transition(struct DMS_Node_Container *dnc, int tstate){

	int retcode = -DMS_FAIL;

	if(IS_LEGAL(DNC_MOD, dnc)){

		DMS_PRINTK(DNC_DBG, DNC_MOD, "dnc = %p, ip = %s, orig_state = %d, tstate = %d \n",
					dnc, dnc->ip_str, atomic_read(&dnc->status), tstate);

		//check status
		switch( tstate ){

			case DNODE_INIT:

				retcode = __DNC_Create_Socket(dnc);

				if(retcode >= 0){
					atomic_set(&dnc->status, DNODE_INIT);
				}
				break;

			case DNODE_BUILDING:

				retcode = __Schedule_to_Node_Builder_Workq(dnc);

				if(retcode >= 0){
					atomic_set(&dnc->status, DNODE_BUILDING);
				}
				break;

			case DNODE_CONNECTED:

				if(dnc->sock->sk->sk_state == TCP_ESTABLISHED){

					//set status to CONNECTED
					atomic_set(&dnc->status, DNODE_CONNECTED);

					//FIXME do I need lock here?, if lock, you should modify Node_Connection_Handler too.
					//create receiver
					if( dnc->crecv_fn && !dnc->recver ){

						dnc->recver = dnc->crecv_fn(dnc->index, dnc->private);

						if(dnc->recver){

							//run receiver
							wake_up_process(dnc->recver);
							retcode = DMS_OK;
						}
					}

				}else{

					eprintk(DNC_MOD, "FAITAL ERROR!! sock->sk_state = %d, but thread %s want to change state to DNODE_CONNECTED! \n",
							dnc->sock->sk->sk_state, (char *)&current->comm);
				}

				break;

			case DNODE_DISCONNECTED:

				//set status to DISCONNECTED
				atomic_set(&dnc->status, DNODE_DISCONNECTED);
				__DNC_Release_Socket(dnc);

				retcode = DMS_OK;

				break;

			default:
				eprintk(DNC_MOD, "unknown transition status = %d \n", tstate);

		}

	}

	DMS_PRINTK(DNC_DBG, DNC_MOD, "end~ dnc = %p, retcode = %d, msg = %s \n", dnc, retcode, __errorntostr(retcode));

	return retcode;
}


/**
 * Build_Connection
 * @param dnc
 * @return	1	: schedule success.
 * 			0	: scheduled already.
 * 			-1	: NULL pointer or improper state.
 * 			-1603: rebuild connection time out, release this node.
 */
int Build_Connection(struct DMS_Node_Container *dnc){

	int retcode = -DMS_FAIL;

	if(IS_LEGAL(DNC_MOD, dnc) &&
			IS_LEGAL(DNC_MOD, dnc->sock) )
	{

		if( atomic_read(&dnc->status) == DNODE_INIT ){

			//transit status to BUILDING
			retcode = __DNC_State_Transition(dnc, DNODE_BUILDING);

		}

	}

	DMS_PRINTK(DNC_DBG, DNC_MOD, "end~! retcode = %d\n", retcode);

	return retcode;
}




/********************************************************************************/
/*																				*/
/*								ERROR HANDLING									*/
/*																				*/
/********************************************************************************/

void __Rebuild_Connection(void *data){

	struct DMS_Node_Container *dnc = (struct DMS_Node_Container *)data;

	if(IS_LEGAL(DNC_MOD, dnc)){

		DMS_PRINTK(DNC_DBG, DNC_MOD, "node ip = %s \n",
									(char *)&dnc->ip_str);

		//release socket first if socket is availabe
		__DNC_State_Transition(dnc, DNODE_DISCONNECTED);

		//init socket
		__DNC_State_Transition(dnc, DNODE_INIT);

		//schedule to build connection
		Build_Connection(dnc);

	}
}


int DMS_Sock_ErrorHandling(struct socket *sock, int send, int errno){

	int retcode = errno;
	struct DMS_Node_Container *dnc = NULL;

	if(IS_LEGAL(DNC_MOD, sock)){

		dnc = (struct DMS_Node_Container *)sock->sk->sk_user_data;

		switch (errno) {

			case DMS_OK:
			case -EAGAIN:
			case -ETIMEDOUT:

				//check socket state, if ESTABLISHED, we don't care receive timeout error.
				if(sock->sk->sk_state == TCP_ESTABLISHED){

					DMS_PRINTK(DNC_DBG, DNC_MOD, "WARN: node ip = %s, errno = %d, msg = %s but sk_state == TCP_ESTABLISHED \n",
							(char *)&dnc->ip_str, errno, __errorntostr(errno));

					//only receive
					if(!send)
						retcode = 0;

				}else{

					eprintk(DNC_MOD, "FATAL ERROR! recv error code = %d, msg = %s\n", errno, __errorntostr(errno));

					if(send){
						retcode = -EDNC_SNDTIMEO;
					} else{
						retcode = -EDNC_CONNECT;
					}

				}

				break;

			case -EIO:
				break;

			case -EPIPE:
				break;

			case -EDNC_CONNECT:
				break;

			case -EDNC_RC_OVTIME:
				break;

			case -EDNC_SNDTIMEO:
#if 0
				//TODO should I do this?
				if( ++dnc->send_retry >= SEND_TIMEO_RETRY_THRESHOLD){

					DMS_PRINTK(DNC_DBG, DNC_MOD, "WARN: node ip = %s, port = %d, errno = %d, msg = %s, changing dnc state \n",
							(char *)&dnc->ip_str, dnc->port, __errorntostr(errno));

					//invalidate this socket
					//TODO I don't know whether this useful or not;
				}
#endif
				break;

			case -EDNC_RECVTIMEO:
				//do nothing
				break;

			default:
				eprintk(DNC_MOD, "Unknow error code = %d \n", errno);
		}
	}

	return retcode;
}


static void DNC_Socket_State_Change(struct sock *sk)
{
	struct DMS_Node_Container *dnc = NULL;

	read_lock(&sk->sk_callback_lock);

	if (!(dnc = (struct DMS_Node_Container *)sk->sk_user_data))
		goto out;


	iprintk(DNC_MOD, " node_ip = %s, sk->state = %d, cur = %s, Are we in interrupt? %lu \n",
			(char *)&dnc->ip_str, sk->sk_state, (char *)&current->comm, in_interrupt());

	//execute user call-back
	if (dnc->node_state_change != NULL){

		dnc->node_state_change(sk);
	}

	switch (sk->sk_state) {

		case TCP_SYN_SENT:
			iprintk(DNC_MOD, "TCP_SYN_SENT\n");
			break;

		case TCP_SYN_RECV:
			iprintk(DNC_MOD, "TCP_SYN_RECV\n");
			break;

		case TCP_ESTABLISHED:
			iprintk(DNC_MOD, "TCP_ESTABLISHED\n");
			break;

		default:

			iprintk(DNC_MOD, "sk_state = %d, SHUTDOWN!\n", sk->sk_state);

			/* wakeup receiver and stop it. */

			//if (!test_and_set_bit(LF_net_shutdown, &loader->flags))
			//	send_sig(SIGTERM, loader->thread, 1);

			/* schedule to node build work queue. */
			write_lock(&dnc->lock);
			INIT_WORK(&dnc->bc_work, __Rebuild_Connection, dnc);
			queue_delayed_work(node_builder_workq, &dnc->bc_work, REBUILD_DEFAULT_DELAY);
			write_unlock(&dnc->lock);
	}


	out:
	read_unlock(&sk->sk_callback_lock);
}

static void DNC_Socket_Error_Report(struct sock *sk)
{
	struct DMS_Node_Container *dnc = NULL;

	read_lock(&sk->sk_callback_lock);

	if (!(dnc = (struct DMS_Node_Container *)sk->sk_user_data))
		goto out;

	iprintk(DNC_MOD, " node_ip = %s, sk->state = %d, cur = %s, nerrep = %p, Are we in interrupt? %lu \n",
				(char *)&dnc->ip_str, sk->sk_state, (char *)&current->comm, dnc->node_error_report, in_interrupt());


	if (dnc->node_error_report != NULL){

		dnc->node_error_report(sk);
	}

out:
    read_unlock(&sk->sk_callback_lock);
}


/********************************************************************************/
/*																				*/
/*								Tx/Rx Operations								*/
/*																				*/
/********************************************************************************/
#if 1
/*
 *  Send or receive packet.
 */
int sock_xmit(struct socket *sock, int send, void *buf, int len,
		int msg_flags)
{
	int result;
	struct msghdr msg;
	struct kvec iov;

#ifdef KILLSIGNAL
	unsigned long flags;
	sigset_t oldset;

	/* Allow interception of SIGKILL only
	 * Don't allow other signals to interrupt the transmission */
	spin_lock_irqsave(&current->sighand->siglock, flags);
	oldset = current->blocked;
	sigfillset(&current->blocked);
	sigdelsetmask(&current->blocked, sigmask(SIGKILL));
	recalc_sigpending();
	spin_unlock_irqrestore(&current->sighand->siglock, flags);
#endif

	do {
		sock->sk->sk_allocation = GFP_KERNEL;
		iov.iov_base = buf;
		iov.iov_len = len;
		msg.msg_name = NULL;
		msg.msg_namelen = 0;
		msg.msg_control = NULL;
		msg.msg_controllen = 0;
		msg.msg_flags = msg_flags | MSG_NOSIGNAL;

		if (send){

			result = kernel_sendmsg(sock, &msg, &iov, 1, len);

			if( result != len && result > 0 ){
				eprintk(DNC_MOD, "sock_sendmsg size mismatch, expect_send_size = %d, socket_send_size = %d\n",
								len, result);
				result = -EDNC_SNDTIMEO;
			}

		} else {

			do{
				//check receiver state
				if (kthread_should_stop()) {
					result = -EPIPE;
					break;
				}

				result = kernel_recvmsg(sock, &msg, &iov, 1, len, 0);

			}while(result == 0 || (result == -EAGAIN) || (result == -ETIMEDOUT));//lego: when lose connection, sock_recvmsg will return 0

		}

#ifdef KILLSIGNAL
		if (signal_pending(current)) {
			siginfo_t info;
			spin_lock_irqsave(&current->sighand->siglock, flags);
			printk(KERN_WARNING "nbd (pid %d: %s) got signal %d\n",
				current->pid, current->comm,
				dequeue_signal(current, &current->blocked, &info));
			spin_unlock_irqrestore(&current->sighand->siglock, flags);
			result = -EINTR;
			break;
		}
#endif

		if (result <= 0) {
			if ( (result = DMS_Sock_ErrorHandling(sock, send, result)) < 0 ){
				DMS_PRINTK(DNC_DBG, DNC_MOD, "FATAL ERROR! result = %d, msg = %s \n", result, __errorntostr(result));
				break;
			}
		}

		len -= result;
		buf += result;

	} while (len > 0 /*&& !kthread_should_stop()*/);

#ifdef KILLSIGNAL
	spin_lock_irqsave(&current->sighand->siglock, flags);
	current->blocked = oldset;
	recalc_sigpending();
	spin_unlock_irqrestore(&current->sighand->siglock, flags);
#endif

	DMS_PRINTK(DNC_DBG, DNC_MOD, "end~! result = %d \n", result);

	return result;
}

#endif

/**
 * TODO this function used to improve socket performance,
 * because tcp_sendpage wouldn't copy data again, it use the page directly if it can be.
 * ref: net/ipv4/tcp.c: tcp_sendpage()
 *
 * @param sock
 * @param pages
 * @param poffset
 * @param psize
 * @param msg_flags
 * @return
 */
int sock_send_pages(struct socket *sock, struct page *pages, int poffset, int psize,
		int msg_flags)
{
	ssize_t result;

#ifdef KILLSIGNAL
	unsigned long flags;
	sigset_t oldset;

	/* Allow interception of SIGKILL only
	 * Don't allow other signals to interrupt the transmission */
	spin_lock_irqsave(&current->sighand->siglock, flags);
	oldset = current->blocked;
	sigfillset(&current->blocked);
	sigdelsetmask(&current->blocked, sigmask(SIGKILL));
	recalc_sigpending();
	spin_unlock_irqrestore(&current->sighand->siglock, flags);
#endif


		result = sock->ops->sendpage(sock, &pages, poffset, psize, msg_flags);

		if( result != psize && result > 0 ){
			eprintk(DNC_MOD, "tcp_sendpage size mismatch, expect_send_size = %d, socket_send_size = %llu \n",
							psize, result);

			result = -EDNC_SNDTIMEO;
		}

#ifdef KILLSIGNAL
		if (signal_pending(current)) {
			siginfo_t info;
			spin_lock_irqsave(&current->sighand->siglock, flags);
			printk(KERN_WARNING "nbd (pid %d: %s) got signal %d\n",
				current->pid, current->comm,
				dequeue_signal(current, &current->blocked, &info));
			spin_unlock_irqrestore(&current->sighand->siglock, flags);
			result = -EINTR;
			break;
		}
#endif

		if (result <= 0) {
			if ( (result = DMS_Sock_ErrorHandling(sock, true, result)) < 0 ){
				DMS_PRINTK(DNC_DBG, DNC_MOD, "FATAL ERROR! result = %d, msg = %s \n", result, __errorntostr(result));
			}
		}



#ifdef KILLSIGNAL
	spin_lock_irqsave(&current->sighand->siglock, flags);
	current->blocked = oldset;
	recalc_sigpending();
	spin_unlock_irqrestore(&current->sighand->siglock, flags);
#endif

	DMS_PRINTK(DNC_DBG, DNC_MOD, "end~! result = %d \n", result);

	return result;
}






/********************************************************************************/
/*																				*/
/*								Init/Release funcs								*/
/*																				*/
/********************************************************************************/

int SPrint_DMS_Node_Containers(char *buf, int len_limit, struct DMS_Node_Container *dnc){

	int len = 0;

	if( IS_LEGAL(DNC_MOD, buf) &&
			IS_LEGAL(DNC_MOD, dnc) )
	{

		if(len_limit > 0){

			len += sprintf(buf+len, "\t%s, dnc = {\n\t\tnode_ip = %s, sock = %p, kref = %d, avg_arv_rate = %d, status = %d, "
					"\n\t\trecver = %p, crecv_fn = %p, rrecv_fn = %p, \n\t\tnode_error_report = %p, node_state_change = %p, mgr_index = %d\n\t} \n",
					DNC_MOD, (char *)&dnc->ip_str, dnc->sock, atomic_read(&dnc->dnc_kref.refcount), dnc->avg_arv_rate,
					atomic_read(&dnc->status), dnc->recver, dnc->crecv_fn, dnc->rrecv_fn, dnc->node_error_report, dnc->node_state_change, dnc->index);

		}

	}

	return len;

}



/**
 * Init_DMS_Node_Container	:init datastructure and create socket.
 * @param dnc
 * @param ip_str
 * @param port
 * @param nerrep
 * @param nstchg
 * @param crecv_fn
 * @param rrecv_fn
 * @return	0	: OK
 * 			-1	: NULL pointer
 */
int Init_DMS_Node_Container(struct DMS_Node_Container *dnc, int node_ip, short port,
		CreateRecv_Fn_t crecv_fn, ReleaseRecv_Fn_t rrecv_fn, void *data,
		NErRep_Fn_t nerrep_fn, NStChg_Fn_t nstchg_fn) {

	int retcode = -DMS_FAIL;

	if( IS_LEGAL(DNC_MOD, dnc) ){

		char *ip_c = (char *)&node_ip;

		memset(dnc, 0, sizeof(struct DMS_Node_Container));

		dnc->ssk.ip = node_ip;
		dnc->ssk.port = port;

		sprintf((char *)&dnc->ip_str, "%d.%d.%d.%d:%d",
				ip_c[3],
				ip_c[2],
				ip_c[1],
				ip_c[0], port);

		//ref count
		kref_init(&dnc->dnc_kref);

		rwlock_init(&dnc->lock);

		dnc->crecv_fn = crecv_fn;
		dnc->rrecv_fn = rrecv_fn;
		dnc->private = data;

		dnc->node_error_report = nerrep_fn;
		dnc->node_state_change = nstchg_fn;

		//create socket. ps: move to state transition
		//dnc->sock = __DNC_Create_Socket(dnc);

		retcode = __DNC_State_Transition(dnc, DNODE_INIT);

	}

	return retcode;
}

/**
 * Create_DMS_Node_Container
 * @param ip_str
 * @param port
 * @param crecv_fn
 * @param rrecv_fn
 * @param data
 * @param nerrep_fn
 * @param nstchg_fn
 *
 * @return	dnc ptr - if success
 * 			NULL	- if fail
 */
struct DMS_Node_Container * Create_DMS_Node_Container(int node_ip, short port,
		CreateRecv_Fn_t crecv_fn, ReleaseRecv_Fn_t rrecv_fn, void *data,
		NErRep_Fn_t nerrep_fn, NStChg_Fn_t nstchg_fn){

	struct DMS_Node_Container *dnc = NULL;

	dnc = Malloc_DMS_Node_Container(GFP_KERNEL);

	if( Init_DMS_Node_Container(dnc, node_ip, port, crecv_fn, rrecv_fn, data, nerrep_fn, nstchg_fn) != DMS_OK ){
		goto INIT_DNC_FAIL;
	}

	return dnc;

INIT_DNC_FAIL:

	if(CHECK_PTR(DNC_MOD, dnc)){

		Release_DMS_Node_Container(dnc);
	}

	PRINT_DMS_ERR_LOG(EDEV_NOMEM, DNC_MOD, "");

	return NULL;

}

/**
 * Release_DMS_Node_Container	: release dnc by it's state
 * @param dnc
 */
void Release_DMS_Node_Container(struct DMS_Node_Container *dnc){

//	unsigned long flags = 0;

	if( IS_LEGAL(DNC_MOD, dnc) ){

		//can't be interrupted, so we disable irq
		//write_lock_irqsave(&dnc->lock, flags);

		//check status
		switch( atomic_read(&dnc->status) ){

			case DNODE_BUILDING:

				__Cancel_from_Node_Builder_Workq(dnc);

			case DNODE_INIT:
			case DNODE_CONNECTED:

				//change state to DNODE_DISCONNECTED
				__DNC_State_Transition(dnc, DNODE_DISCONNECTED);

			case DNODE_DISCONNECTED:
				//fall go
				break;

			default:
				eprintk(DNC_MOD, "unknown status = %d \n", atomic_read(&dnc->status));

		}

		//free memory
		Free_DMS_Node_Container(dnc);

		//write_unlock_irqrestore(&dnc->lock, flags);

	}

}

/**
 * Init_Node_Builder	:init node builder work queue
 * @return	0	: OK
 * 			-1	: Init fail.
 */
int Init_Node_Builder(void){

	int retcode = -DMS_FAIL;

	node_builder_workq = create_workqueue("node_builder_workq");

	if(node_builder_workq){

		CONNECTION_RETRY_INTERVAL_MAX_HZ = HZ * CONNECTION_RETRY_INTERVAL_MAX;
		retcode = DMS_OK;
	}

	DMS_PRINTK(DNC_DBG, DNC_MOD, "done! retcode = %d \n", retcode);

	return retcode;
}


/**
 * Release_Node_Builder	:flush node builder work queue
 */
void Release_Node_Builder(void){

	if(IS_LEGAL(DNC_MOD, node_builder_workq)){

		DMS_PRINTK(DNC_DBG, DNC_MOD, "start! node_builder_workq = %p \n", node_builder_workq);

		//flush work queue, prevent there are remain works.
		flush_workqueue(node_builder_workq);

		destroy_workqueue(node_builder_workq);

		node_builder_workq = NULL;
	}

	DMS_PRINTK(DNC_DBG, DNC_MOD, "done! \n");
}











#ifdef DMS_UTEST

EXPORT_SYMBOL(_inet_str2n);

#endif


