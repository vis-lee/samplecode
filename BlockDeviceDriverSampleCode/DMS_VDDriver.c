/*
 * DMS_Dev.c
 *
 *  Created on: 2011/7/11
 *      Author: Vis Lee
 *      		Lego Lin
 *
 *  DMS Client IOCTL and interfaces with kernel.
 *
 */

#ifndef __KERNEL__
#define __KERNEL__
#endif
#ifndef MODULE
#define MODULE
#endif


#include <linux/module.h>
#include <linux/moduleparam.h>
#include <linux/init.h>
#include <linux/hdreg.h> /* HDIO_GETGEO */
#include <linux/bio.h>

//#include "DMS_Dev.h"
#include "DMS_Common.h"
#include "DMS_VDDriver.h"
#include "DMS_IOCTL.h"
#include "Volume_Manager.h"
#include "LogicalBlock.h"
#include "IO_Request.h"
#include "SSockets_Manager.h"
#include "Metadata_Manager.h"




/********************************************************************************/
/*																				*/
/*								PARAMETERS										*/
/*																				*/
/********************************************************************************/

/* DMS Client Block Device Module*/
char *DMSDEV_MOD =			"VDDEV: ";

/* IOCTL */
char *IOCTL_MOD = 			"IOCTL: ";

/* name node IP */
static char *namenode_ip =	"10.0.0.1       ";
static short namenode_port = 1234;

/* this flag used to record the state of the dms_vdd, the prefix 'g' means global*/
int g_dms_state;

int DMS_DEV_MAJOR;


/*
 * file operations for dms dev controller use.
 */
struct file_operations controller_fops = {
		owner: THIS_MODULE,
		ioctl: DMS_IOCTL_Handler
};


const char *r_str = "READ";
const char *w_str = "WRITE";
const char *ovw_str = "OVERWRITE";


/********************************************************************************/
/*																				*/
/*								DEFINITIONS										*/
/*																				*/
/********************************************************************************/

#define GET_INODE_BY_FP(fp)				(fp->f_dentry->d_inode)
#define GET_GENDISK_BY_INODE(inode)		(inode->i_bdev->bd_disk)

//enum {
//	RM_SIMPLE  = 0,	/* The extra-simple request function */
//	RM_FULL    = 1,	/* The full-blown version */
//	RM_NOQUEUE = 2,	/* Use make_request */
//};


/********************************************************************************/
/*																				*/
/*									FUNCS										*/
/*																				*/
/********************************************************************************/

void Print_Request_Info(struct request *req){

	if (IS_LEGAL(DMSDEV_MOD, req)) {

		printk(KERN_INFO "request info: rw = %lu, sector_start = %llu, nr_sectors = %lu, cmd = %s, cmd_len = %u, hard_sector = %llu, nr_hw_segments = %d, nr_phys_segments = %d, flag_special = %lu, special = %p, buffer = %p, tag = %d \n",
				rq_data_dir(req), req->sector, req->nr_sectors, req->cmd, req->cmd_len, req->hard_sector, req->nr_hw_segments, req->nr_phys_segments, (req->flags & REQ_SPECIAL), req->special, req->buffer, req->tag);

	}

	{
		int i = 0;
		struct bio *bio;

		rq_for_each_bio(bio, req) {

			__Print_BIO(i, bio);

			int j = 0;
			struct bio_vec *bvec;
			__bio_for_each_segment(bvec, bio, j, 0) {

				printk(" - bi_vect[%d]: page_ptr = %p, bv_offset = %u \n",
						j, bio_page_idx(bio, j), bvec->bv_offset );

			}
			i++;
		}
	}

}


/**
 * DMS_End_Request_Handler - request queue's request handler.
 * @param kreq
 * @param nsectors
 * @param result	<= 0:	fail, the error type direct in error.h
 * @return	-EIO		-5(or 0)	I/O error
 * 			-EFAULT		-14	 		Bad address
 * 			-EEXIST		-17	 		File exists
 * 			-ENODEV		-19	 		No such device
 * 			-ENOSPC		-28	 		No space left on device
 * 			-ESPIPE		-29	 		Illegal seek
 * 			-EROFS		-30	 		Read-only file system
 * 			-EPIPE		-32	 		Broken pipe
 *
 * Description:
 *    when DMS IO has been done, call to here to commit to user.
 */
int DMS_End_Request_Handler(struct request *kreq, unsigned long nsectors, int result){

	int retcode = -DMS_FAIL;
	struct request_queue *r_queue = NULL;
	ulong64 rid = 0;

	if(IS_LEGAL(DMSDEV_MOD, kreq) &&
			IS_LEGAL(DMSDEV_MOD, kreq->special))
	{
		rid = ((struct IO_Request *)kreq->special)->rid;

		DMS_PRINTK(DMSDEV_DBG, DMSDEV_MOD, "th = %s, rid = %llu, result = %d, start~!\n", (char *)&current->comm, rid, result);

		r_queue = kreq->q;

		//get request queue lock first, necessary
		spin_lock_irq(r_queue->queue_lock);

		retcode = end_that_request_first(kreq, result, nsectors);

		if(!retcode) {

			add_disk_randomness(kreq->rq_disk);

	#if (ENABLE_TCQ)

			DMS_PRINTK(DMSDEV_DBG, DMSDEV_MOD, "request = %p, tag_id = %d \n", kreq, kreq->tag);
			blk_queue_end_tag(r_queue, kreq);

	#endif

			//reset the "special" ptr
			kreq->special = NULL;

			end_that_request_last(kreq, result);

		} else {

			DMS_PRINTK(DMSDEV_DBG, DMSDEV_MOD, "end_that_request_first returns = %d, something remained.\n", retcode);
		}

		spin_unlock_irq(r_queue->queue_lock);

	}

	DMS_PRINTK(DMSDEV_DBG, DMSDEV_MOD, "th = %s, rid = %llu end~!\n", (char *)&current->comm, rid);

	return retcode;
}


/**
 * DMS_Request_Handler - request queue's request handler.
 * @r_queue:		request queue that want to dispatch requests to be processed
 *
 * Description:
 *    This function receive request from IO_Scheduler and perform IO.
 */
void DMS_Request_Handler(struct request_queue *r_queue) {

	struct request *kreq = NULL;
	int retcode = 0;

	struct DMS_Volume *volume = NULL;

	/*
	 * it's not proper if we only check blk_queue un-plugged, it may get into infinity loop
	 */
	while( !blk_queue_plugged(r_queue) && (kreq = elv_next_request(r_queue)) ) {

		if (!blk_fs_request (kreq) || kreq->special) {

			wprintk(DMSDEV_MOD, "skip non-fs request... req->special = %p \n", kreq->special);
			Print_Request_Info(kreq);
			end_request(kreq, 0);
			continue;
		}

#if (ENABLE_TCQ)

		retcode = blk_queue_start_tag(r_queue, kreq);

		//there is no more tags.
		if(retcode != 0 ){
			break;
		}

		DMS_PRINTK(DMSDEV_DBG, DMSDEV_MOD, "request = %p, tag_id = %d \n", kreq, kreq->tag);

#else
		//dequeue the request
		blkdev_dequeue_request(kreq);
#endif

		//let request keep receive request
		spin_unlock_irq(r_queue->queue_lock);

		//handle io_req
		volume = Get_DMS_Volume_from_KRequest(kreq);

		if(volume->vIO_Handler != NULL){

			volume->vIO_Handler(volume, kreq);

		} else {

			DMS_End_Request_Handler(kreq, kreq->nr_sectors, false);
			eprintk(DMSDEV_MOD, "There are NO SUCH DEVICE! in Volume Manager!");
		}

		kreq = NULL;

		//lock request queue and ready to elevate next request
		spin_lock_irq(r_queue->queue_lock);

	}//while

	DMS_PRINTK(DMSDEV_DBG, DMSDEV_MOD, "end~! \n");

}


/**
 * IOCTL_AttachVolume - attach volume request IOCTL handler.
 * @ioctl_param:		request parameter includes volumeID, volume capacity, etc.
 *
 * Description:
 *    This function receive request from user space and simply call to volume manager to attach volume.
 */
int IOCTL_AttachVolume(void __user *ioctl_param){

	int retcode = -ATTACH_FAIL;
	struct dms_volume_info vinfo = {0};
	sector_t dsectors = 0;

	copy_from_user(&vinfo, ioctl_param, sizeof(struct dms_volume_info));

	iprintk(IOCTL_MOD, "capacity in bytes = %llu volumeID = %lld\n", vinfo.capacity_in_bytes, vinfo.volid);

	dsectors = CAL_NR_DMSBLKS(vinfo.capacity_in_bytes);
	//TODO add replica factor
	retcode = DMS_Attach_Volume(vinfo.volid, dsectors);

	iprintk(IOCTL_MOD, "attach vol END, volumeID = %lld\n", vinfo.volid);

	return retcode;
}



/**
 * IOCTL_DetachVolume - detach volume request IOCTL handler.
 * @ioctl_param:		request parameter includes volumeID, etc.
 *
 * Description:
 *    This function receive request from user space and simply call to volume manager to detach volume.
 */
int IOCTL_DetachVolume(long ioctl_param){

	int retcode = -DETACH_FAIL;
	long64 volumeID = ioctl_param;

	iprintk(IOCTL_MOD, "detach vol called, ready to detach volume = %lld\n", volumeID);

	retcode = DMS_Detach_Volume(volumeID);

	iprintk(IOCTL_MOD, "detach volume = %lld done~\n", volumeID);

	return retcode;



}

/**
 * DMS_IOCTL_Handler - IOCTL handler.
 * @inode:
 * @file:
 * @ioctl_cmd:
 * @ioctl_param:
 *
 * Description:
 *    This function receive request from user space and perform requests.
 */
int DMS_IOCTL_Handler(struct inode *inode, /* see include/linux/fs.h */
						 struct file *file, /* ditto */
						 unsigned int ioctl_cmd, /* number and param for ioctl */
						 unsigned long ioctl_param) {
	long volumeID;
	struct ResetCacheInfo * reset_cache_info = NULL;
//	UT_Param_t test_param = {0};

//	struct dms_volume_info vinfo = {0};


	/* only system administrator has permission */
	if (!capable(CAP_SYS_ADMIN))
		return -EPERM;


	//TODO check system status, for example: reject when system is unloading. or service not ready

	switch (ioctl_cmd) {

		case IOCTL_RESET_DRIVER:
			clear_bit(DMS_DRIVER_STATE_WORKING_BIT, &g_dms_state);
			//ioctl_reset_driver();
			return DMS_OK;
			break;

		case IOCTL_ATTACH_VOL:

			return IOCTL_AttachVolume((void __user *)ioctl_param);

			break;

		case IOCTL_DETATCH_VOL:
			return IOCTL_DetachVolume(ioctl_param);
			break;

		case IOCTL_FLUSH_VOL:
			printk(KERN_INFO "%s%s, IOCTL_FLUSH_VOL called with volume id: %ld\n", IOCTL_MOD, __func__, ioctl_param);
			return -ENOTTY;
			//wait for all linger ios to finish
			//TODO open this comment to enable FLUSH: return Flush_IO_in_Volume(ioctl_param);
			break;

		case IOCTL_RESETOVERWRITTEN:
			volumeID = ioctl_param;
			iprintk(IOCTL_MOD, "reset overwritten flag called with volume id: %ld\n", volumeID);
			//return clear_overwritten(volid);
			return -ENOTTY;
			break;

		case IOCTL_INVALIDCACHE:
			iprintk(IOCTL_MOD, "received invalidate cache commands from C Daemon\n");
			reset_cache_info = (struct ResetCacheInfo *)kmalloc(sizeof(struct ResetCacheInfo),GFP_ATOMIC);
			copy_from_user(reset_cache_info, (void __user *)ioctl_param, sizeof(struct ResetCacheInfo));
			//return clear_metadata_cache(reset_cache_info);
			kfree(reset_cache_info);
			return -ENOTTY;
			break;

//		case IOCTL_FORCE_REMOVE:
//			iprintk(KERN_INFO "%s, Force to remove the driver\n", IOCTL_MOD);
//			//force_remove_module();
//			return -ENOTTY;
//			break;

		case IOCTL_RELEASE_DRIVER:
			iprintk(IOCTL_MOD, "release driver received\n");
			//clear_pending_queue();
			//release_namenode_req_queue();
			//release_datanode_req_queue();
			//release_namenode_connection();
			return -ENOTTY;
			break;


		default:
			eprintk(IOCTL_MOD, "unknown ioctl:%d\n", ioctl_cmd);
			return -ENOTTY;
			break;
    }

	return 0;
}

/**
 * DMS_IOCTL_Handler - IOCTL handler.
 * @filp:
 * @ioctl_cmd:
 * @ioctl_param:
 *
 * Description:
 *    This function handle the block device IOCTL requests.
 */
long Client_Unlocked_IOCTL(struct file *filp, unsigned int ioctl_cmd,
                              unsigned long ioctl_param) {
	ulong64 size = 0;
//	struct hd_geometry geo = {0};
	struct inode *inode = NULL;
	struct gendisk *disk = NULL;
	struct DMS_Volume *volume = NULL;


	/* only system administrator has permission */
	if (!capable(CAP_SYS_ADMIN))
		return -EPERM;


	DMS_PRINTK(DMSDEV_DBG, IOCTL_MOD, "cmd=%d (BLKFLSBUF=%d BLKGETSIZE=%d BLKSSZGET=%d HDIO_GETGEO=%d)\n",
			ioctl_cmd, BLKFLSBUF, BLKGETSIZE, BLKSSZGET, HDIO_GETGEO);

	switch (ioctl_cmd) {

		case BLKFLSBUF:

			DMS_PRINTK(DMSDEV_DBG, IOCTL_MOD, "BLKFLSBUF: flush buffer\n");
			return 0;

		case BLKGETSIZE:

			if (!ioctl_param) {
				eprintk(IOCTL_MOD, "BLKGETSIZE: null argument\n");
				return -EINVAL; /* NULL pointer: not valid */
			}

			inode = GET_INODE_BY_FP(filp);
			disk= GET_GENDISK_BY_INODE(inode);

			size= get_capacity(disk);
			volume = (struct DMS_Volume *)disk->private_data;

			if(volume != NULL)
				DMS_PRINTK(DMSDEV_DBG, IOCTL_MOD, "The size of volume id %lld is %llu (in DMSBLK sectors), disk->capacity = %llu (in kernel sectors)\n",
						volume->volumeID, volume->dsectors, size);

			if (copy_to_user ((void __user *)ioctl_param, &size, sizeof (size))) {
				return -EFAULT;
			}

			return 0;

		case BLKSSZGET:

			size = BYTES_PER_DMSBLK;
			DMS_PRINTK(DMSDEV_DBG, IOCTL_MOD, "BLKSSZGET, size = %llu\n", size);

			if (copy_to_user((void __user *) ioctl_param, &size, sizeof(size)))
				return -EFAULT;

			return 0;


		default:
			eprintk(IOCTL_MOD, "unknown ioctl %u, (the 21264 cmd is a cdrom ioctl command, don't care!)\n", ioctl_cmd);
			return -ENOTTY;
	}

}


int DMS_VDisk_Open(struct inode * inode, struct file* flip) {

	unsigned unit = iminor(inode);

	DMS_PRINTK(DMSDEV_DBG, DMSDEV_MOD, "flip = %p, unit = %d\n", flip, unit);

	return Open_DMS_Volume(unit);

}

int DMS_VDisk_Release(struct inode * inode, struct file* flip) {

	unsigned unit = iminor(inode);

	DMS_PRINTK(DMSDEV_DBG, DMSDEV_MOD, "unit = %d\n", unit);

	return Close_DMS_Volume(unit);

}

/********************************************************************************/
/*																				*/
/*							MODULE INIT/RELEASE FUNCS 							*/
/*																				*/
/********************************************************************************/
/*
 * init resources dms device needed
 */
int Init_DMS_VDD_Resources(void){


	int retcode = -DMS_FAIL;

	//retcode = Init_DMS_Dev_Resources();
	retcode = DMS_OK;

	DMS_PRINTK(DMSDEV_DBG, DMSDEV_MOD, "done! retcode = %d\n", retcode);

	return retcode;

}

/*
 * release resources of dms device
 */
void Release_DMS_VDD_Resources(void){


	DMS_PRINTK(DMSDEV_DBG, DMSDEV_MOD, "done!\n");

}


/*
 * init all modules
 *
 * if add a new module, please put init function here.
 *
 */

int Init_All_Modules(void){

	int retcode = -DMS_FAIL;

	//register device
	if( (retcode = Init_DMS_VDD_Resources()) < 0)
		goto exit;

	//init dms memory pool manager
	if( (retcode = Init_DMP_Manager()) < 0)
		goto exit;

	//init volume manager
	if( (retcode = Init_DMS_Volume_Manager()) < 0)
		goto exit;

	//init DMS nodes containers
	if( (retcode = Init_DNCS_Manager()) < 0)
			goto exit;

	//init metadata manager
	if( (retcode = Init_Metadata_Manager(_inet_str2n(namenode_ip), namenode_port)) < 0)
			goto exit;

exit:

	DMS_PRINTK(DMSDEV_DBG, DMSDEV_MOD, "done! retcode = %d\n", retcode);

	return retcode;

}

/*
 * release all modules
 *
 * if add a new module, please put release function here.
 *
 */
void Release_All_Modules(void){

	Release_Metadata_Manager();

	Release_DNCS_Manager();

	Release_DMS_Volume_Manager();

	Release_DMP_Manager();

	Release_DMS_VDD_Resources();

	DMS_PRINTK(DMSDEV_DBG, DMSDEV_MOD, "done!\n");
}

/**
 * function: init_rxd()
 * description: Init routine for module insertion.
 */
static int
__init Init_DMS_VDDriver(void) {

	int retcode = DMS_OK;

	allow_signal(SIGINT);
	allow_signal(SIGTERM);

#ifdef OPERATION_TIME_MEASURING
	Init_DMS_Client_Latency_Time_Record();
	Init_DMS_Client_Req_Traversal_Time_Record();
#endif


	DMS_PRINTK(DMSDEV_DBG, DMSDEV_MOD, "namenode ip = %s, port = %d\n", namenode_ip, namenode_port);

	retcode = Init_All_Modules();

	if(retcode < 0)
		goto init_modules_fail;


	//set the driver state
	g_dms_state = 0;

	//set the driver state to INIT
	set_bit(INIT, &g_dms_state);


	//TODO: change MAJOR to 0 to avoid conflict in Linux
	if (register_chrdev(DMS_CONTROLLER_MAJOR, DMS_CONTROLLER_NAME, &controller_fops) < 0) {

		goto reg_chr_fail;
	}

	DMS_DEV_MAJOR = register_blkdev(DMS_DEV_MAJOR, DMS_DEV_NAME);

	if (DMS_DEV_MAJOR < 0) {

		goto reg_blk_fail;

	}

	iprintk(DMSDEV_MOD, "Module name = %s, MAJOR = %d\n",
			DMS_DEV_NAME, DMS_DEV_MAJOR);

	return retcode;


reg_blk_fail:

	eprintk(DMSDEV_MOD, "Module name = %s, unable to get major number, retcode = %d\n",
			DMS_DEV_NAME, DMS_DEV_MAJOR);

	unregister_chrdev(DMS_CONTROLLER_MAJOR, DMS_CONTROLLER_NAME);

reg_chr_fail:

	//TODO disconnect namenode

nn_ip_fail:
	eprintk(DMSDEV_MOD, "error module parameter: namenode_ip = %s\n", namenode_ip);

init_modules_fail:

	return -EIO;

}


/**
 * function: Exit_DMS_Client()
 * description: Exit routine for module removal.
 * return: (void)
 */
static void
__exit Exit_DMS_VDDriver(void) {

#ifdef OPERATION_TIME_MEASURING
	del_timer_sync(rx_statistic_timer);
#endif

	Release_All_Modules();

    unregister_chrdev(DMS_CONTROLLER_MAJOR, DMS_CONTROLLER_NAME);
    unregister_blkdev(DMS_DEV_MAJOR, DMS_DEV_NAME);

    iprintk(DMSDEV_MOD, "Module \"%s\": Major No. = %d unload successfully!\n",
    		DMS_DEV_NAME, DMS_DEV_MAJOR);
}

module_init(Init_DMS_VDDriver);
module_exit(Exit_DMS_VDDriver);

//setup module parameters. user can pass it down when insmod
module_param(namenode_ip, charp, PARAM_PERMISSION);
MODULE_PARM_DESC(namenode_ip, "Namenode ip address");\
module_param(namenode_port, short, PARAM_PERMISSION);
MODULE_PARM_DESC(namenode_ip, "Namenode server port");
MODULE_LICENSE("GPL");
MODULE_AUTHOR(DRIVER_AUTHOR);
MODULE_DESCRIPTION(DRIVER_DESC);
MODULE_VERSION(VERSION_STR);
MODULE_INFO(Copyright, COPYRIGHT);




/********************************************************************************/
/*																				*/
/*							FUNCS FOR UTEST										*/
/*																				*/
/********************************************************************************/

#ifdef DMS_UTEST
/**
 * Test_Commit_Func - only for Test_Volume_Manager utest.
 * @req:	the request should be committed.
 * @r_queue:		request to insert
 *
 * Description:
 *    This function simply end request when testing.
 */
void Test_Commit_Func(struct request *req, struct request_queue *r_queue) {

	struct bio_vec *bvec = NULL;
	struct bio *bio = NULL;
	int i = 0;

	int retcode = -DMS_FAIL;

	DMS_PRINTK(CMT_DBG, DMSDEV_MOD, "start~!\n");

	if(rq_data_dir(req) == DMS_OP_READ){

		rq_for_each_bio(bio, req) {
			bio_for_each_segment(bvec, bio, i) {
				void* kmptr = kmap(bvec->bv_page) + bvec->bv_offset;
				memset(kmptr, 0, bvec->bv_len);
				kunmap(bvec->bv_page);
			}
		}
	}

	spin_lock_irq(r_queue->queue_lock);

	retcode = end_that_request_first(req, 1, req->nr_sectors);

	if(!retcode) {

		//add_disk_randomness(dms->req->rq_disk);

#if (ENABLE_TCQ)

		DMS_PRINTK(CMT_DBG, DMSDEV_MOD, "request = %p, tag_id = %d \n", req, req->tag);
		blk_queue_end_tag(r_queue, req);

#endif

		end_that_request_last(req, 1);

	} else {

		eprintk(DMSDEV_MOD, "end_that_request_first returns error = %d\n", retcode);
	}

	spin_unlock_irq(r_queue->queue_lock);

	DMS_PRINTK(CMT_DBG, DMSDEV_MOD, "end~!\n");
}
EXPORT_SYMBOL(Test_Commit_Func);

/**
 * Test_Request_Handler - only for Test_Volume_Manager utest.
 * @r_queue:		request to be processed
 *
 * Description:
 *    This function simply get request and call commit immediately. Because when we
 *    call add_disk(), kernel will submit request to grab some info.
 */
void Test_Request_Handler(struct request_queue *r_queue) {

	struct request *req = NULL;
	int retcode = 0;

	while( !blk_queue_plugged(r_queue) ) {

		req = elv_next_request(r_queue);

		if ( CHECK_PTR(DMSDEV_MOD, req) ) {

			if (!blk_fs_request (req)) {
				wprintk(DMSDEV_MOD, "skip non-fs request... \n");
				end_request(req, 0);
				continue;
			}

#if (ENABLE_TCQ)

			retcode = blk_queue_start_tag(r_queue, req);

			//there is no more tags.
			if(retcode != 0 ){
				break;
			}

			DMS_PRINTK(CMT_DBG, DMSDEV_MOD, "request = %p, tag_id = %d \n", req, req->tag);

#else
			//dequeue the request
			blkdev_dequeue_request(req);
#endif

			//let request keep receive request
			spin_unlock_irq(r_queue->queue_lock);

			//TODO handle io_req
			Test_Commit_Func(req, r_queue);

			req = NULL;

			//lock request queue and ready to elevate next request
			spin_lock_irq(r_queue->queue_lock);


		} else {

			break;
		}

	}//while

	DMS_PRINTK(CMT_DBG, DMSDEV_MOD, "end~! \n");

}
EXPORT_SYMBOL(Test_Request_Handler);


EXPORT_SYMBOL(DMS_Request_Handler);
EXPORT_SYMBOL(DMS_End_Request_Handler);
#endif








