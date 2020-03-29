/*
 * DMS_Mem_Pool.c
 *
 *  Created on: 2011/7/14
 *      Author: Vis Lee
 *      		Lego Lin
 *
 *  DMS memory pool:
 *  we use mempool to help us implement locking mechanism with kmem_cache, but you
 *  should know it not always return a valid address. you should use IS_Legal() or
 *  CHECK_PTR() to check the returned mem ptr.
 *
 * 	N.B. add a new memory pool steps:
 * 		step 1: add {name, size, NULL}, to dmp_manager
 * 		step 2: #define DM_INDEX_"YOUR_MEMPOOL" as corresponding index in dmp_manager
 * 		step 3: implements one Malloc func, and
 * 		step 4: implements one Free func at Malloc/Free section.
 */


#include "DMS_Common.h"
#include "DMS_Mem_Pool.h"
#include "DList.h"
#include "DIO.h"
#include "LogicalBlock.h"
#include "IO_Request.h"
#include "Metadata.h"
#include "SSocket.h"
#include "DMS_Protocol_Header.h"
#include "Payload.h"

/********************************************************************************/
/*																				*/
/*							Global variables 									*/
/*																				*/
/********************************************************************************/

/* Memory Pool Manager */
static char *DMP_MOD =				"MEMPOOL: ";

/**
 * DMS Memory Pool Manager - manage all pools
 *
 * DESCRIPTION:
 * 		management all memory pool includes INIT, RELEASE etc.
 * 		you have to follow the rule {name, size, NULL} to insert a new mempool, if necessary.
 *
 */
static struct DMP_Manager dmp_manager [] = {

		{"dms_mpool/dlist_node", 	sizeof(struct DList_Node), 	NULL},
		{"dms_mpool/dlist", 		sizeof(struct DList), 			NULL},
		{"dms_mpool/dio", 			sizeof(struct DMS_IO), 		NULL},
		{"dms_mpool/LB_list", 		sizeof(struct LogicalBlock_List), 		NULL},
		{"dms_mpool/LBMD", 			sizeof(struct LogicalBlock_MetaData), 	NULL},
		{"dms_mpool/ioreq", 		sizeof(struct IO_Request), 			NULL},
		{"dms_mpool/dn_locs", 		sizeof(struct Datanode_Location), 		NULL},
		{"dms_mpool/loc_req", 		sizeof(struct Located_Request), 		NULL},
		{"dms_mpool/dmeta", 		sizeof(struct DMS_Metadata), 			NULL},
		{"dms_mpool/pheader", 		sizeof(struct DMS_Protocol_Header), 	NULL},
		{"dms_mpool/dntag", 		sizeof(struct DMS_Datanode_Tag), 		NULL},
		{"dms_mpool/paltag", 		sizeof(struct DMS_Payload_Tag), 		NULL},
		{NULL, 0, NULL},
};


/* please define your index in dmp_manager here */
#define DM_INDEX_DLN			0
#define DM_INDEX_DLIST			1
#define DM_INDEX_DIO			2
#define DM_INDEX_LBLIST			3
#define DM_INDEX_LBMD			4
#define DM_INDEX_IOREQ			5
#define DM_INDEX_DNLOC			6
#define DM_INDEX_LOCREQ			7
#define DM_INDEX_DMETA			8
#define DM_INDEX_PHEADER		9
#define DM_INDEX_DNTAG			10
#define DM_INDEX_PALTAG			11


#define GET_MEMPOOL(index)		(dmp_manager[index].dmpp->poolp)


/********************************************************************************/
/*																				*/
/*					Some data structures who needn't pool						*/
/*																				*/
/********************************************************************************/
inline void* DMS_Malloc(size_t size) {
	return _DMS_Malloc_Generic(size, GFP_KERNEL);
}

inline void* DMS_Malloc_NOWAIT(size_t size) {
	return _DMS_Malloc_Generic(size, GFP_NOWAIT);
}

void* _DMS_Malloc_Generic(size_t size, gfp_t flags){

	void * result = kmalloc(size, flags);

	if(IS_ERR(result)) {

		eprintk(DMP_MOD, "FATAL ERROR!! malloc err, result = %p \n", result);
		return NULL;

	}else if(result==NULL) {

		eprintk(DMP_MOD, "FATAL ERROR!! malloc is null! \n");
		//BUG();
		return NULL;

	}

	//init to 0
    memset(result, 0, size);

	return result;

}

inline void * DMS_Volume_Malloc(size_t size){

	return DMS_Malloc_NOWAIT(size);

}

inline void DMS_Volume_Free(void *ptr){
	kfree(ptr);
}


/**
 * Malloc_DMS_Node_Container
 *
 * @param flags
 * @return struct DMS_Node_Container * -
 *
 * DESCRIPTION:
 * 		allocate DMS_Node_Container memory space
 */
inline struct DMS_Node_Container * Malloc_DMS_Node_Container ( gfp_t flags ){

	return (struct DMS_Node_Container *)_DMS_Malloc_Generic( sizeof(struct DMS_Node_Container), flags|GFP_NOIO );

}

inline void Free_DMS_Node_Container(struct DMS_Node_Container *dnc){

	if(IS_LEGAL(DMP_MOD, dnc)){
		kfree( (void *)dnc );
	}

}

/********************************************************************************/
/*																				*/
/*								POOL APIs										*/
/*																				*/
/********************************************************************************/

struct DMS_Mempool * __Init_Pool(char *cache_name, size_t obj_size){

	int retcode = DMS_OK;
	kmem_cache_t *cp = NULL;
	mempool_t *pp = NULL;

	struct DMS_Mempool *dmp = NULL;
	dmp = (struct DMS_Mempool *)kzalloc(sizeof(struct DMS_Mempool), GFP_KERNEL);

	/* prepare cache */
	cp = kmem_cache_create(cache_name, obj_size, 0, 0, NULL, NULL);

	if( CHECK_PTR(DMP_MOD, cp) != true ){
		eprintk(DMP_MOD, "FATAL ERROR!! kmem_cache_create return null! cache_name = %s, obj_size = %lu \n",
				cache_name, obj_size);
		retcode = -ENOMEM;
		goto exit;
	}

	dmp->cachep = cp;

	DMS_PRINTK(DMP_DBG, DMP_MOD, "cache_name = %s, cache create done! cachep = %p, obj_size = %lu \n",
			cache_name, dmp->cachep, obj_size);

	/* prepare mempool */
	pp = mempool_create_slab_pool(BLKDEV_MIN_RQ, (struct kmem_cache *)dmp->cachep);

	if( CHECK_PTR(DMP_MOD, pp) != true  ){
		eprintk(DMP_MOD, "FATAL ERROR!! mempool_create_slab_pool return null! cache_name = %s \n",
				cache_name);
		retcode = -ENOMEM;
		goto free_cache;
	}

	dmp->poolp = pp;

	DMS_PRINTK(DMP_DBG, DMP_MOD, "cache_name = %s, pool create done! poolp = %p \n",
			cache_name, dmp->poolp);

	return dmp;

free_pool:
	/* release mempool first, because it holds pre-allocate objs */
	mempool_destroy(dmp->poolp);

free_cache:
	/* release cache */
	kmem_cache_destroy(dmp->cachep);

exit:

	kfree(dmp);

	dmp = ERR_PTR(retcode);

	return dmp;
}



void __Release_Pool(char *cache_name, struct DMS_Mempool *dmp){

	if( IS_LEGAL(DMP_MOD, dmp) ){

		/* release mempool first, because it holds pre-allocate objs */
		if( IS_LEGAL(DMP_MOD, dmp->poolp) ){

			mempool_destroy(dmp->poolp);
			DMS_PRINTK(DMP_DBG, DMP_MOD, "cache_name = %s, pool free-ed! \n", cache_name);
		}

		/* release cache */
		if( IS_LEGAL(DMP_MOD, dmp->cachep) ){

			if(kmem_cache_destroy(dmp->cachep) == DMS_OK){

				DMS_PRINTK(DMP_DBG, DMP_MOD, "cache_name = %s, cache free-ed! \n", cache_name);

			}else{

				eprintk(DMP_MOD, "cache_name = %s, cache free FAIL! there are something remained. \n", cache_name);
			}

		}

		//TODO should we do something here, if free fail.
		/* release dm */
		kfree(dmp);

	}

}




/********************************************************************************/
/*																				*/
/*								Malloc/Free Functions							*/
/*																				*/
/********************************************************************************/

/**
 * Malloc_DLN
 *
 * @param flags
 * @return DList_Node * -
 *
 * DESCRIPTION:
 * 		allocate DList_Node
 */
struct DList_Node * Malloc_DLN ( gfp_t flags ){

	return (struct DList_Node *)mempool_alloc( GET_MEMPOOL(DM_INDEX_DLN), flags );

}

void Free_DLN(struct DList_Node *dln){

	//TODO we can remove this after the driver stable.
	if(IS_LEGAL(DMP_MOD, dln)){
		mempool_free( (void *)dln, GET_MEMPOOL(DM_INDEX_DLN) );
	}

}


/**
 * Malloc_DList
 *
 * @param flags
 * @return struct DList * -
 *
 * DESCRIPTION:
 * 		allocate DList memory space
 */
struct DList * Malloc_DList ( gfp_t flags ){

	return (struct DList *)mempool_alloc( GET_MEMPOOL(DM_INDEX_DLIST), flags );

}

void Free_DList(struct DList *dlist){

	if(IS_LEGAL(DMP_MOD, dlist)){
		mempool_free( (void *)dlist, GET_MEMPOOL(DM_INDEX_DLIST) );
	}

}


/**
 * Malloc_DIO
 *
 * @param flags
 * @return struct DMS_IO * -
 *
 * DESCRIPTION:
 * 		allocate DIO memory space
 */
struct DMS_IO * Malloc_DIO ( gfp_t flags ){

	return (struct DMS_IO *)mempool_alloc( GET_MEMPOOL(DM_INDEX_DIO), flags );

}

void Free_DIO(struct DMS_IO *dio){

	if(IS_LEGAL(DMP_MOD, dio)){
		mempool_free( (void *)dio, GET_MEMPOOL(DM_INDEX_DIO) );
	}

}


/**
 * Malloc_LBMD
 *
 * @param flags
 * @return struct LogicalBlock_MetaData * -
 *
 * DESCRIPTION:
 * 		allocate LogicalBlock_MetaData memory space
 */
struct LogicalBlock_MetaData * Malloc_LBMD ( gfp_t flags ){

	return (struct LogicalBlock_MetaData *)mempool_alloc( GET_MEMPOOL(DM_INDEX_LBMD), flags );

}

void Free_LBMD(struct LogicalBlock_MetaData *lbmd){

	if(IS_LEGAL(DMP_MOD, lbmd)){
		mempool_free( (void *)lbmd, GET_MEMPOOL(DM_INDEX_LBMD) );
	}

}


/**
 * Malloc_LB_List
 *
 * @param flags
 * @return struct LogicalBlock_List * -
 *
 * DESCRIPTION:
 * 		allocate LogicalBlock_List memory space
 */
struct LogicalBlock_List * Malloc_LB_List ( gfp_t flags ){

	return (struct LogicalBlock_List *)mempool_alloc( GET_MEMPOOL(DM_INDEX_LBLIST), flags );

}

void Free_LB_List(struct LogicalBlock_List *lb_list){

	if(IS_LEGAL(DMP_MOD, lb_list)){
		mempool_free( (void *)lb_list, GET_MEMPOOL(DM_INDEX_LBLIST) );
	}

}


/**
 * Malloc_IO_Request
 *
 * @param flags
 * @return struct IO_Request * -
 *
 * DESCRIPTION:
 * 		allocate IO_Request memory space
 */
struct IO_Request * Malloc_IO_Request ( gfp_t flags ){

	return (struct IO_Request *)mempool_alloc( GET_MEMPOOL(DM_INDEX_IOREQ), flags );

}

void Free_IO_Request(struct IO_Request *ior){

	if(IS_LEGAL(DMP_MOD, ior)){
		mempool_free( (void *)ior, GET_MEMPOOL(DM_INDEX_IOREQ) );
	}

}


/**
 * Malloc_Datanode_Location
 *
 * @param flags
 * @return struct Datanode_Location * -
 *
 * DESCRIPTION:
 * 		allocate Datanode_Location memory space
 */
struct Datanode_Location * Malloc_Datanode_Location ( gfp_t flags ){

	return (struct Datanode_Location *)mempool_alloc( GET_MEMPOOL(DM_INDEX_DNLOC), flags );

}

void Free_Datanode_Location(struct Datanode_Location *dn_loc){

	if(IS_LEGAL(DMP_MOD, dn_loc)){
		mempool_free( (void *)dn_loc, GET_MEMPOOL(DM_INDEX_DNLOC) );
	}

}


/**
 * Malloc_Located_Request
 *
 * @param flags
 * @return struct Located_Request * -
 *
 * DESCRIPTION:
 * 		allocate Located_Request memory space
 */
struct Located_Request * Malloc_Located_Request ( gfp_t flags ){

	return (struct Located_Request *)mempool_alloc( GET_MEMPOOL(DM_INDEX_LOCREQ), flags );

}

void Free_Located_Request(struct Located_Request *lr){

	if(IS_LEGAL(DMP_MOD, lr)){
		mempool_free( (void *)lr, GET_MEMPOOL(DM_INDEX_LOCREQ) );
	}

}



/**
 * Malloc_DMS_Metadata
 *
 * @param flags
 * @return struct DMS_Metadata * -
 *
 * DESCRIPTION:
 * 		allocate DMS_Metadata memory space
 */
struct DMS_Metadata * Malloc_DMS_Metadata ( gfp_t flags ){

	return (struct DMS_Metadata *)mempool_alloc( GET_MEMPOOL(DM_INDEX_DMETA), flags );

}

void Free_DMS_Metadata(struct DMS_Metadata *dmeta){

	if(IS_LEGAL(DMP_MOD, dmeta)){
		mempool_free( (void *)dmeta, GET_MEMPOOL(DM_INDEX_DMETA) );
	}

}



/**
 * Malloc_Protocol_Header
 *
 * @param flags
 * @return struct DMS_Protocol_Header * -
 *
 * DESCRIPTION:
 * 		allocate Malloc_Protocol_Header memory space
 */
struct DMS_Protocol_Header * Malloc_Protocol_Header ( gfp_t flags ){

	return (struct DMS_Protocol_Header *)mempool_alloc( GET_MEMPOOL(DM_INDEX_PHEADER), flags );

}

void Free_Protocol_Header(struct DMS_Protocol_Header *header){

	if(IS_LEGAL(DMP_MOD, header)){
		mempool_free( (void *)header, GET_MEMPOOL(DM_INDEX_PHEADER) );
	}

}



/**
 * DMS_Datanode_Tag
 *
 * @param flags
 * @return struct DMS_Datanode_Tag * -
 *
 * DESCRIPTION:
 * 		allocate Malloc_DMS_Datanode_Tag memory space
 */
struct DMS_Datanode_Tag * Malloc_DMS_Datanode_Tag ( gfp_t flags ){

	return (struct DMS_Datanode_Tag *)mempool_alloc( GET_MEMPOOL(DM_INDEX_DNTAG), flags );

}

void Free_DMS_Datanode_Tag(struct DMS_Datanode_Tag *dntag){

	if(IS_LEGAL(DMP_MOD, dntag)){
		mempool_free( (void *)dntag, GET_MEMPOOL(DM_INDEX_DNTAG) );
	}

}



/**
 * Malloc_DMS_Payload_Tag
 *
 * @param flags
 * @return struct DMS_Payload_Tag * -
 *
 * DESCRIPTION:
 * 		allocate Malloc_DMS_Payload_Tag memory space
 */
struct DMS_Payload_Tag * Malloc_DMS_Payload_Tag ( gfp_t flags ){

	return (struct DMS_Payload_Tag *)mempool_alloc( GET_MEMPOOL(DM_INDEX_PALTAG), flags );

}

void Free_DMS_Payload_Tag(struct DMS_Payload_Tag *dpayload){

	if(IS_LEGAL(DMP_MOD, dpayload)){
		mempool_free( (void *)dpayload, GET_MEMPOOL(DM_INDEX_PALTAG) );
	}

}






#if 0
/**
 * Malloc_DMS_Node_Container
 *
 * @param flags
 * @return struct DMS_Node_Container * -
 *
 * DESCRIPTION:
 * 		allocate DMS_Node_Container memory space
 */
struct DMS_Node_Container * Malloc_DMS_Node_Container ( gfp_t flags ){

	return (struct DMS_Node_Container *)mempool_alloc( GET_MEMPOOL(DM_INDEX_DNCS), flags );

}

void Free_DMS_Node_Container(struct DMS_Node_Container *dnc){

	if(IS_LEGAL(DMP_MOD, dnc)){
		mempool_free( (void *)dnc, GET_MEMPOOL(DM_INDEX_DNCS) );
	}

}
#endif

/********************************************************************************/
/*																				*/
/*								DMP Manager APIs								*/
/*																				*/
/********************************************************************************/
/**
 * Release_DMP_Manager -
 *
 * DESCRIPTION:
 * 		release all memory pools
 */
void Release_DMP_Manager(){

	int i = 0;

	while( CHECK_PTR(DMP_MOD, dmp_manager[i].dmpp) ){

		__Release_Pool(dmp_manager[i].dmp_name, dmp_manager[i].dmpp);

		//reset to NULL;
		dmp_manager[i++].dmpp = NULL;

	}

	DMS_PRINTK(DMP_DBG, DMP_MOD, "release DMS Memory Pool Manager done! nr_pools = %d \n", i);

}

/**
 * Init_DMP_Manager -
 *
 * @return -ENOMEM : no memory
 *
 * DESCRIPTION:
 * 		init all memeory pools
 */
int Init_DMP_Manager(){

	int retcode = DMS_OK;
	int i = 0;

	struct DMS_Mempool *dmp = NULL;

	while(dmp_manager[i].dmp_name != NULL){

		//init all the memory pools
		dmp = __Init_Pool(dmp_manager[i].dmp_name, dmp_manager[i].obj_size);

		if( CHECK_PTR(DMP_MOD, dmp) ){

			//record into dmp_manager
			dmp_manager[i++].dmpp = dmp;

		}else{

			retcode = -ENOMEM;
			goto release_dmp_manager;

		}
	}

	DMS_PRINTK(DMP_DBG, DMP_MOD, "init DMS Memory Pool Manager done! nr_pools = %d \n", i);

	return retcode;

release_dmp_manager:

	Release_DMP_Manager();

	return retcode;

}




#ifdef DMS_UTEST
EXPORT_SYMBOL(DMS_Malloc);
EXPORT_SYMBOL(Malloc_DLN);
EXPORT_SYMBOL(Free_DLN);

#endif







