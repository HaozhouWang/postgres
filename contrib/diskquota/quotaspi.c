/* -------------------------------------------------------------------------
 *
 * quotamodel.c
 *
 * This code is responsible for init disk quota model and refresh disk quota 
 * model.
 *
 * Copyright (C) 2013, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/diskquota/quotamodel.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_database.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#include "diskquota.h"

/*****************************************
*
*  DISK QUOTA HELPER FUNCTIONS
*
******************************************/

PG_FUNCTION_INFO_V1(diskquota_fetch_active_table_stat);

Datum	diskquota_fetch_active_table_stat(PG_FUNCTION_ARGS);

/* Cache to detect the active table list */
typedef struct DiskQuotaActiveTableEntry
{
	Oid         dbid;
	Oid         relfilenode;
	Oid         tablespaceoid;
} DiskQuotaActiveTableEntry;

typedef struct DiskQuotaSizeResultsEntry
{
	Oid     tableoid;
	Oid     dbid;
	Size    tablesize;
} DiskQuotaSizeResultsEntry;

/* The results set cache for SRF call*/
typedef struct DiskQuotaSetOFCache
{
	HTAB                *result;
	HASH_SEQ_STATUS     pos;
} DiskQuotaSetOFCache;

static SmgrStat_hook_type prev_SmgrStat_hook = NULL;

/* functions to refresh disk quota model*/
static void report_active_table_SmgrStat(SMgrRelation reln);
static HTAB *get_active_tables_shm(Oid databaseID);

void
init_disk_quota_hook(void)
{
	prev_SmgrStat_hook = SmgrStat_hook;
	SmgrStat_hook = report_active_table_SmgrStat;
}

Datum
diskquota_fetch_active_table_stat(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	AttInMetadata *attinmeta;
	bool isFirstCall = true;

	HTAB *localResultsCacheTable;
	DiskQuotaSetOFCache *cache;
	DiskQuotaSizeResultsEntry *results_entry;

	/* Init the container list in the first call and get the results back */
	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;

		HASHCTL ctl;
		HTAB *localCacheTable = NULL;
		HASH_SEQ_STATUS iter;
		DiskQuotaActiveTableEntry *shmCache_entry;
		DiskQuotaSizeResultsEntry *sizeResults_entry;

		ScanKeyData relfilenode_skey[2];
		Relation	relation;
		HeapTuple	tuple;
		SysScanDesc relScan;
		Oid			relOid;
		TupleDesc tupdesc;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* build skey */
		MemSet(&relfilenode_skey, 0, sizeof(relfilenode_skey));

		for (int i = 0; i < 2; i++)
		{
			fmgr_info_cxt(F_OIDEQ,
			              &relfilenode_skey[i].sk_func,
			              CacheMemoryContext);
			relfilenode_skey[i].sk_strategy = BTEqualStrategyNumber;
			relfilenode_skey[i].sk_subtype = InvalidOid;
			relfilenode_skey[i].sk_collation = InvalidOid;
		}

		relfilenode_skey[0].sk_attno = Anum_pg_class_reltablespace;
		relfilenode_skey[1].sk_attno = Anum_pg_class_relfilenode;

		/* build the result HTAB */
		memset(&ctl, 0, sizeof(ctl));

		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(DiskQuotaSizeResultsEntry);
		ctl.hcxt = funcctx->multi_call_memory_ctx;
		ctl.hash = oid_hash;

		localResultsCacheTable = hash_create("disk quota Active Table Entry lookup hash table",
		                                     1024,
		                                     &ctl,
		                                     HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION);


		/* Read the SHM and using a local cache to store */
		localCacheTable = get_active_tables_shm(MyDatabaseId);
		hash_seq_init(&iter, localCacheTable);

		/* check for plain relations by looking in pg_class */
		relation = heap_open(RelationRelationId, AccessShareLock);

		/* Scan whole HTAB, get the Oid of each table and calculate the size of them */
		while ((shmCache_entry = (DiskQuotaActiveTableEntry *) hash_seq_search(&iter)) != NULL)
		{
			Size tablesize;
			bool found;
			ScanKeyData skey[2];

			/* set scan arguments */
			memcpy(skey, relfilenode_skey, sizeof(skey));
			skey[0].sk_argument = ObjectIdGetDatum(shmCache_entry->tablespaceoid);
			skey[1].sk_argument = ObjectIdGetDatum(shmCache_entry->relfilenode);

			relScan = systable_beginscan(relation,
			                             ClassTblspcRelfilenodeIndexId,
			                             true,
			                             NULL,
			                             2,
			                             skey);

			tuple = systable_getnext(relScan);

			if (!HeapTupleIsValid(tuple))
			{

				systable_endscan(relScan);

				/* tablespace oid may be 0 if the table is in default table space*/
				memcpy(skey, relfilenode_skey, sizeof(skey));
				skey[0].sk_argument = ObjectIdGetDatum(0);
				skey[1].sk_argument = ObjectIdGetDatum(shmCache_entry->relfilenode);

				relScan = systable_beginscan(relation,
				                             ClassTblspcRelfilenodeIndexId,
				                             true,
				                             NULL,
				                             2,
				                             skey);

				tuple = systable_getnext(relScan);

				if (!HeapTupleIsValid(tuple))
				{
					systable_endscan(relScan);
					continue;
				}

			}
			relOid = HeapTupleGetOid(tuple);

			/* Call function directly to get size of table by oid */
			tablesize = (Size) DatumGetInt64(DirectFunctionCall1(pg_total_relation_size, ObjectIdGetDatum(relOid)));

			systable_endscan(relScan);

			sizeResults_entry = (DiskQuotaSizeResultsEntry*) hash_search(localResultsCacheTable, &relOid, HASH_ENTER, &found);

			if (!found)
			{
				sizeResults_entry->dbid = MyDatabaseId;
				sizeResults_entry->tablesize = tablesize;
				sizeResults_entry->tableoid = relOid;
			}

		}

		heap_close(relation, AccessShareLock);

		/* total number of active tables to be returned, each tuple contains one active table stat */
		funcctx->max_calls = (uint32) hash_get_num_entries(localResultsCacheTable);

		/*
		 * prepare attribute metadata for next calls that generate the tuple
		 */

		tupdesc = CreateTemplateTupleDesc(3, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "TABLE_OID",
		                   OIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "DATABASE_ID",
		                   OIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "TABLE_SIZE",
		                   INT8OID, -1, 0);

		attinmeta = TupleDescGetAttInMetadata(tupdesc);
		funcctx->attinmeta = attinmeta;

		/* Prepare SetOf results HATB */
		cache = (DiskQuotaSetOFCache *) palloc(sizeof(DiskQuotaSetOFCache));
		cache->result = localResultsCacheTable;
		hash_seq_init(&(cache->pos), localResultsCacheTable);

		/* clean the local cache table */
		hash_destroy(localCacheTable);

		MemoryContextSwitchTo(oldcontext);
	} else {
		isFirstCall = false;
	}

	funcctx = SRF_PERCALL_SETUP();

	attinmeta = funcctx->attinmeta;

	if (isFirstCall) {
		funcctx->user_fctx = (void *) cache;
	} else {
		cache = (DiskQuotaSetOFCache *) funcctx->user_fctx;
	}

	/* return the results back to SPI caller */
	while ((results_entry = (DiskQuotaSizeResultsEntry *) hash_seq_search(&(cache->pos))) != NULL)
	{
		Datum result;
		Datum values[3];
		bool nulls[3];
		HeapTuple	tuple;

		memset(values, 0, sizeof(values));
		memset(nulls, false, sizeof(nulls));

		values[0] = ObjectIdGetDatum(results_entry->tableoid);
		values[1] = ObjectIdGetDatum(results_entry->dbid);
		values[2] = Int64GetDatum(results_entry->tablesize);

		tuple = heap_form_tuple(funcctx->attinmeta->tupdesc, values, nulls);

		result = HeapTupleGetDatum(tuple);
		funcctx->call_cntr++;

		SRF_RETURN_NEXT(funcctx, result);
	}

	/* finished, do the clear staff */
	hash_destroy(cache->result);
	pfree(cache);
	SRF_RETURN_DONE(funcctx);
}

/**
 *  Consume hash table in SHM
 **/

static
HTAB* get_active_tables_shm(Oid databaseID)
{
	HASHCTL ctl;
	HTAB *localHashTable = NULL;
	HASH_SEQ_STATUS iter;
	DiskQuotaActiveTableEntry *shmCache_entry;
	bool found;

	int num = 0;

	memset(&ctl, 0, sizeof(ctl));

	ctl.keysize = sizeof(DiskQuotaActiveTableEntry);
	ctl.entrysize = sizeof(DiskQuotaActiveTableEntry);
	ctl.hcxt = CurrentMemoryContext;
	ctl.hash = tag_hash;

	localHashTable = hash_create("local active table map",
								1024,
								&ctl,
								HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION);

	LWLockAcquire(active_table_shm_lock->lock, LW_EXCLUSIVE);

	hash_seq_init(&iter, active_tables_map);

	while ((shmCache_entry = (DiskQuotaActiveTableEntry *) hash_seq_search(&iter)) != NULL)
	{
		bool  found;
		DiskQuotaActiveTableEntry *entry;

		if (shmCache_entry->dbid != databaseID)
		{
			continue;
		}

		/* Add the active table entry into local hash table*/
		entry = hash_search(localHashTable, shmCache_entry, HASH_ENTER, &found);
		*entry = *shmCache_entry;
		hash_search(active_tables_map, shmCache_entry, HASH_REMOVE, NULL);
		num++;
	}

	LWLockRelease(active_table_shm_lock->lock);

	elog(DEBUG1, "number of active tables = %d\n", num);

	return localHashTable;
}

/*
 *  Hook function in smgr to report the active table
 *  information and stroe them in active table shared memory
 *  diskquota worker will consuming these active tables and
 *  recalculate their file size to update diskquota model.
 */
static void
report_active_table_SmgrStat(SMgrRelation reln)
{
	DiskQuotaActiveTableEntry *entry;
	DiskQuotaActiveTableEntry item;
	bool found = false;

	if (prev_SmgrStat_hook)
		(*prev_SmgrStat_hook)(reln);

	item.dbid = reln->smgr_rnode.node.dbNode;
	item.relfilenode = reln->smgr_rnode.node.relNode;
	item.tablespaceoid = reln->smgr_rnode.node.spcNode;

	LWLockAcquire(active_table_shm_lock->lock, LW_EXCLUSIVE);
	entry = hash_search(active_tables_map, &item, HASH_ENTER_NULL, &found);
	if (entry && !found)
		*entry = item;
	LWLockRelease(active_table_shm_lock->lock);

	if (!found && entry == NULL) {
		/* We may miss the file size change of this relation at current refresh interval.*/
		ereport(WARNING, (errmsg("Share memory is not enough for active tables.")));
	}
}
