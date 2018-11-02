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

#include "access/htup_details.h"
#include "catalog/indexing.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"

#include "activetable.h"
#include "diskquota.h"

static SmgrStat_hook_type prev_SmgrStat_hook = NULL;

/* functions to refresh disk quota model*/
static void report_active_table_SmgrStat(SMgrRelation reln);

void
init_active_table_hook(void)
{
	prev_SmgrStat_hook = SmgrStat_hook;
	SmgrStat_hook = report_active_table_SmgrStat;
}

/*
 *
 */
HTAB* get_active_tables()
{
	HASHCTL ctl;
	HTAB *local_active_table_map = NULL;
	HTAB *local_active_table_stats_map = NULL;
	HASH_SEQ_STATUS iter;
	DiskQuotaActiveTableFileEntry *active_table_file_entry;
	DiskQuotaActiveTableEntry *active_table_entry;

	ScanKeyData relfilenode_skey[2];
	Relation relation;
	HeapTuple tuple;
	SysScanDesc relScan;
	Oid relOid;
	TupleDesc tupdesc;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(DiskQuotaActiveTableFileEntry);
	ctl.entrysize = sizeof(DiskQuotaActiveTableFileEntry);
	ctl.hcxt = CurrentMemoryContext;
	ctl.hash = tag_hash;

	local_active_table_map = hash_create("local active table map with relfilenode info",
								1024,
								&ctl,
								HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION);

	/* Move active table from shared memory to local active table map */
	LWLockAcquire(active_table_shm_lock->lock, LW_EXCLUSIVE);

	hash_seq_init(&iter, active_tables_map);

	while ((active_table_file_entry = (DiskQuotaActiveTableFileEntry *) hash_seq_search(&iter)) != NULL)
	{
		bool  found;
		DiskQuotaActiveTableFileEntry *entry;

		if (active_table_file_entry->dbid != MyDatabaseId)
		{
			continue;
		}

		/* Add the active table entry into local hash table*/
		entry = hash_search(local_active_table_map, active_table_file_entry, HASH_ENTER, &found);
		if (entry)
			*entry = *active_table_file_entry;
		hash_search(active_tables_map, active_table_file_entry, HASH_REMOVE, NULL);
	}

	LWLockRelease(active_table_shm_lock->lock);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(DiskQuotaActiveTableEntry);
	ctl.hcxt = CurrentMemoryContext;
	ctl.hash = oid_hash;

	local_active_table_stats_map = hash_create("local active table map with relfilenode info",
								1024,
								&ctl,
								HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION);

	/* traverse local active table map and calculate their file size. */
	hash_seq_init(&iter, local_active_table_map);

	/* check for plain relations by looking in pg_class */
	relation = heap_open(RelationRelationId, AccessShareLock);

	/* Scan whole HTAB, get the Oid of each table and calculate the size of them */
	while ((active_table_entry = (DiskQuotaActiveTableEntry *) hash_seq_search(&iter)) != NULL)
	{
		Size tablesize;
		bool found;
		ScanKeyData skey[2];

		/* set scan arguments */
		memcpy(skey, relfilenode_skey, sizeof(skey));
		skey[0].sk_argument = ObjectIdGetDatum(active_table_file_entry->tablespaceoid);
		skey[1].sk_argument = ObjectIdGetDatum(active_table_file_entry->relfilenode);

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
			skey[1].sk_argument = ObjectIdGetDatum(active_table_file_entry->relfilenode);

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

		active_table_entry = hash_search(local_active_table_stats_map, &relOid, HASH_ENTER, &found);
		if (active_table_entry)
		{
			active_table_entry->tableoid = relOid;
			active_table_entry->tablesize = tablesize;
		}
		systable_endscan(relScan);
	}

	heap_close(relation, AccessShareLock);
	hash_destroy(local_active_table_map);

	return local_active_table_stats_map;
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
	DiskQuotaActiveTableFileEntry *entry;
	DiskQuotaActiveTableFileEntry item;
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
