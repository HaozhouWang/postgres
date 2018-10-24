/* -------------------------------------------------------------------------
 *
 * worker_spi.c
 *		Sample background worker code that demonstrates various coding
 *		patterns: establishing a database connection; starting and committing
 *		transactions; using GUC variables, and heeding SIGHUP to reread
 *		the configuration file; reporting to pg_stat_activity; using the
 *		process latch to sleep and exit in case of postmaster death.
 *
 * This code connects to a database, creates a schema and table, and summarizes
 * the numbers contained therein.  To see it working, insert an initial value
 * with "total" type and some initial value; then insert some other rows with
 * "delta" type.  Delta rows will be deleted by this worker and their values
 * aggregated into the total.
 *
 * Copyright (C) 2013, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/worker_spi/worker_spi.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

/* These are always necessary for a bgworker */
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/reloptions.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_database.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

/* these headers are used by this particular worker's code */
#include "commands/dbcommands.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "pgstat.h"
#include "utils/builtins.h"
//#include "utils/dbsize.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/ps_status.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timeout.h"
#include "utils/timestamp.h"
#include "utils/tqual.h"
#include "utils/builtins.h"
#include "tcop/utility.h"
#include "executor/executor.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(set_schema_quota_limit);
PG_FUNCTION_INFO_V1(set_role_quota_limit);


/* cluster level max size of black list */
#define MAX_DISK_QUOTA_BLACK_ENTRIES 8192 * 1024
/* cluster level init size of black list */
#define INIT_DISK_QUOTA_BLACK_ENTRIES 8192
/* per database level max size of black list */
#define MAX_LOCAL_DISK_QUOTA_BLACK_ENTRIES 8192
/* max number of disk quota worker process */
#define NUM_WORKITEMS			10
/* initial active table size */
#define INIT_ACTIVE_TABLE_SIZE	64


void		_PG_init(void);

void		disk_quota_worker_spi_main(Datum);
void		disk_quota_launcher_spi_main(Datum);

/* flags set by signal handlers */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

/* GUC variables */
static int	worker_spi_naptime = 10;
static int	worker_spi_total_workers = 2;
static char *worker_spi_monitored_database_list = NULL;
/* max number of active tables monitored by disk-quota */
static int worker_spi_max_active_tables;


typedef struct worktable
{
	const char *schema;
	const char *name;
} worktable;



/* Memory context for long-lived data */
//static MemoryContext diskquotaMemCxt;

typedef struct TableSizeEntry TableSizeEntry;
typedef struct NamespaceSizeEntry NamespaceSizeEntry;
typedef struct RoleSizeEntry RoleSizeEntry;
typedef struct QuotaLimitEntry QuotaLimitEntry;
typedef struct BlackMapEntry BlackMapEntry;
typedef struct LocalBlackMapEntry LocalBlackMapEntry;
typedef struct DiskQuotaWorkerEntry DiskQuotaWorkerEntry;

/* disk quota worker info used by launcher to manage the worker processes. */
struct DiskQuotaWorkerEntry
{
	char dbname[NAMEDATALEN];
	BackgroundWorkerHandle *handle;
};

/* local cache of table disk size and corresponding schema and owner */
struct TableSizeEntry
{
	Oid			reloid;
	Oid			namespaceoid;
	Oid			owneroid;
	int64		totalsize;
};

/* local cache of namespace disk size */
struct NamespaceSizeEntry
{
	Oid			namespaceoid;
	int64		totalsize;
};

/* local cache of role disk size */
struct RoleSizeEntry
{
	Oid			owneroid;
	int64		totalsize;
};

/* local cache of disk quota limit */
struct QuotaLimitEntry
{
	Oid			targetoid;
	int64		limitsize;
};

/* global blacklist for which exceed their quota limit */
struct BlackMapEntry
{
	Oid 		targetoid;
	Oid		databaseoid;
};

// active table entry in shm, one HTAB per segment
struct ActiveTableEntry
{
	Oid	reloid;
};
typedef struct ActiveTableEntry ActiveTableEntry;

/* local blacklist for which exceed their quota limit */
struct LocalBlackMapEntry
{
	Oid 		targetoid;
	bool		isexceeded;
};

/*
 * Get active table list to check their size
 */
static HTAB *pgstat_table_map = NULL;
static HTAB *pgstat_active_table_map = NULL;
static HTAB *active_tables_map = NULL;
static HTAB *disk_quota_worker_map = NULL;

/* Cache to detect the active table list */
typedef struct DiskQuotaLocalTableCache
{
	Oid			tableid;
	PgStat_Counter tuples_inserted;
	PgStat_Counter tuples_updated;
	PgStat_Counter tuples_deleted;
	PgStat_Counter vacuum_count;
	PgStat_Counter autovac_vacuum_count;
	PgStat_Counter tuples_living;
} DiskQuotaLocalTableCache;

/* struct to describe the active table */
typedef struct DiskQuotaActiveHashEntry
{
	Oid			reloid;
	PgStat_Counter t_refcount; /* TODO: using refcount for active queue */
} DiskQuotaActiveHashEntry;


/*
 * disk_quota_table_stat entry: store the last checked results of table status
 */
typedef struct DiskQuotaStateHashEntry
{
	Oid			reloid;
	DiskQuotaLocalTableCache t_entry;
} DiskQuotaStatHashEntry;

/* using hash table to support incremental update the table size entry.*/
static HTAB *table_size_map = NULL;
static HTAB *namespace_size_map = NULL;
static HTAB *role_size_map = NULL;
static HTAB *quota_limit_map = NULL;

/* black list for database objects which exceed their quota limit */
static HTAB *disk_quota_black_map = NULL;
static HTAB *local_disk_quota_black_map = NULL;

typedef struct
{
	LWLock	   *lock;		/* protects shared memory of blackMap */
} disk_quota_shared_state;
static disk_quota_shared_state *shared;

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static void init_disk_quota_shmem(void);
static void init_shm_worker_active_tables(void);
static void init_disk_quota_model(void);
static void refresh_disk_quota_model(void);
static void calculate_table_disk_usage(void);
static void calculate_schema_disk_usage(void);
static void calculate_role_disk_usage(void);
static void flush_local_black_map(void);
static void reset_local_black_map(void);
static void check_disk_quota_by_oid(Oid targetOid, int64 current_usage);
//static void get_rel_owner_schema(Oid relid, Oid *ownerOid, Oid *nsOid);
static void update_namespace_map(Oid namespaceoid, int64 updatesize);
static void update_role_map(Oid owneroid, int64 updatesize);
static void remove_namespace_map(Oid namespaceoid);
static void remove_role_map(Oid owneroid);
static bool check_table_is_active(Oid reloid);
static void build_active_table_map(void);
static int64 calculate_total_relation_size_by_oid(Oid reloid);
static void load_quotas(void);


static Size DiskQuotaShmemSize(void);
static void disk_quota_shmem_startup(void);
static int start_worker(char* dbname);

static List *get_database_list(void);
static void refresh_wokrer_list(void);

/* enforcement */
static void init_quota_enforcement(void);
static bool quota_check_ExecCheckRTPerms(List *rangeTable, bool ereport_on_violation);
static void get_rel_owner_schema(Oid relid, Oid *ownerOid, Oid *nsOid);
static ExecutorCheckPerms_hook_type prev_ExecutorCheckPerms_hook;
static BufferExtendCheckPerms_hook_type prev_BufferExtendCheckPerms_hook;

static bool quota_check_ReadBufferExtendCheckPerms(Oid reloid, BlockNumber blockNum);
static bool quota_check_common(Oid reloid);

static void set_quota_limit_internal(Oid targetoid, int64 quota_limit_mb);
static int64 get_size_in_mb(char *str);
/*
 * Entrypoint of this module.
 *
 * We register more than one worker process here, to demonstrate how that can
 * be done.
 */
void
_PG_init(void)
{
	BackgroundWorker worker;

	init_disk_quota_shmem();
	init_quota_enforcement();

	/* get the configuration */
	DefineCustomIntVariable("worker_spi.naptime",
							"Duration between each check (in seconds).",
							NULL,
							&worker_spi_naptime,
							10,
							1,
							INT_MAX,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);

	if (!process_shared_preload_libraries_in_progress)
		return;

	DefineCustomIntVariable("worker_spi.total_workers",
							"Number of workers.",
							NULL,
							&worker_spi_total_workers,
							1,
							1,
							100,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomStringVariable("worker_spi.monitor_databases",
								gettext_noop("database list with disk quota monitored."),
								NULL,
								&worker_spi_monitored_database_list,
								"postgres",
								PGC_SIGHUP, GUC_LIST_INPUT,
								NULL,
								NULL,
								NULL);

	DefineCustomIntVariable("worker_spi.max_active_tables",
							"max number of active tables monitored by disk-quota",
							NULL,
							&worker_spi_max_active_tables,
							8 * 1024,
							1,
							INT_MAX,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);

	/* set up common data for all our workers */
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_main = disk_quota_launcher_spi_main;
	worker.bgw_notify_pid = 0;

	snprintf(worker.bgw_name, BGW_MAXLEN, "disk quota launcher");

	RegisterBackgroundWorker(&worker);
}

/*
 * Signal handler for SIGTERM
 *		Set a flag to let the main loop to terminate, and set our latch to wake
 *		it up.
 */
static void
worker_spi_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigterm = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

/*
 * Signal handler for SIGHUP
 *		Set a flag to tell the main loop to reread the config file, and set
 *		our latch to wake it up.
 */
static void
worker_spi_sighup(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sighup = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

/*
 * generate the new shared blacklist from the localblack list which
 * exceed the quota limit.
 * */
static void
flush_local_black_map(void)
{
	HASH_SEQ_STATUS iter;
	LocalBlackMapEntry* localblackentry;
	BlackMapEntry* blackentry;
	bool found;

	LWLockAcquire(shared->lock, LW_EXCLUSIVE);

	hash_seq_init(&iter, local_disk_quota_black_map);
	while ((localblackentry = hash_seq_search(&iter)) != NULL)
	{
		if (localblackentry->isexceeded)
		{
			blackentry = (BlackMapEntry*) hash_search(disk_quota_black_map,
							   (void *) &localblackentry->targetoid,
							   HASH_ENTER_NULL, &found);
			if (blackentry == NULL)
			{
				elog(WARNING, "shared disk quota black map size limit reached.");
			}
			else
			{
				/* new db objects which exceed quota limit */
				if (!found)
				{
					blackentry->targetoid = localblackentry->targetoid;
					blackentry->databaseoid = MyDatabaseId;
				}
			}
		}
		else
		{
			/* db objects are removed or under quota limit in the new loop */
			(void) hash_search(disk_quota_black_map,
							   (void *) &localblackentry->targetoid,
							   HASH_REMOVE, NULL);
		}
	}
	LWLockRelease(shared->lock);
}

/* fetch the new blacklist from shared blacklist at each refresh iteration. */
static void
reset_local_black_map(void)
{
	HASH_SEQ_STATUS iter;
	LocalBlackMapEntry* localblackentry;
	BlackMapEntry* blackentry;
	bool found;
	/* clear entries in local black map*/
	hash_seq_init(&iter, local_disk_quota_black_map);

	while ((localblackentry = hash_seq_search(&iter)) != NULL)
	{
		(void) hash_search(local_disk_quota_black_map,
				(void *) &localblackentry->targetoid,
				HASH_REMOVE, NULL);
	}

	/* get black map copy from shared black map */
	LWLockAcquire(shared->lock, LW_SHARED);
	hash_seq_init(&iter, disk_quota_black_map);
	while ((blackentry = hash_seq_search(&iter)) != NULL)
	{
		/* only reset entries for current db */
		if (blackentry->databaseoid == MyDatabaseId)
		{
			localblackentry = (LocalBlackMapEntry*) hash_search(local_disk_quota_black_map,
								(void *) &blackentry->targetoid,
								HASH_ENTER, &found);
			if (!found)
			{
				localblackentry->targetoid = blackentry->targetoid;
				localblackentry->isexceeded = false;
			}
		}
	}
	LWLockRelease(shared->lock);

}

/*
 * Compare the disk quota limit and current usage of a database object.
 * Put them into local blacklist if quota limit is exceeded.
 */
static void check_disk_quota_by_oid(Oid targetOid, int64 current_usage)
{
	bool					found;
	int32 					quota_limit_mb;
	int32 					current_usage_mb;
	LocalBlackMapEntry*	localblackentry;

	QuotaLimitEntry* quota_entry;
	quota_entry = (QuotaLimitEntry *)hash_search(quota_limit_map,
											&targetOid,
											HASH_FIND, &found);
	if (!found)
	{
		/* default no limit */
		return;
	}

	quota_limit_mb = quota_entry->limitsize;
	current_usage_mb = current_usage / (1024 *1024);
	if(current_usage_mb > quota_limit_mb)
	{
		elog(LOG,"Put object %u to blacklist with quota limit:%d, current usage:%d",
				targetOid, quota_limit_mb, current_usage_mb);
		localblackentry = (LocalBlackMapEntry*) hash_search(local_disk_quota_black_map,
					&targetOid,
					HASH_ENTER, &found);
		localblackentry->isexceeded = true;
	}

}

static void
remove_namespace_map(Oid namespaceoid)
{
	hash_search(namespace_size_map,
			&namespaceoid,
			HASH_REMOVE, NULL);
}

static void
update_namespace_map(Oid namespaceoid, int64 updatesize)
{
	bool found;
	NamespaceSizeEntry* nsentry;
	nsentry = (NamespaceSizeEntry *)hash_search(namespace_size_map,
			&namespaceoid,
			HASH_ENTER, &found);
	if (!found)
	{
		nsentry->namespaceoid = namespaceoid;
		nsentry->totalsize = updatesize;
	}
	else {
		nsentry->totalsize += updatesize;
	}

}

static void
remove_role_map(Oid owneroid)
{
	hash_search(role_size_map,
			&owneroid,
			HASH_REMOVE, NULL);
}

static void
update_role_map(Oid owneroid, int64 updatesize)
{
	bool found;
	RoleSizeEntry* rolentry;
	rolentry = (RoleSizeEntry *)hash_search(role_size_map,
			&owneroid,
			HASH_ENTER, &found);
	if (!found)
	{
		rolentry->owneroid = owneroid;
		rolentry->totalsize = updatesize;
	}
	else {
		rolentry->totalsize += updatesize;
	}

}

static void
add_to_pgstat_map(Oid relOid)
{
	DiskQuotaStatHashEntry *entry;
	bool found;

	entry = hash_search(pgstat_table_map, &relOid, HASH_ENTER, &found);

	if (!found)
	{
		memset(&entry->t_entry, 0, sizeof(entry->t_entry));
	}
}

static void
remove_pgstat_map(Oid relOid)
{
	hash_search(pgstat_table_map, &relOid, HASH_REMOVE, NULL);
}

static int64 calculate_total_relation_size_by_oid(Oid reloid)
{
	int ret;
	StringInfoData buf;

	initStringInfo(&buf);
	appendStringInfo(&buf, "select pg_total_relation_size(%u);", reloid);

	ret = SPI_execute(buf.data, false, 0);

	if (ret != SPI_OK_SELECT)
		elog(FATAL, "cannot get table size %u error code %d",reloid, ret);
	if (SPI_processed > 0)
	{
		bool		isnull;
		int64		val;

		val = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
										  SPI_tuptable->tupdesc,
										  1, &isnull));
		if (!isnull){
			return val;
		}
	}
	return 0;
}
/*
 *  Incremental way to update the disk quota of every database objects
 *  Recalculate the table's disk usage when it's a new table or be update.
 *  Detect the removed table if it's nolonger in pg_class.
 *  If change happens, no matter size change or owner change,
 *  update schemasizemap and rolesizemap correspondingly.
 *
 */
static void
calculate_table_disk_usage(void)
{
	bool found;
	Relation	classRel;
	HeapTuple	tuple;
	HeapScanDesc relScan;
	TableSizeEntry *tsentry;
	Oid			relOid;
	HASH_SEQ_STATUS iter;

	classRel = heap_open(RelationRelationId, AccessShareLock);
	relScan = heap_beginscan_catalog(classRel, 0, NULL);

	while ((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		Form_pg_class classForm = (Form_pg_class) GETSTRUCT(tuple);
		found = false;
		if (classForm->relkind != RELKIND_RELATION &&
			classForm->relkind != RELKIND_MATVIEW)
			continue;
		relOid = HeapTupleGetOid(tuple);

		/* ignore system table*/
		if(relOid < FirstNormalObjectId)
			continue;

		/* skip to recalculate the tables which are not in active list.*/

		tsentry = (TableSizeEntry *)hash_search(table_size_map,
							 &relOid,
							 HASH_ENTER, &found);
		/* namespace and owner may be changed since last check*/
		if (!found)
		{
			/* if it's a new table*/
			tsentry->reloid = relOid;
			tsentry->namespaceoid = classForm->relnamespace;
			tsentry->owneroid = classForm->relowner;
			tsentry->totalsize = calculate_total_relation_size_by_oid(relOid);
			elog(LOG, "table: %u, size: %ld", tsentry->reloid, (int64)tsentry->totalsize);
			update_namespace_map(tsentry->namespaceoid, tsentry->totalsize);
			update_role_map(tsentry->owneroid, tsentry->totalsize);
			/* add to pgstat_table_map hash map */
			add_to_pgstat_map(relOid);
		}
		else if (check_table_is_active(tsentry->reloid))
		{
			/* if table size is modified*/
			int64 oldtotalsize = tsentry->totalsize;
			tsentry->totalsize = calculate_total_relation_size_by_oid(relOid);
            elog(LOG, "table: %u, size: %ld", tsentry->reloid, (int64)tsentry->totalsize);

			update_namespace_map(tsentry->namespaceoid, tsentry->totalsize - oldtotalsize);
			update_role_map(tsentry->owneroid, tsentry->totalsize - oldtotalsize);
		}
		/* check the disk quota limit TODO only check the modified table */
		//TODO
		check_disk_quota_by_oid(tsentry->reloid, tsentry->totalsize);

		/* if schema change */
		if (tsentry->namespaceoid != classForm->relnamespace)
		{
			update_namespace_map(tsentry->namespaceoid, -1 * tsentry->totalsize);
			tsentry->namespaceoid = classForm->relnamespace;
			update_namespace_map(tsentry->namespaceoid, tsentry->totalsize);
		}
		/* if owner change*/
		if(tsentry->owneroid != classForm->relowner)
		{
			update_role_map(tsentry->owneroid, -1 * tsentry->totalsize);
			tsentry->owneroid = classForm->relowner;
			update_role_map(tsentry->owneroid, tsentry->totalsize);
		}
	}

	heap_endscan(relScan);
	heap_close(classRel, AccessShareLock);

	/* Process removed tables*/
	hash_seq_init(&iter, table_size_map);

	while ((tsentry = hash_seq_search(&iter)) != NULL)
	{
		/* check if namespace is already be deleted */
		tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(tsentry->reloid));
		if (!HeapTupleIsValid(tuple))
		{
			update_role_map(tsentry->owneroid, -1 * tsentry->totalsize);
			update_namespace_map(tsentry->namespaceoid, -1 * tsentry->totalsize);

			hash_search(table_size_map,
					&tsentry->reloid,
					HASH_REMOVE, NULL);
			remove_pgstat_map(tsentry->reloid);
			continue;
		}
		ReleaseSysCache(tuple);
	}
}

static void calculate_schema_disk_usage(void)
{
	HeapTuple	tuple;
	HASH_SEQ_STATUS iter;
	NamespaceSizeEntry* nsentry;
	hash_seq_init(&iter, namespace_size_map);

	while ((nsentry = hash_seq_search(&iter)) != NULL)
	{
		/* check if namespace is already be deleted */
		tuple = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(nsentry->namespaceoid));
		if (!HeapTupleIsValid(tuple))
		{
			remove_namespace_map(nsentry->namespaceoid);
			continue;
		}
		ReleaseSysCache(tuple);
		elog(DEBUG1, "check namespace:%u with usage:%ld", nsentry->namespaceoid, nsentry->totalsize);
		check_disk_quota_by_oid(nsentry->namespaceoid, nsentry->totalsize);
	}
}

static void calculate_role_disk_usage(void)
{
	HeapTuple	tuple;
	HASH_SEQ_STATUS iter;
	RoleSizeEntry* rolentry;
	hash_seq_init(&iter, role_size_map);

	while ((rolentry = hash_seq_search(&iter)) != NULL)
	{
		/* check if namespace is already be deleted */
		tuple = SearchSysCache1(AUTHOID, ObjectIdGetDatum(rolentry->owneroid));
		if (!HeapTupleIsValid(tuple))
		{
			remove_role_map(rolentry->owneroid);
			continue;
		}
		ReleaseSysCache(tuple);
		elog(DEBUG1, "check role:%u with usage:%ld", rolentry->owneroid, rolentry->totalsize);
		check_disk_quota_by_oid(rolentry->owneroid, rolentry->totalsize);
	}
}

static bool check_table_is_active(Oid reloid)
{
	bool found = false;
	hash_search(active_tables_map, &reloid, HASH_REMOVE, &found);
	if (found)
	{
		elog(DEBUG1,"table is active with oid:%u", reloid);
	}
    found = true;
	return found;
}

static void build_active_table_map(void)
{
	DiskQuotaStatHashEntry *hash_entry;
	HASH_SEQ_STATUS status;

	hash_seq_init(&status, pgstat_table_map);

	/* reset current pg_stat snapshot to get new data */
	pgstat_clear_snapshot();

	while ((hash_entry = (DiskQuotaStatHashEntry *) hash_seq_search(&status)) != NULL)
	{

		PgStat_StatTabEntry *stat_entry;

		stat_entry = pgstat_fetch_stat_tabentry(hash_entry->reloid);
		if (stat_entry == NULL) {
			continue;
		}

		if (stat_entry->tuples_inserted != hash_entry->t_entry.tuples_inserted ||
			stat_entry->tuples_updated != hash_entry->t_entry.tuples_updated ||
			stat_entry->tuples_deleted != hash_entry->t_entry.tuples_deleted ||
			stat_entry->autovac_vacuum_count !=  hash_entry->t_entry.autovac_vacuum_count ||
			stat_entry->vacuum_count !=  hash_entry->t_entry.vacuum_count ||
			stat_entry->n_live_tuples != hash_entry->t_entry.tuples_living)
		{
			/* Update the entry */
			hash_entry->t_entry.tuples_inserted = stat_entry->tuples_inserted;
			hash_entry->t_entry.tuples_updated = stat_entry->tuples_updated;
			hash_entry->t_entry.tuples_deleted = stat_entry->tuples_deleted;
			hash_entry->t_entry.autovac_vacuum_count = stat_entry->autovac_vacuum_count;
			hash_entry->t_entry.vacuum_count = stat_entry->vacuum_count;
			hash_entry->t_entry.tuples_living = stat_entry->n_live_tuples;

			/* Add this entry to active hash table if not exist */
			hash_search(pgstat_active_table_map, &hash_entry->reloid, HASH_ENTER, NULL);

		}
	}
}

/*
 * Scan file system, to update the model with all files.
 */
static void
refresh_disk_quota_model(void)
{
	reset_local_black_map();

	/* recalculate the disk usage of table, schema and role */

	calculate_table_disk_usage();
	calculate_schema_disk_usage();
	calculate_role_disk_usage();

	flush_local_black_map();
}

/*
 * Load quotas from configuration table.
 */
static void
load_quotas(void)
{
	int			ret;
	TupleDesc	tupdesc;
	int			i;
	bool		found;
	QuotaLimitEntry* quota_entry;

	RangeVar   *rv;
	Relation	rel;

	rv = makeRangeVar("diskquota", "quota_config", -1);
	rel = heap_openrv_extended(rv, AccessShareLock, true);
	if (!rel)
	{
		/* configuration table is missing. */
		elog(LOG, "configuration table \"pg_quota.quotas\" is missing in database \"%s\"",
			 get_database_name(MyDatabaseId));
		heap_close(rel, NoLock);
		return;
	}
	heap_close(rel, NoLock);

	ret = SPI_execute("select targetOid, quotalimitMB from diskquota.quota_config", true, 0);
	if (ret != SPI_OK_SELECT)
		elog(FATAL, "SPI_execute failed: error code %d", ret);

	tupdesc = SPI_tuptable->tupdesc;
	if (tupdesc->natts != 2 ||
		TupleDescAttr(tupdesc, 0)->atttypid != OIDOID ||
		TupleDescAttr(tupdesc, 1)->atttypid != INT8OID)
		elog(ERROR, "query must yield two columns, oid and int8");

	for (i = 0; i < SPI_processed; i++)
	{
		HeapTuple	tup = SPI_tuptable->vals[i];
		Datum		dat;
		Oid			targetOid;
		int64		quota_limit_mb;
		bool		isnull;

		dat = SPI_getbinval(tup, tupdesc, 1, &isnull);
		if (isnull)
			continue;
		targetOid = DatumGetObjectId(dat);

		dat = SPI_getbinval(tup, tupdesc, 2, &isnull);
		if (isnull)
			continue;
		quota_limit_mb = DatumGetInt64(dat);

		quota_entry = (QuotaLimitEntry *)hash_search(quota_limit_map,
												&targetOid,
												HASH_ENTER, &found);
		quota_entry->limitsize = quota_limit_mb;
	}

}



/*
 * DiskQuotaShmemSize
 *		Compute space needed for diskquota-related shared memory
 */
Size
DiskQuotaShmemSize(void)
{
	Size		size;

	size = MAXALIGN(sizeof(disk_quota_shared_state));
	size = add_size(size, hash_estimate_size(MAX_DISK_QUOTA_BLACK_ENTRIES, sizeof(BlackMapEntry)));
	size = add_size(size, hash_estimate_size(worker_spi_max_active_tables, sizeof(ActiveTableEntry)));
	return size;
}

/*
 * DiskQuotaShmemInit
 *		Allocate and initialize diskquota-related shared memory
 */
void
disk_quota_shmem_startup(void)
{
	bool		found;
	HASHCTL		hash_ctl;

	if (prev_shmem_startup_hook)
			prev_shmem_startup_hook();

	shared = NULL;
	disk_quota_black_map = NULL;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);


	shared = ShmemInitStruct("disk_quota",
								 sizeof(disk_quota_shared_state),
								 &found);
	if (!found)
	{
		//shared->lock = &(GetNamedLWLockTranche("disk_quota"))->lock;
		//TODO this is not correct. should using disk quota lock
		//need to add them to lwlock.h since named tranche not supported.
		shared->lock = LWLockAssign();
	}

	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(BlackMapEntry);
	hash_ctl.dsize = hash_ctl.max_dsize = hash_select_dirsize(MAX_DISK_QUOTA_BLACK_ENTRIES);
	disk_quota_black_map = ShmemInitHash("blackmap whose quota limitation is reached",
									INIT_DISK_QUOTA_BLACK_ENTRIES,
									MAX_DISK_QUOTA_BLACK_ENTRIES,
									&hash_ctl,
									HASH_DIRSIZE | HASH_SHARED_MEM | HASH_ALLOC | HASH_ELEM);

	init_shm_worker_active_tables();

	LWLockRelease(AddinShmemInitLock);
}

static void
init_disk_quota_shmem(void)
{
	/*
	 * Request additional shared resources.  (These are no-ops if we're not in
	 * the postmaster process.)  We'll allocate or attach to the shared
	 * resources in pgss_shmem_startup().
	 */
	RequestAddinShmemSpace(DiskQuotaShmemSize());
	//RequestNamedLWLockTranche("disk_quota", 1);

	/*
	 * Install startup hook to initialize our shared memory.
	 */
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = disk_quota_shmem_startup;
}


/*
 * init disk quota model when the worker process firstly started.
 */
void
init_disk_quota_model(void)
{
	HASHCTL		hash_ctl;
	MemoryContext DSModelContext;
	DSModelContext = AllocSetContextCreate(TopMemoryContext,
										   "Disk quotas model context",
										   ALLOCSET_DEFAULT_MINSIZE,
										   ALLOCSET_DEFAULT_INITSIZE,
										   ALLOCSET_DEFAULT_MAXSIZE);

	/* init hash table for table/schema/role etc.*/
	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(TableSizeEntry);
	hash_ctl.hcxt = DSModelContext;

	table_size_map = hash_create("TableSizeEntry map",
								1024,
								&hash_ctl,
								HASH_ELEM | HASH_CONTEXT);

	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(NamespaceSizeEntry);
	hash_ctl.hcxt = DSModelContext;

	namespace_size_map = hash_create("NamespaceSizeEntry map",
								1024,
								&hash_ctl,
								HASH_ELEM | HASH_CONTEXT);

	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(RoleSizeEntry);
	hash_ctl.hcxt = DSModelContext;

	role_size_map = hash_create("RoleSizeEntry map",
								1024,
								&hash_ctl,
								HASH_ELEM | HASH_CONTEXT);


	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(QuotaLimitEntry);
	hash_ctl.hcxt = DSModelContext;

	quota_limit_map = hash_create("QuotaLimitEntry map",
								1024,
								&hash_ctl,
								HASH_ELEM | HASH_CONTEXT);

	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(LocalBlackMapEntry);
	local_disk_quota_black_map = hash_create("local blackmap whose quota limitation is reached",
									MAX_LOCAL_DISK_QUOTA_BLACK_ENTRIES,
									&hash_ctl,
									HASH_ELEM | HASH_CONTEXT);
	if (pgstat_table_map == NULL)
	{
		HASHCTL ctl;

		memset(&ctl, 0, sizeof(ctl));

		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(DiskQuotaStatHashEntry);

		pgstat_table_map = hash_create("disk quota Table State Entry lookup hash table",
									NUM_WORKITEMS,
									&ctl,
									HASH_ELEM);
	}

	if (pgstat_active_table_map == NULL)
	{
		HASHCTL ctl;

		memset(&ctl, 0, sizeof(ctl));

		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(DiskQuotaActiveHashEntry);

		pgstat_active_table_map = hash_create("disk quota Active Table Entry lookup hash table",
									INIT_ACTIVE_TABLE_SIZE,
									&ctl,
									HASH_ELEM);
	}
}

static void
init_shm_worker_active_tables()
{
	HASHCTL ctl;
	memset(&ctl, 0, sizeof(ctl));
	

	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(ActiveTableEntry);

	elog(LOG, "max tables = %d\n", worker_spi_max_active_tables);

	active_tables_map = ShmemInitHash ("active_tables",
				worker_spi_max_active_tables,
				worker_spi_max_active_tables,
				&ctl,
				HASH_ELEM);
 
}

void
disk_quota_worker_spi_main(Datum main_arg)
{
	char *dbname=MyBgworkerEntry->bgw_name;
	elog(LOG,"start disk quota worker process to monitor database:%s", dbname);

	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, worker_spi_sighup);
	pqsignal(SIGTERM, worker_spi_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to our database */
	BackgroundWorkerInitializeConnection(dbname, NULL);


	init_disk_quota_model();

	/*
	 * Main loop: do this until the SIGTERM handler tells us to terminate
	 */
	while (!got_sigterm)
	{
		int			rc;

		StartTransactionCommand();
		SPI_connect();
		PushActiveSnapshot(GetTransactionSnapshot());
		load_quotas();
		build_active_table_map();
		refresh_disk_quota_model();
		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();

		/*
		 * Background workers mustn't call usleep() or any direct equivalent:
		 * instead, they may wait on their process latch, which sleeps as
		 * necessary, but is awakened if postmaster dies.  That way the
		 * background process goes away immediately in an emergency.
		 */
		rc = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   worker_spi_naptime * 1000L);
		ResetLatch(&MyProc->procLatch);

		/* emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		/*
		 * In case of a SIGHUP, just reload the configuration.
		 */
		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}
	}

	proc_exit(1);
}


/*
 * database list found in guc monitored_database_list
 */
static List *
get_database_list(void)
{
	List	   *dblist = NULL;
	if (!SplitIdentifierString(worker_spi_monitored_database_list, ',', &dblist))
	{
		elog(FATAL, "cann't get database list from guc:'%s'", worker_spi_monitored_database_list);
		return NULL;
	}
	return dblist;
}

static void
refresh_wokrer_list(void)
{
	List *monitor_dblist;
	List *removed_workerlist;
	ListCell *cell;
	ListCell *removed_workercell;
	bool flag = false;
	bool found;
	DiskQuotaWorkerEntry *hash_entry;
	HASH_SEQ_STATUS status;

	removed_workerlist = NIL;
	monitor_dblist = get_database_list();
	/*
	 * refresh the worker process based on the config change.
	 * step 1 is to terminate the worker not in monitor dblist.
	 */
	elog(LOG,"BEGIN to refresh monitor database list.");
	hash_seq_init(&status, disk_quota_worker_map);

	while ((hash_entry = (DiskQuotaWorkerEntry*) hash_seq_search(&status)) != NULL)
	{
		flag = false;
		foreach(cell, monitor_dblist)
		{
			char *db_name;

			db_name = (char *)lfirst(cell);
			if (db_name == NULL || *db_name == '\0')
			{
				continue;
			}
			 elog(LOG,"BEGIN to refresh monitor database list. curdb: %s:%s",db_name,hash_entry->dbname) ;
			if (strcmp(db_name, hash_entry->dbname) == 0 )
			{
				flag = true;
				break;
			}
		}
		if (!flag)
		{
			elog(LOG,"BEGIN to refresh monitor database list. removedb: %s", hash_entry->dbname);
			removed_workerlist = lappend(removed_workerlist, hash_entry->dbname);
		}
	}
	foreach(removed_workercell, removed_workerlist)
	{
		DiskQuotaWorkerEntry* workerentry;
		char *db_name;
		BackgroundWorkerHandle *handle;

		db_name = (char *)lfirst(removed_workercell);

		workerentry = (DiskQuotaWorkerEntry *)hash_search(disk_quota_worker_map,
					(void *)db_name,
					HASH_REMOVE, &found);
		if(found)
		{
			handle = workerentry->handle;
			TerminateBackgroundWorker(handle);
		}
	}

	/* step 2: start new worker which appears in monitor dblist. */
	foreach(cell, monitor_dblist)
	{
		char *db_name;

		db_name = (char *)lfirst(cell);
		if (db_name == NULL || *db_name == '\0')
		{
			continue;
		}
		hash_search(disk_quota_worker_map,
							(void *)db_name,
							HASH_FIND, &found);
		if(!found)
		{
			start_worker(db_name);
		}
	}
}


void
disk_quota_launcher_spi_main(Datum main_arg)
{
	List *dblist;
	ListCell *cell;
	HASHCTL		hash_ctl;


	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, worker_spi_sighup);
	pqsignal(SIGTERM, worker_spi_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to our database */
	BackgroundWorkerInitializeConnection("postgres", NULL);

	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = NAMEDATALEN;
	hash_ctl.entrysize = sizeof(DiskQuotaWorkerEntry);

	disk_quota_worker_map = hash_create("disk quota worker map",
										  1024,
										  &hash_ctl,
										  HASH_ELEM);

	dblist = get_database_list();

	foreach(cell, dblist)
	{
		char *db_name;

		db_name = (char *)lfirst(cell);
		if (db_name == NULL || *db_name == '\0')
		{
			elog(WARNING, "invalid db name='%s'", db_name);
			continue;
		}
		start_worker(db_name);
	}
	/*
	 * Main loop: do this until the SIGTERM handler tells us to terminate
	 */
	while (!got_sigterm)
	{
		int			rc;

		/*
		 * Background workers mustn't call usleep() or any direct equivalent:
		 * instead, they may wait on their process latch, which sleeps as
		 * necessary, but is awakened if postmaster dies.  That way the
		 * background process goes away immediately in an emergency.
		 */
		rc = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   worker_spi_naptime * 1000L);
		ResetLatch(&MyProc->procLatch);

		/* emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		/*
		 * In case of a SIGHUP, just reload the configuration.
		 */
		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
			/* terminate not monitored worker process and start new worker process*/
			refresh_wokrer_list();
		}

	}

	proc_exit(1);
}



/*
 * Dynamically launch an SPI worker.
 */
static int
start_worker(char* dbname)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status;
	pid_t		pid;
	bool found;
	DiskQuotaWorkerEntry* workerentry;

	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_main = NULL;		/* new worker might not have library loaded */
	sprintf(worker.bgw_library_name, "worker_spi");
	sprintf(worker.bgw_function_name, "disk_quota_worker_spi_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "%s", dbname);
	/* set bgw_notify_pid so that we can use WaitForBackgroundWorkerStartup */
	worker.bgw_notify_pid = MyProcPid;

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		return -1;

	status = WaitForBackgroundWorkerStartup(handle, &pid);

	if (status == BGWH_STOPPED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not start background process"),
			   errhint("More details may be available in the server log.")));
	if (status == BGWH_POSTMASTER_DIED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
			  errmsg("cannot start background processes without postmaster"),
				 errhint("Kill all remaining database processes and restart the database.")));
	Assert(status == BGWH_STARTED);

	/* put the worker handle into the worker map */
	workerentry = (DiskQuotaWorkerEntry *)hash_search(disk_quota_worker_map,
				(void *)dbname,
				HASH_ENTER, &found);
	if (!found)
	{
		workerentry->handle = handle;
	}

	return pid;
}

/*
 * Set disk quota limit for role.
 */
Datum
set_role_quota_limit(PG_FUNCTION_ARGS)
{
	Oid roleoid;
	char *rolname;
	char *sizestr;
	int64 quota_limit_mb;

	if (!superuser())
	{
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to set disk quota limit")));
	}

	rolname = text_to_cstring(PG_GETARG_TEXT_PP(0));
	roleoid = get_role_oid(rolname, false);
	
	sizestr = text_to_cstring(PG_GETARG_TEXT_PP(1));
	quota_limit_mb = get_size_in_mb(sizestr);

	set_quota_limit_internal(roleoid, quota_limit_mb);
	PG_RETURN_VOID();
}

/*
 * Set disk quota limit for schema.
 */
Datum
set_schema_quota_limit(PG_FUNCTION_ARGS)
{
	Oid namespaceoid;
	char *nspname;
	char *sizestr;
	int64 quota_limit_mb;
	if (!superuser())
	{
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to set disk quota limit")));
	}

	nspname = text_to_cstring(PG_GETARG_TEXT_PP(0));
	namespaceoid = get_namespace_oid(nspname, false);

	sizestr = text_to_cstring(PG_GETARG_TEXT_PP(1));
	quota_limit_mb = get_size_in_mb(sizestr);

	set_quota_limit_internal(namespaceoid, quota_limit_mb);
	PG_RETURN_VOID();
}

/*
 * Write the quota limit info into quota_config table under
 * diskquota namespace of the database.
 */
static void
set_quota_limit_internal(Oid targetoid, int64 quota_limit_mb)
{
	int ret;
	StringInfoData buf;
	
	initStringInfo(&buf);
	appendStringInfo(&buf,
					"insert into diskquota.quota_config values(%u,%ld);",
					targetoid, quota_limit_mb);

	SPI_connect();

	/* We can now execute queries via SPI */
	ret = SPI_execute(buf.data, false, 0);

	if (ret != SPI_OK_INSERT)
		elog(ERROR, "cannot insert into quota setting table, error code %d", ret);

	/*
	 * And finish our transaction.
	 */
	SPI_finish();
	return;
}

/*
 * Convert a human-readable size to a size in MB.
 */
static int64
get_size_in_mb(char *str)
{
	char	   *strptr,
			   *endptr;
	char		saved_char;
	Numeric		num;
	int64		result;
	bool		have_digits = false;

	/* Skip leading whitespace */
	strptr = str;
	while (isspace((unsigned char) *strptr))
		strptr++;

	/* Check that we have a valid number and determine where it ends */
	endptr = strptr;

	/* Part (1): sign */
	if (*endptr == '-' || *endptr == '+')
		endptr++;

	/* Part (2): main digit string */
	if (isdigit((unsigned char) *endptr))
	{
		have_digits = true;
		do
			endptr++;
		while (isdigit((unsigned char) *endptr));
	}

	/* Part (3): optional decimal point and fractional digits */
	if (*endptr == '.')
	{
		endptr++;
		if (isdigit((unsigned char) *endptr))
		{
			have_digits = true;
			do
				endptr++;
			while (isdigit((unsigned char) *endptr));
		}
	}

	/* Complain if we don't have a valid number at this point */
	if (!have_digits)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid size: \"%s\"", str)));

	/* Part (4): optional exponent */
	if (*endptr == 'e' || *endptr == 'E')
	{
		long		exponent;
		char	   *cp;

		/*
		 * Note we might one day support EB units, so if what follows 'E'
		 * isn't a number, just treat it all as a unit to be parsed.
		 */
		exponent = strtol(endptr + 1, &cp, 10);
		(void) exponent;		/* Silence -Wunused-result warnings */
		if (cp > endptr + 1)
			endptr = cp;
	}

	/*
	 * Parse the number, saving the next character, which may be the first
	 * character of the unit string.
	 */
	saved_char = *endptr;
	*endptr = '\0';

	num = DatumGetNumeric(DirectFunctionCall3(numeric_in,
											  CStringGetDatum(strptr),
											  ObjectIdGetDatum(InvalidOid),
											  Int32GetDatum(-1)));

	*endptr = saved_char;

	/* Skip whitespace between number and unit */
	strptr = endptr;
	while (isspace((unsigned char) *strptr))
		strptr++;

	/* Handle possible unit */
	if (*strptr != '\0')
	{
		int64		multiplier = 0;

		/* Trim any trailing whitespace */
		endptr = str + strlen(str) - 1;

		while (isspace((unsigned char) *endptr))
			endptr--;

		endptr++;
		*endptr = '\0';

		/* Parse the unit case-insensitively */
		if (pg_strcasecmp(strptr, "mb") == 0)
			multiplier = ((int64) 1);

		else if (pg_strcasecmp(strptr, "gb") == 0)
			multiplier = ((int64) 1024);

		else if (pg_strcasecmp(strptr, "tb") == 0)
			multiplier = ((int64) 1024) * 1024 ;

		else
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid size: \"%s\"", str),
					 errdetail("Invalid size unit: \"%s\".", strptr),
					 errhint("Valid units are \"MB\", \"GB\", and \"TB\".")));

		if (multiplier > 1)
		{
			Numeric		mul_num;

			mul_num = DatumGetNumeric(DirectFunctionCall1(int8_numeric,
														  Int64GetDatum(multiplier)));

			num = DatumGetNumeric(DirectFunctionCall2(numeric_mul,
													  NumericGetDatum(mul_num),
													  NumericGetDatum(num)));
		}
	}

	result = DatumGetInt64(DirectFunctionCall1(numeric_int8,
											   NumericGetDatum(num)));

	return result;
}

/* enforcement */
/*
 * Initialize enforcement, by installing the executor permission hook.
 */
static void
init_quota_enforcement(void)
{
	/* enforcement hook before query is loading data */
	prev_ExecutorCheckPerms_hook = ExecutorCheckPerms_hook;
	ExecutorCheckPerms_hook = quota_check_ExecCheckRTPerms;

	/* enforcement hook during query is loading data*/
	prev_BufferExtendCheckPerms_hook = BufferExtendCheckPerms_hook;
	BufferExtendCheckPerms_hook = quota_check_ReadBufferExtendCheckPerms;
}


static void
get_rel_owner_schema(Oid relid, Oid *ownerOid, Oid *nsOid)
{
	HeapTuple	tp;

	tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (HeapTupleIsValid(tp))
	{
		Form_pg_class reltup = (Form_pg_class) GETSTRUCT(tp);
		*ownerOid = reltup->relowner;
		*nsOid = reltup->relnamespace;
		ReleaseSysCache(tp);
		return ;
	}
	else
	{
		elog(DEBUG1, "could not find owner for relation %u", relid);
		return;
	}
}


static bool
quota_check_common(Oid reloid)
{
	Oid ownerOid = InvalidOid;
	Oid nsOid = InvalidOid;
	bool found;

	get_rel_owner_schema(reloid, &ownerOid, &nsOid);
	LWLockAcquire(shared->lock, LW_SHARED);
	hash_search(disk_quota_black_map,
				&reloid,
				HASH_FIND, &found);
	if (found)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DISK_FULL),
				 errmsg("table's disk space quota exceeded")));
		return false;
	}

	if ( nsOid != InvalidOid)
	{
		hash_search(disk_quota_black_map,
				&nsOid,
				HASH_FIND, &found);
		if (found)
		{
			ereport(ERROR,
					(errcode(ERRCODE_DISK_FULL),
					 errmsg("schema's disk space quota exceeded")));
			return false;
		}

	}

	if ( ownerOid != InvalidOid)
	{
		hash_search(disk_quota_black_map,
				&ownerOid,
				HASH_FIND, &found);
		if (found)
		{
			ereport(ERROR,
					(errcode(ERRCODE_DISK_FULL),
					 errmsg("role's disk space quota exceeded")));
			return false;
		}
	}
	LWLockRelease(shared->lock);
	return true;
}
/*
 * Permission check hook function. Throws an error if you try to INSERT
 * (or COPY) into a table, and the quota has been exceeded.
 */
static bool
quota_check_ExecCheckRTPerms(List *rangeTable, bool ereport_on_violation)
{
	ListCell   *l;

	foreach(l, rangeTable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(l);

		/* see ExecCheckRTEPerms() */
		if (rte->rtekind != RTE_RELATION)
			continue;

		/*
		 * Only check quota on inserts. UPDATEs may well increase
		 * space usage too, but we ignore that for now.
		 */
		if ((rte->requiredPerms & ACL_INSERT) == 0 && (rte->requiredPerms & ACL_UPDATE) == 0)
			continue;

		/* Perform the check as the relation's owner and namespace */
		quota_check_common(rte->relid);

	}

	return true;
}

static bool
quota_check_ReadBufferExtendCheckPerms(Oid reloid, BlockNumber blockNum)
{
	bool isExtend;

	isExtend = (blockNum == P_NEW);
	/* if not buffer extend, we could skip quota limit check*/
	if (!isExtend)
	{
		return true;
	}

	/* Perform the check as the relation's owner and namespace */
	quota_check_common(reloid);
	return true;
}
