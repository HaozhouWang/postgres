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
#include "utils/varlena.h"
#include "tcop/utility.h"
#include "executor/executor.h"
#include "storage/smgr.h"
#include "funcapi.h"

#include "diskquota.h"
PG_MODULE_MAGIC;

/*****************************************
*
*  DISK QUOTA HELPER FUNCTIONS
*
******************************************/


static bool quota_check_ExecCheckRTPerms(List *rangeTable, bool ereport_on_violation);
static ExecutorCheckPerms_hook_type prev_ExecutorCheckPerms_hook;
static BufferExtendCheckPerms_hook_type prev_BufferExtendCheckPerms_hook;

static bool quota_check_ReadBufferExtendCheckPerms(Oid reloid, BlockNumber blockNum);

/* enforcement */
/*
 * Initialize enforcement, by installing the executor permission hook.
 */
void
init_disk_quota_enforcement(void)
{
    /* enforcement hook before query is loading data */
    prev_ExecutorCheckPerms_hook = ExecutorCheckPerms_hook;
    ExecutorCheckPerms_hook = quota_check_ExecCheckRTPerms;

    /* enforcement hook during query is loading data*/
    prev_BufferExtendCheckPerms_hook = BufferExtendCheckPerms_hook;
    BufferExtendCheckPerms_hook = quota_check_ReadBufferExtendCheckPerms;
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



