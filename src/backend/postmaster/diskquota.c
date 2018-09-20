/*-------------------------------------------------------------------------
 *
 * diskquota.c
 *
 * PostgreSQL Integrated vacuum Daemon
 *
 * The diskquota system is structured in two different kinds of processes: the
 * diskquota launcher and the diskquota worker.  The launcher is an
 * always-running process, started by the postmaster when the diskquota GUC
 * parameter is set.  The launcher schedules diskquota workers to be started
 * when appropriate.  The workers are the processes which execute the actual
 * vacuuming; they connect to a database as determined in the launcher, and
 * once connected they examine the catalogs to select the tables to vacuum.
 *
 * The diskquota launcher cannot start the worker processes by itself,
 * because doing so would cause robustness issues (namely, failure to shut
 * them down on exceptional conditions, and also, since the launcher is
 * connected to shared memory and is thus subject to corruption there, it is
 * not as robust as the postmaster).  So it leaves that task to the postmaster.
 *
 * There is an diskquota shared memory area, where the launcher stores
 * information about the database it wants vacuumed.  When it wants a new
 * worker to start, it sets a flag in shared memory and sends a signal to the
 * postmaster.  Then postmaster knows nothing more than it must start a worker;
 * so it forks a new child, which turns into a worker.  This new process
 * connects to shared memory, and there it can inspect the information that the
 * launcher has set up.
 *
 * If the fork() call fails in the postmaster, it sets a flag in the shared
 * memory area, and sends a signal to the launcher.  The launcher, upon
 * noticing the flag, can try starting the worker again by resending the
 * signal.  Note that the failure can only be transient (fork failure due to
 * high load, memory pressure, too many processes, etc); more permanent
 * problems, like failure to connect to a database, are detected later in the
 * worker and dealt with just by having the worker exit normally.  The launcher
 * will launch a new worker again later, per schedule.
 *
 * When the worker is done vacuuming it sends SIGUSR2 to the launcher.  The
 * launcher then wakes up and is able to launch another worker, if the schedule
 * is so tight that a new worker is needed immediately.  At this time the
 * launcher can also balance the settings for the various remaining workers'
 * cost-based vacuum delay feature.
 *
 * Note that there can be more than one worker in a database concurrently.
 * They will store the table they are currently vacuuming in shared memory, so
 * that other workers avoid being blocked waiting for the vacuum lock for that
 * table.  They will also reload the pgstats data just before vacuuming each
 * table, to avoid vacuuming a table that was just finished being vacuumed by
 * another worker and thus is no longer noted in shared memory.  However,
 * there is a window (caused by pgstat delay) on which a worker may choose a
 * table that was already vacuumed; this is a bug in the current design.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/diskquota.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <sys/time.h>
#include <unistd.h>

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/reloptions.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_database.h"
#include "catalog/pg_diskquota.h"
#include "catalog/pg_tablespace.h"
#include "commands/dbcommands.h"
#include "commands/vacuum.h"
#include "executor/executor.h"
#include "lib/ilist.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "pgstat.h"
#include "postmaster/diskquota.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lmgr.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/sinvaladt.h"
#include "storage/smgr.h"
#include "tcop/tcopprot.h"
#include "utils/dbsize.h"
#include "utils/fmgroids.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timeout.h"
#include "utils/timestamp.h"
#include "utils/tqual.h"
#include "utils/varlena.h"


/*
 * GUC parameters
 */
char *guc_dq_database_list = NULL;
bool		diskquota_start_daemon = true;
int			diskquota_max_workers;

/* cluster level max size of black list */
#define MAX_DISK_QUOTA_BLACK_ENTRIES 8192 * 1024
/* cluster level init size of black list */
#define INIT_DISK_QUOTA_BLACK_ENTRIES 8192
/* per database level max size of black list */
#define MAX_LOCAL_DISK_QUOTA_BLACK_ENTRIES 8192

/* Flags to tell if we are in an diskquota process */
static bool am_diskquota_launcher = false;
static bool am_diskquota_worker = false;

/* Flags set by signal handlers */
static volatile sig_atomic_t got_SIGHUP = false;
static volatile sig_atomic_t got_SIGUSR2 = false;
static volatile sig_atomic_t got_SIGTERM = false;

/* Memory context for long-lived data */
static MemoryContext diskquotaMemCxt;

typedef struct TableSizeEntry TableSizeEntry;
typedef struct NamespaceSizeEntry NamespaceSizeEntry;
typedef struct RoleSizeEntry RoleSizeEntry;
typedef struct BlackMapEntry BlackMapEntry;
typedef struct LocalBlackMapEntry LocalBlackMapEntry;

struct TableSizeEntry
{
	Oid			reloid;
	Oid			namespaceoid;
	Oid			owneroid;
	int64		totalsize;
};

struct NamespaceSizeEntry
{
	Oid			namespaceoid;
	int64		totalsize;
};

struct RoleSizeEntry
{
	Oid			owneroid;
	int64		totalsize;
};

struct BlackMapEntry
{
	Oid 		targetoid;
	Oid			databaseoid;
};

struct LocalBlackMapEntry
{
	Oid 		targetoid;
	bool		isexceeded;
};

/*
 * Hash table for O(1) t_id -> tsa_entry lookup
 */
static HTAB *pgstat_table_map = NULL;
static HTAB *pgstat_active_table_map = NULL;

typedef struct DiskQuotaLocalTableCache
{
	Oid			tableid;
	PgStat_Counter tuples_inserted;
	PgStat_Counter tuples_updated;
	PgStat_Counter tuples_deleted;
	PgStat_Counter vacuum_count;
	PgStat_Counter autovac_vacuum_count;

} DiskQuotaLocalTableCache;

typedef struct DiskQuotaActiveHashEntry
{
	Oid			t_id;
	PgStat_Counter t_refcount;
} DiskQuotaActiveHashEntry;


/*
 * pgstat_table_map entry: map from relation OID to PgStat_TableStatus pointer
 */
typedef struct DiskQuotaStateHashEntry
{
	Oid			t_id;
	DiskQuotaLocalTableCache t_entry;
} DiskQuotaStatHashEntry;

/* using hash table to support incremental update the table size entry.*/
static HTAB *table_size_map = NULL;
static HTAB *namespace_size_map = NULL;
static HTAB *role_size_map = NULL;

static HTAB *disk_quota_black_map = NULL;
static HTAB *local_disk_quota_black_map = NULL;
/*
 * Possible signals received by the launcher from remote processes.  These are
 * stored atomically in shared memory so that other processes can set them
 * without locking.
 */
typedef enum
{
	DiskQuotaForkFailed,			/* failed trying to start a worker */
	DiskQuotaRebalance,			/* rebalance the cost limits */
	DiskQuotaNumSignals			/* must be last */
} DiskQuotaSignal;

typedef enum 
{
    WIS_INVALID = 0,
    WIS_STARTING,
    WIS_RUNNING,
    WIS_STOPPING,
}WIState;
/*
 * vacuum workitem array, stored in DiskQuotaShmem->dq_workItems.  This
 * list is mostly protected by DiskQuotaLock, except that if an item is
 * marked 'active' other processes must not modify the work-identifying
 * members.
 */
typedef struct DiskQuotaWorkItem
{
    WIState     dqw_state;
	Oid			dqw_database;
    TimestampTz dqw_last_active;
    TimestampTz dqw_launchtime;
} DiskQuotaWorkItem;

#define NUM_WORKITEMS	256
#define INIT_WORKING_TABLE_SIZE 64

/*-------------
 * The main diskquota shmem struct.  On shared memory we store this main
 * struct and the array of WorkerInfo structs.  This struct keeps:
 *
 * dq_launcherpid	the PID of the diskquota launcher
 * dq_freeWorkers	the WorkerInfo freelist
 * dq_runningWorkers the WorkerInfo non-free queue
 * dq_startingWorker pointer to WorkerInfo currently being started (cleared by
 *					the worker itself as soon as it's up and running)
 * dq_workItems		work item array
 *
 * This struct is protected by DiskQuotaLock, except for dq_signal and parts
 * of the worker list (see above).
 *-------------
 */
typedef struct
{
	pid_t		dq_launcherpid;
    DiskQuotaWorkItem *dq_startingWorker;
	DiskQuotaWorkItem dq_workItems[NUM_WORKITEMS];
} DiskQuotaShmemStruct;

static DiskQuotaShmemStruct *DiskQuotaShmem;
static DiskQuotaWorkItem *myWorkItem;

/*
 * the database list (of dql_dbase elements) in the launcher, and the context
 * that contains it
 */

/* PID of launcher, valid only in worker while shutting down */
int			DiskquotaLauncherPid = 0;

#ifdef EXEC_BACKEND
static pid_t dqlauncher_forkexec(void);
static pid_t dqworker_forkexec(void);
#endif
NON_EXEC_STATIC void DiskQuotaWorkerMain(int argc, char *argv[]) pg_attribute_noreturn();
NON_EXEC_STATIC void DiskQuotaLauncherMain(int argc, char *argv[]) pg_attribute_noreturn();

static List *get_database_list(void);

static void do_diskquota(void);
static void FreeWorkerInfo(int code, Datum arg);

static void dq_sighup_handler(SIGNAL_ARGS);
static void dql_sigusr2_handler(SIGNAL_ARGS);
static void dql_sigterm_handler(SIGNAL_ARGS);

static void init_worker_parameters(void);
static void launcher_init_disk_quota();
static void launcher_monitor_disk_quota();
static void _do_start_all_workers(void);
static void _do_start_worker(DiskQuotaWorkItem *item);

static void init_disk_quota_model(void);
static void refresh_disk_quota_model(void);
static void calculate_table_disk_usage();
static void calculate_schema_disk_usage();
static void calculate_role_disk_usage();
static void flush_local_black_map();
static void reset_local_black_map();
static void check_disk_quota_by_oid(Oid targetOid, int64 current_usage, int8 diskquota_type);
static void get_rel_owner_schema(Oid relid, Oid *ownerOid, Oid *nsOid);
static void update_namespace_map(Oid namespaceoid, int64 updatesize);
static void update_role_map(Oid owneroid, int64 updatesize);
static void remove_namespace_map(Oid namespaceoid);
static void remove_role_map(Oid owneroid);
static bool check_table_is_active(Oid reloid);
static void build_active_table_map();
static bool check_table_quota(Oid reloid, bool ereport_on_violation);
/********************************************************************
 *					  DISKQUOTA LAUNCHER CODE
 ********************************************************************/

#ifdef EXEC_BACKEND
/*
 * forkexec routine for the diskquota launcher process.
 *
 * Format up the arglist, then fork and exec.
 */
static pid_t
dqlauncher_forkexec(void)
{
	char	   *av[10];
	int			ac = 0;

	av[ac++] = "postgres";
	av[ac++] = "--forkdqlauncher";
	av[ac++] = NULL;			/* filled in by postmaster_forkexec */
	av[ac] = NULL;

	Assert(ac < lengthof(av));

	return postmaster_forkexec(ac, av);
}

/*
 * We need this set from the outside, before InitProcess is called
 */
void
vacuumLauncherIAm(void)
{
	am_diskquota_launcher = true;
}
#endif

/*
 * Main entry point for diskquota launcher process, to be called from the
 * postmaster.
 */
int
StartDiskQuotaLauncher(void)
{
	pid_t		DiskQuotaPID;

#ifdef EXEC_BACKEND
	switch ((DiskQuotaPID = dqlauncher_forkexec()))
#else
	switch ((DiskQuotaPID = fork_process()))
#endif
	{
		case -1:
			ereport(LOG,
					(errmsg("could not fork diskquota launcher process: %m")));
			return 0;

#ifndef EXEC_BACKEND
		case 0:
			/* in postmaster child ... */
			InitPostmasterChild();
			/* Close the postmaster's sockets */
			ClosePostmasterPorts(false);
			DiskQuotaLauncherMain(0, NULL);
			break;
#endif
		default:
			return (int) DiskQuotaPID;
	}

	/* shouldn't get here */
	return 0;
}

/*
 * After init stage, launcher is responsible for monitor the disk usage of db objects in active database
 * Laucher will assign a freeworker slot to a active database based on a score.
 */
static void
launcher_monitor_disk_quota()
{
	while(!got_SIGTERM)
	{
		int rc;
		rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					5000, WAIT_EVENT_DISKQUOTA_MAIN);
		ResetLatch(MyLatch);
		/*
		 * Emergency bailout if postmaster has died.  This is to avoid the
		 * necessity for manual cleanup of all postmaster children.
		 */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		/* the normal shutdown case */
		if (got_SIGTERM)
			break;
	}
}

static HeapTuple
GetDatabaseTuple(const char *dbname)
{
    HeapTuple   tuple;
    Relation    relation;
    SysScanDesc scan;
    ScanKeyData key[1];

    /*
     * form a scan key
     */
    ScanKeyInit(&key[0],
                Anum_pg_database_datname,
                BTEqualStrategyNumber, F_NAMEEQ,
                CStringGetDatum(dbname));

    /*
     * Open pg_database and fetch a tuple.  Force heap scan if we haven't yet
     * built the critical shared relcache entries (i.e., we're starting up
     * without a shared relcache cache file).
     */
    relation = heap_open(DatabaseRelationId, AccessShareLock);
    scan = systable_beginscan(relation, DatabaseNameIndexId,
                              criticalSharedRelcachesBuilt,
                              NULL,
                              1, key);

    tuple = systable_getnext(scan);

    /* Must copy tuple before releasing buffer */
    if (HeapTupleIsValid(tuple))
        tuple = heap_copytuple(tuple);

    /* all done */
    systable_endscan(scan);
    heap_close(relation, AccessShareLock);

    return tuple;
}
static Oid
db_name_to_oid(const char *db_name)
{
    Oid oid = InvalidOid;
    HeapTuple tuple;
    StartTransactionCommand();
    tuple = GetDatabaseTuple(db_name);
    if (HeapTupleIsValid(tuple))
    {
        oid = HeapTupleGetOid(tuple);
    }
    CommitTransactionCommand();
    return oid;
}

/*
 *
 */
static void 
launcher_init_disk_quota()
{
    elog(DEBUG1, "start func<%s>", __func__);
    if (diskquota_max_workers > NUM_WORKITEMS)
    {
        elog(DEBUG1, "guc diskquota_max_workers > fixed size of work items: %d - %d",
            diskquota_max_workers, NUM_WORKITEMS);
        diskquota_max_workers = NUM_WORKITEMS;
    }
    _do_start_all_workers();
}

/*
 * Main loop for the diskquota launcher process.
 */
NON_EXEC_STATIC void
DiskQuotaLauncherMain(int argc, char *argv[])
{
	sigjmp_buf	local_sigjmp_buf;

	am_diskquota_launcher = true;

	/* Identify myself via ps */
	init_ps_display(pgstat_get_backend_desc(B_DISKQUOTA_LAUNCHER), "", "", "");
    elog(LOG, "disk quota enabled database list:'%s'\n", guc_dq_database_list);

	if (PostAuthDelay)
		pg_usleep(PostAuthDelay * 1000000L);

	SetProcessingMode(InitProcessing);
	/*
	 * Set up signal handlers.  We operate on databases much like a regular
	 * backend, so we use the same signal handling.  See equivalent code in
	 * tcop/postgres.c.
	 */
	pqsignal(SIGHUP, dq_sighup_handler);
	pqsignal(SIGINT, StatementCancelHandler);
	pqsignal(SIGTERM, dql_sigterm_handler);

	pqsignal(SIGQUIT, quickdie);
	InitializeTimeouts();		/* establishes SIGALRM handler */

	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGUSR2, dql_sigusr2_handler);
	pqsignal(SIGFPE, FloatExceptionHandler);
	pqsignal(SIGCHLD, SIG_DFL);

	/* Early initialization */
	BaseInit();
	/*
	 * Create a per-backend PGPROC struct in shared memory, except in the
	 * EXEC_BACKEND case where this was done in SubPostmasterMain. We must do
	 * this before we can use LWLocks (and in the EXEC_BACKEND case we already
	 * had to do some stuff with LWLocks).
	 */
#ifndef EXEC_BACKEND
	InitProcess();
#endif
	InitPostgres(NULL, InvalidOid, NULL, InvalidOid, NULL, false);

	SetProcessingMode(NormalProcessing);

	/*
	 * Create a memory context that we will do all our work in.  We do this so
	 * that we can reset the context during error recovery and thereby avoid
	 * possible memory leaks.
	 */
	diskquotaMemCxt = AllocSetContextCreate(TopMemoryContext,
										  "diskquota Launcher",
										  ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(diskquotaMemCxt);

	/*
	 * If an exception is encountered, processing resumes here.
	 *
	 * This code is a stripped down version of PostgresMain error recovery.
	 */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* since not using PG_TRY, must reset error stack by hand */
		error_context_stack = NULL;

		/* Prevents interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/* Forget any pending QueryCancel or timeout request */
		disable_all_timeouts(false);
		QueryCancelPending = false; /* second to avoid race condition */

		/* Report the error to the server log */
		EmitErrorReport();

		/* Abort the current transaction in order to recover */
		AbortCurrentTransaction();

		/*
		 * Release any other resources, for the case where we were not in a
		 * transaction.
		 */
		LWLockReleaseAll();
		pgstat_report_wait_end();
		AbortBufferIO();
		UnlockBuffers();
		if (CurrentResourceOwner)
		{
			ResourceOwnerRelease(CurrentResourceOwner,
								 RESOURCE_RELEASE_BEFORE_LOCKS,
								 false, true);
			/* we needn't bother with the other ResourceOwnerRelease phases */
		}
		AtEOXact_Buffers(false);
		AtEOXact_SMgr();
		AtEOXact_Files(false);
		AtEOXact_HashTables(false);

		/*
		 * Now return to normal top-level context and clear ErrorContext for
		 * next time.
		 */
		MemoryContextSwitchTo(diskquotaMemCxt);
		FlushErrorState();

		/* Flush any leaked data in the top-level context */
		MemoryContextResetAndDeleteChildren(diskquotaMemCxt);

		/*
		 * Make sure pgstat also considers our stat data as gone.  Note: we
		 * mustn't use diskquota_refresh_stats here.
		 */
		pgstat_clear_snapshot();

		/* Now we can allow interrupts again */
		RESUME_INTERRUPTS();

		/* if in shutdown mode, no need for anything further; just go away */
		if (got_SIGTERM)
			goto shutdown;

		/*
		 * Sleep at least 1 second after any error.  We don't want to be
		 * filling the error logs as fast as we can.
		 */
		pg_usleep(1000000L);
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	/* must unblock signals before calling rebuild_database_list */
	PG_SETMASK(&UnBlockSig);

	/*
	 * Set always-secure search path.  Launcher doesn't connect to a database,
	 * so this has no effect.
	 */
	SetConfigOption("search_path", "", PGC_SUSET, PGC_S_OVERRIDE);

	/*
	 * Force zero_damaged_pages OFF in the diskquota process, even if it is set
	 * in postgresql.conf.  We don't really want such a dangerous option being
	 * applied non-interactively.
	 */
	SetConfigOption("zero_damaged_pages", "false", PGC_SUSET, PGC_S_OVERRIDE);

	/*
	 * Force settable timeouts off to avoid letting these settings prevent
	 * regular maintenance from being executed.
	 */
	SetConfigOption("statement_timeout", "0", PGC_SUSET, PGC_S_OVERRIDE);
	SetConfigOption("lock_timeout", "0", PGC_SUSET, PGC_S_OVERRIDE);
	SetConfigOption("idle_in_transaction_session_timeout", "0",
					PGC_SUSET, PGC_S_OVERRIDE);

	/*
	 * Force default_transaction_isolation to READ COMMITTED.  We don't want
	 * to pay the overhead of serializable mode, nor add any risk of causing
	 * deadlocks or delaying other transactions.
	 */
	SetConfigOption("default_transaction_isolation", "read committed",
					PGC_SUSET, PGC_S_OVERRIDE);

	DiskQuotaShmem->dq_launcherpid = MyProcPid;

	/* init disk quota information */
	launcher_init_disk_quota();

	/* monitor disk quota change */
	launcher_monitor_disk_quota();

	/* Normal exit from the diskquota launcher is here */
shutdown:
	ereport(DEBUG1,
			(errmsg("diskquota launcher shutting down")));
	DiskQuotaShmem->dq_launcherpid = 0;

	proc_exit(0);				/* done */
}

/* get max number of workers */
static int
get_num_workers()
{
    return diskquota_max_workers;
}

static void
_do_start_worker(DiskQuotaWorkItem *item)
{
    SendPostmasterSignal(PMSIGNAL_START_DISKQUOTA_WORKER);
}

static void
_do_start_all_workers()
{
	MemoryContext tmpcxt,
				oldcxt;
    int idx = 0;
    int rc, N;
    DiskQuotaWorkItem *item;
    DiskQuotaWorkItem *itemArray;

    elog(DEBUG1, "%s ...", __func__);
	tmpcxt = AllocSetContextCreate(CurrentMemoryContext,
								   "Start worker tmp cxt",
								   ALLOCSET_DEFAULT_SIZES);
	oldcxt = MemoryContextSwitchTo(tmpcxt);

    init_worker_parameters();
    itemArray = DiskQuotaShmem->dq_workItems;
    N = get_num_workers();
    while(idx<N) {
		LWLockAcquire(DiskQuotaLock, LW_EXCLUSIVE);
        item = DiskQuotaShmem->dq_startingWorker;
        if (item != NULL)
        { // TODO: something is wrong when creating a worker process
            if (item->dqw_state != WIS_RUNNING)
            {
                elog(WARNING, "running failed, state=%d", (int)item->dqw_state);
            }
            LWLockRelease(DiskQuotaLock);
            rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                    2000, WAIT_EVENT_DISKQUOTA_MAIN);
            continue;
        }
        DiskQuotaShmem->dq_startingWorker = item =  &itemArray[idx];
        if (!OidIsValid(item->dqw_database)) 
        {
            LWLockRelease(DiskQuotaLock);
            break;
        }
        item->dqw_state = WIS_STARTING;
        item->dqw_launchtime = GetCurrentTimestamp();
        LWLockRelease(DiskQuotaLock);

        _do_start_worker(item);

        elog(WARNING, "start to wait ... <%s>", __func__);
        rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
            2000, WAIT_EVENT_DISKQUOTA_MAIN);
        elog(WARNING, "wait latch returned <%s> rc=%d", __func__, rc);
        ResetLatch(MyLatch);
        
        idx++;
    }


	MemoryContextSwitchTo(oldcxt);
	MemoryContextDelete(tmpcxt);
}

/* SIGHUP: set flag to re-read config file at next convenient time */
static void
dq_sighup_handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_SIGHUP = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/* SIGUSR2: a worker is up and running, or just finished, or failed to fork */
static void
dql_sigusr2_handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_SIGUSR2 = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/* SIGTERM: time to die */
static void
dql_sigterm_handler(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_SIGTERM = true;
	SetLatch(MyLatch);

	errno = save_errno;
}


/********************************************************************
 *					  DISKQUOTA WORKER CODE
 ********************************************************************/

#ifdef EXEC_BACKEND
/*
 * forkexec routines for the diskquota worker.
 *
 * Format up the arglist, then fork and exec.
 */
static pid_t
dqworker_forkexec(void)
{
	char	   *av[10];
	int			ac = 0;

	av[ac++] = "postgres";
	av[ac++] = "--forkdqworker";
	av[ac++] = NULL;			/* filled in by postmaster_forkexec */
	av[ac] = NULL;

	Assert(ac < lengthof(av));

	return postmaster_forkexec(ac, av);
}

/*
 * We need this set from the outside, before InitProcess is called
 */
void
vacuumWorkerIAm(void)
{
	am_diskquota_worker = true;
}
#endif

/*
 * Main entry point for diskquota worker process.
 *
 * This code is heavily based on pgarch.c, q.v.
 */
int
StartDiskQuotaWorker(void)
{
	pid_t		worker_pid;

#ifdef EXEC_BACKEND
	switch ((worker_pid = dqworker_forkexec()))
#else
	switch ((worker_pid = fork_process()))
#endif
	{
		case -1:
			ereport(LOG,
					(errmsg("could not fork diskquota worker process: %m")));
			return 0;

#ifndef EXEC_BACKEND
		case 0:
			/* in postmaster child ... */
			InitPostmasterChild();

			/* Close the postmaster's sockets */
			ClosePostmasterPorts(false);

			DiskQuotaWorkerMain(0, NULL);
			break;
#endif
		default:
			return (int) worker_pid;
	}

	/* shouldn't get here */
	return 0;
}

/*
 * DiskQuotaWorkerMain
 */
NON_EXEC_STATIC void
DiskQuotaWorkerMain(int argc, char *argv[])
{
	sigjmp_buf	local_sigjmp_buf;
	Oid			dbid;

	am_diskquota_worker = true;

	/* Identify myself via ps */
	init_ps_display(pgstat_get_backend_desc(B_DISKQUOTA_WORKER), "", "", "");

	SetProcessingMode(InitProcessing);

	/*
	 * Set up signal handlers.  We operate on databases much like a regular
	 * backend, so we use the same signal handling.  See equivalent code in
	 * tcop/postgres.c.
	 */
	pqsignal(SIGHUP, dq_sighup_handler);

	/*
	 * SIGINT is used to signal canceling the current table's vacuum; SIGTERM
	 * means abort and exit cleanly, and SIGQUIT means abandon ship.
	 */
	pqsignal(SIGINT, StatementCancelHandler);
	pqsignal(SIGTERM, die);
	pqsignal(SIGQUIT, quickdie);
	InitializeTimeouts();		/* establishes SIGALRM handler */

	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGUSR2, SIG_IGN);
	pqsignal(SIGFPE, FloatExceptionHandler);
	pqsignal(SIGCHLD, SIG_DFL);

	/* Early initialization */
	BaseInit();
	/*
	 * Create a per-backend PGPROC struct in shared memory, except in the
	 * EXEC_BACKEND case where this was done in SubPostmasterMain. We must do
	 * this before we can use LWLocks (and in the EXEC_BACKEND case we already
	 * had to do some stuff with LWLocks).
	 */
#ifndef EXEC_BACKEND
	InitProcess();
#endif

	/*
	 * If an exception is encountered, processing resumes here.
	 *
	 * See notes in postgres.c about the design of this coding.
	 */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* Prevents interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/* Report the error to the server log */
		EmitErrorReport();

		/*
		 * We can now go away.  Note that because we called InitProcess, a
		 * callback was registered to do ProcKill, which will clean up
		 * necessary state.
		 */
		proc_exit(0);
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	PG_SETMASK(&UnBlockSig);

	/*
	 * Set always-secure search path, so malicious users can't redirect user
	 * code (e.g. pg_index.indexprs).  (That code runs in a
	 * SECURITY_RESTRICTED_OPERATION sandbox, so malicious users could not
	 * take control of the entire diskquota worker in any case.)
	 */
	SetConfigOption("search_path", "", PGC_SUSET, PGC_S_OVERRIDE);

	/*
	 * Force zero_damaged_pages OFF in the diskquota process, even if it is set
	 * in postgresql.conf.  We don't really want such a dangerous option being
	 * applied non-interactively.
	 */
	SetConfigOption("zero_damaged_pages", "false", PGC_SUSET, PGC_S_OVERRIDE);

	/*
	 * Force settable timeouts off to avoid letting these settings prevent
	 * regular maintenance from being executed.
	 */
	SetConfigOption("statement_timeout", "0", PGC_SUSET, PGC_S_OVERRIDE);
	SetConfigOption("lock_timeout", "0", PGC_SUSET, PGC_S_OVERRIDE);
	SetConfigOption("idle_in_transaction_session_timeout", "0",
					PGC_SUSET, PGC_S_OVERRIDE);

	/*
	 * Force default_transaction_isolation to READ COMMITTED.  We don't want
	 * to pay the overhead of serializable mode, nor add any risk of causing
	 * deadlocks or delaying other transactions.
	 */
	SetConfigOption("default_transaction_isolation", "read committed",
					PGC_SUSET, PGC_S_OVERRIDE);

	/*
	 * Force synchronous replication off to allow regular maintenance even if
	 * we are waiting for standbys to connect. This is important to ensure we
	 * aren't blocked from performing anti-wraparound tasks.
	 */
	if (synchronous_commit > SYNCHRONOUS_COMMIT_LOCAL_FLUSH)
		SetConfigOption("synchronous_commit", "local",
						PGC_SUSET, PGC_S_OVERRIDE);

	/*
	 * Get the info about the database we're going to work on.
	 */
	LWLockAcquire(DiskQuotaLock, LW_EXCLUSIVE);
	/*
	 * beware of startingWorker being INVALID; this should normally not
	 * happen, but if a worker fails after forking and before this, the
	 * launcher might have decided to remove it from the queue and start
	 * again.
	 */
	if (DiskQuotaShmem->dq_startingWorker != NULL)
	{
        myWorkItem = DiskQuotaShmem->dq_startingWorker;
		dbid = myWorkItem->dqw_database;
        myWorkItem->dqw_state = WIS_RUNNING;

		/*
		 * remove from the "starting" pointer, so that the launcher can start
		 * a new worker if required
		 */
		DiskQuotaShmem->dq_startingWorker = NULL;
		LWLockRelease(DiskQuotaLock);

		on_shmem_exit(FreeWorkerInfo, 0);

		/* wake up the launcher */
		if (DiskQuotaShmem->dq_launcherpid != 0)
			kill(DiskQuotaShmem->dq_launcherpid, SIGUSR2);
	}
	else
	{
		dbid = InvalidOid;
		LWLockRelease(DiskQuotaLock);
	}

	if (OidIsValid(dbid))
	{
		char		dbname[NAMEDATALEN];

		/*
		 * Connect to the selected database
		 *
		 * Note: if we have selected a just-deleted database (due to using
		 * stale stats info), we'll fail and exit here.
		 */
		InitPostgres(NULL, dbid, NULL, InvalidOid, dbname, false);
		SetProcessingMode(NormalProcessing);
		set_ps_display(dbname, false);
		ereport(DEBUG1,
				(errmsg("diskquota: processing database \"%s\"", dbname)));

		if (PostAuthDelay)
			pg_usleep(PostAuthDelay * 1000000L);


		do_diskquota();
	}

	/*
	 * The launcher will be notified of my death in ProcKill, *if* we managed
	 * to get a worker slot at all
	 */

	/* All done, go away */
	proc_exit(0);
}

/*
 * Return a WorkerInfo to the free list
 */
static void
FreeWorkerInfo(int code, Datum arg)
{
    elog(WARNING, "worker exit <%s> pid=%d", __func__, (int)getpid());
	if (myWorkItem != NULL)
	{
		LWLockAcquire(DiskQuotaLock, LW_EXCLUSIVE);

		/*
		 * Wake the launcher up so that he can launch a new worker immediately
		 * if required.  We only save the launcher's PID in local memory here;
		 * the actual signal will be sent when the PGPROC is recycled.  Note
		 * that we always do this, so that the launcher can rebalance the cost
		 * limit setting of the remaining workers.
		 *
		 * We somewhat ignore the risk that the launcher changes its PID
		 * between us reading it and the actual kill; we expect ProcKill to be
		 * called shortly after us, and we assume that PIDs are not reused too
		 * quickly after a process exits.
		 */
		DiskquotaLauncherPid = DiskQuotaShmem->dq_launcherpid;

		myWorkItem->dqw_launchtime = 0;
        myWorkItem->dqw_state = WIS_INVALID;

		/* not mine anymore */
		myWorkItem = NULL;

		/*
		 * now that we're inactive, cause a rebalancing of the surviving
		 * workers
		 */
		LWLockRelease(DiskQuotaLock);
	}
}


/*
 * database list found in guc
 */
static List *
get_database_list(void)
{
	List	   *dblist = NULL;
    if (!SplitIdentifierString(guc_dq_database_list, ',', &dblist))
    {
        elog(FATAL, "cann't get database list from guc:'%s'", guc_dq_database_list);
        return NULL;
    }
	return dblist;
}


void
init_disk_quota_model(void)
{
	HASHCTL		hash_ctl;
	MemoryContext DSModelContext;
	DSModelContext = AllocSetContextCreate(TopMemoryContext,
										   "Disk quotas model context",
										   ALLOCSET_DEFAULT_SIZES);

	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(TableSizeEntry);
	hash_ctl.hcxt = DSModelContext;
	
	table_size_map = hash_create("TableSizeEntry map",
								1024,
								&hash_ctl,
								HASH_ELEM | HASH_CONTEXT| HASH_BLOBS);
	
	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(NamespaceSizeEntry);
	hash_ctl.hcxt = DSModelContext;
	
	namespace_size_map = hash_create("NamespaceSizeEntry map",
								1024,
								&hash_ctl,
								HASH_ELEM | HASH_CONTEXT| HASH_BLOBS);

	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(RoleSizeEntry);
	hash_ctl.hcxt = DSModelContext;
	
	role_size_map = hash_create("RoleSizeEntry map",
								1024,
								&hash_ctl,
								HASH_ELEM | HASH_CONTEXT| HASH_BLOBS);
	/* */
	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(LocalBlackMapEntry);
	local_disk_quota_black_map = hash_create("local blackmap whose quota limitation is reached",
									MAX_LOCAL_DISK_QUOTA_BLACK_ENTRIES,
									&hash_ctl,
									HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);
	/* Prepare the hash table */
	if (pgstat_table_map == NULL)
	{
		HASHCTL ctl;

		memset(&ctl, 0, sizeof(ctl));

		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(DiskQuotaStatHashEntry);

		pgstat_table_map = hash_create("disk quota Table State Entry lookup hash table",
		                            NUM_WORKITEMS,
		                            &ctl,
		                            HASH_ELEM | HASH_BLOBS);
	}

	if (pgstat_active_table_map == NULL)
	{
		HASHCTL ctl;

		memset(&ctl, 0, sizeof(ctl));

		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(DiskQuotaActiveHashEntry);

		pgstat_active_table_map = hash_create("disk quota Active Table Entry lookup hash table",
		                             INIT_WORKING_TABLE_SIZE,
		                             &ctl,
		                             HASH_ELEM | HASH_BLOBS);
	}

	refresh_disk_quota_model();
}

static void
flush_local_black_map()
{
	HASH_SEQ_STATUS iter;
	LocalBlackMapEntry* localblackentry;
	BlackMapEntry* blackentry;
	bool found;

	LWLockAcquire(DiskQuotaLock, LW_EXCLUSIVE);

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
					blackentry->targetoid = blackentry->targetoid;
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
	LWLockRelease(DiskQuotaLock);

}

static void 
reset_local_black_map()
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
	LWLockAcquire(DiskQuotaLock, LW_SHARED);
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
	LWLockRelease(DiskQuotaLock);

}

static void check_disk_quota_by_oid(Oid targetOid, int64 current_usage, int8 diskquota_type)
{
	bool					found;
	HeapTuple				tuple;
	int32 					quota_limit_mb;
	int32 					current_usage_mb;
	LocalBlackMapEntry*	localblackentry;

	tuple = SearchSysCache1(DISKQUOTATARGETOID, ObjectIdGetDatum(targetOid));
	if (!HeapTupleIsValid(tuple))
	{
		/* default no limit */
		return;
	}
	quota_limit_mb = ((Form_pg_diskquota) GETSTRUCT(tuple))->quotalimit;
	ReleaseSysCache(tuple);	
	
	current_usage_mb = current_usage / (1024 *1024);
	if(current_usage_mb > quota_limit_mb) 
	{
		elog(DEBUG1,"Put object %u to blacklist with quota limit:%d, current usage:%d", 
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

/*
 *  Incremental way to update the disk quota of every database objects
 *  Recalculate the table's disk usage when it's a new table or be update.
 *  Detect the removed table if it's nolonger in pg_class.
 *  If change happens, no matter size change or owner change, 
 *  update schemasizemap and rolesizemap correspondingly.
 *
 */
static void 
calculate_table_disk_usage()
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
			update_namespace_map(tsentry->namespaceoid, tsentry->totalsize - oldtotalsize);
			update_role_map(tsentry->owneroid, tsentry->totalsize - oldtotalsize);
		}
		/* check the disk quota limit */
		check_disk_quota_by_oid(tsentry->reloid, tsentry->totalsize, DISKQUOTA_TYPE_TABLE);

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

static void calculate_schema_disk_usage()
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
		elog(LOG,"check namespace:%u with usage:%ld", nsentry->namespaceoid, nsentry->totalsize);
		check_disk_quota_by_oid(nsentry->namespaceoid, nsentry->totalsize, DISKQUOTA_TYPE_SCHEMA);
	}
}

static void calculate_role_disk_usage()
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
		elog(LOG,"check role:%u with usage:%ld", rolentry->owneroid, rolentry->totalsize);
		check_disk_quota_by_oid(rolentry->owneroid, rolentry->totalsize, DISKQUOTA_TYPE_ROLE);
	}
}

static bool check_table_is_active(Oid reloid)
{
	bool found = false;
	hash_search(pgstat_active_table_map, &reloid, HASH_REMOVE, &found);
	if (found)
	{
		elog(LOG,"table is active with oid:%u", reloid);
	}
	return found;
}

static void build_active_table_map()
{
	DiskQuotaStatHashEntry *hash_entry;
	HASH_SEQ_STATUS status;

	hash_seq_init(&status, pgstat_table_map);

	/* reset current pg_stat snapshot to get new data */
	pgstat_clear_snapshot();

	while ((hash_entry = (DiskQuotaStatHashEntry *) hash_seq_search(&status)) != NULL)
	{

		bool found_entry;
		PgStat_StatTabEntry *stat_entry;

		stat_entry = pgstat_fetch_stat_tabentry(hash_entry->t_id);
		if (stat_entry == NULL) {
			continue;
		}

		if (stat_entry->tuples_inserted != hash_entry->t_entry.tuples_inserted ||
			stat_entry->tuples_updated != hash_entry->t_entry.tuples_updated ||
			stat_entry->tuples_deleted != hash_entry->t_entry.tuples_deleted ||
			stat_entry->autovac_vacuum_count !=  hash_entry->t_entry.autovac_vacuum_count ||
			stat_entry->vacuum_count !=  hash_entry->t_entry.vacuum_count)
		{
			/* Update the entry */
			hash_entry->t_entry.tuples_inserted = stat_entry->tuples_inserted;
			hash_entry->t_entry.tuples_updated = stat_entry->tuples_updated;
			hash_entry->t_entry.tuples_deleted = stat_entry->tuples_deleted;
			hash_entry->t_entry.autovac_vacuum_count = stat_entry->autovac_vacuum_count;
			hash_entry->t_entry.vacuum_count = stat_entry->vacuum_count;

			/* Add this entry to active hash table if not exist */
			hash_search(pgstat_active_table_map, &hash_entry->t_id, HASH_ENTER, &found_entry);

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
	StartTransactionCommand();
	/* */
	calculate_table_disk_usage();
	/**/
	calculate_schema_disk_usage();
	/* need to consider parent role?*/
	calculate_role_disk_usage();
	
	CommitTransactionCommand();
	flush_local_black_map();
}

/*
 * Process a database table-by-table
 *
 * Note that CHECK_FOR_INTERRUPTS is supposed to be used in certain spots in
 * order not to ignore shutdown commands for too long.
 */
static void
do_diskquota(void)
{
	init_disk_quota_model();	

	SetLatch(MyLatch);
	while (!got_SIGTERM)
	{
		int rc;
		rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					5000, WAIT_EVENT_DISKQUOTA_MAIN);
		ResetLatch(MyLatch);

		/*
		 * Emergency bailout if postmaster has died.  This is to avoid the
		 * necessity for manual cleanup of all postmaster children.
		 */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		/* the normal shutdown case */
		if (got_SIGTERM)
			break;

		elog(LOG,"refresh disk quota model begin");
		build_active_table_map();
		refresh_disk_quota_model();
		elog(LOG,"refresh disk quota model end");
	}
}

/*
 * DiskQuotaingActive
 *		Check GUC vars and report whether the diskquota process should be
 *		running.
 */
bool
DiskQuotaingActive(void)
{
	if (!diskquota_start_daemon || !pgstat_track_counts)
		return false;
	return true;
}

/*
 * diskquota_init
 *		This is called at postmaster initialization.
 *
 * All we do here is annoy the user if he got it wrong.
 */
void
diskquota_init(void)
{
	if (diskquota_start_daemon && !pgstat_track_counts)
		ereport(WARNING,
				(errmsg("diskquota not started because of misconfiguration"),
				 errhint("Enable the \"track_counts\" option.")));
}

/*
 * IsDiskQuota functions
 *		Return whether this is either a launcher diskquota process or a worker
 *		process.
 */
bool
IsDiskQuotaLauncherProcess(void)
{
	return am_diskquota_launcher;
}

bool
IsDiskQuotaWorkerProcess(void)
{
	return am_diskquota_worker;
}


/*
 * DiskQuotaShmemSize
 *		Compute space needed for diskquota-related shared memory
 */
Size
DiskQuotaShmemSize(void)
{
	Size		size;

	size = sizeof(DiskQuotaShmemStruct);
	size = MAXALIGN(size);
	size = add_size(size, hash_estimate_size(MAX_DISK_QUOTA_BLACK_ENTRIES, sizeof(BlackMapEntry)));
	return size;
}

/*
 * DiskQuotaShmemInit
 *		Allocate and initialize diskquota-related shared memory
 */
void
DiskQuotaShmemInit(void)
{
	bool		found;
	HASHCTL		hash_ctl;

	DiskQuotaShmem = (DiskQuotaShmemStruct *)
		ShmemInitStruct("DiskQuota Data",
						DiskQuotaShmemSize(),
						&found);

	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(BlackMapEntry);
	hash_ctl.alloc = ShmemAllocNoError;
	hash_ctl.dsize = hash_ctl.max_dsize = hash_select_dirsize(MAX_DISK_QUOTA_BLACK_ENTRIES);
	disk_quota_black_map = ShmemInitHash("blackmap whose quota limitation is reached",
									INIT_DISK_QUOTA_BLACK_ENTRIES,
									MAX_DISK_QUOTA_BLACK_ENTRIES,
									&hash_ctl,
									HASH_DIRSIZE | HASH_SHARED_MEM | HASH_ALLOC | HASH_ELEM | HASH_BLOBS);


	if (!IsUnderPostmaster)
	{
		Assert(!found);

		DiskQuotaShmem->dq_launcherpid = 0;
		DiskQuotaShmem->dq_startingWorker = NULL;
		memset(DiskQuotaShmem->dq_workItems, 0,
			   sizeof(DiskQuotaWorkItem) * NUM_WORKITEMS);
	}
	else
		Assert(found);
	
}
static void
init_worker_parameters()
{
    List *dblist;
    ListCell *cell;
    int i = 0;
    DiskQuotaWorkItem *worker;

    dblist = get_database_list();
    worker = DiskQuotaShmem->dq_workItems;
    elog(WARNING, "%s, max=%d", __func__, diskquota_max_workers);

    foreach(cell, dblist)
    {
        char *db_name;
        Oid db_oid = InvalidOid;

        db_name = (char *)lfirst(cell);
        if (db_name == NULL || *db_name == '\0')
        {
            elog(WARNING, "invalid db name='%s'", db_name);
            continue;
        }
        db_oid = db_name_to_oid(db_name);
        if (db_oid == InvalidOid)
        {
            elog(WARNING, "cann't find oid for db='%s'", db_name);
            continue;
        }
        if (i>=diskquota_max_workers)
        {
            elog(WARNING, "diskquota_max_workers<NUM_WORKITEMS: %d - %d\n", diskquota_max_workers, NUM_WORKITEMS);
            break;
        }
        worker[i].dqw_database = db_oid;
        elog(WARNING, "db_name[%d] = '%s' oid=%d", i, db_name, db_oid);
        ++i;
    }
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
bool
CheckTableQuota(RangeTblEntry *rte)
{
	bool found;
	Oid ownerOid = InvalidOid;
	Oid nsOid = InvalidOid;
	Oid reloid;

	/* see ExecCheckRTEPerms() */
	if (rte->rtekind != RTE_RELATION)
		return true;

	/* check for insert and update tables */
	if ((rte->requiredPerms & ACL_INSERT) == 0 && (rte->requiredPerms & ACL_UPDATE) == 0)
		return true;

	reloid = rte->relid;
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
	
	get_rel_owner_schema(reloid, &ownerOid, &nsOid);
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
	return true;
}
