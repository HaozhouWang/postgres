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
#include "catalog/pg_tablespace.h"
#include "commands/dbcommands.h"
#include "commands/vacuum.h"
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
char *_guc_dq_database_list = NULL;
bool		diskquota_start_daemon = true;
int			diskquota_max_workers;

/* how long to keep pgstat data in the launcher, in milliseconds */
#define STATS_READ_DELAY 1000

/* the minimum allowed time between two awakenings of the launcher */
#define MIN_DISKQUOTA_SLEEPTIME 100.0 /* milliseconds */
#define MAX_DISKQUOTA_SLEEPTIME 300	/* seconds */

/* Flags to tell if we are in an diskquota process */
static bool am_diskquota_launcher = false;
static bool am_diskquota_worker = false;

/* Flags set by signal handlers */
static volatile sig_atomic_t got_SIGHUP = false;
static volatile sig_atomic_t got_SIGUSR2 = false;
static volatile sig_atomic_t got_SIGTERM = false;


/* Memory context for long-lived data */
static MemoryContext diskquotaMemCxt;

/*
 * Hash table for O(1) t_id -> tsa_entry lookup
 */
static HTAB *pgStatTabHash = NULL;
static HTAB *pgActiveabHash = NULL;

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
 * pgStatTabHash entry: map from relation OID to PgStat_TableStatus pointer
 */
typedef struct DiskQuotaStateHashEntry
{
	Oid			t_id;
	DiskQuotaLocalTableCache t_entry;
} DiskQuotaStatHashEntry;

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
#define MAX_WORKING_TABLE 64
#define REFRESH_QUOTA_TIME 2

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

/********************************************************************
 *					  AUTOVACUUM LAUNCHER CODE
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
		sleep(10);
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
    elog(WARNING, "start func<%s>", __func__);
    if (diskquota_max_workers > NUM_WORKITEMS)
    {
        elog(WARNING, "guc diskquota_max_workers > fixed size of work items: %d - %d",
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
	elog(WARNING, "worker hubert4");
    elog(WARNING, "_guc_dq_databases='%s'\n", _guc_dq_database_list);
	ereport(DEBUG1,
			(errmsg("diskquota launcher started")));

	if (PostAuthDelay)
		pg_usleep(PostAuthDelay * 1000000L);

	SetProcessingMode(InitProcessing);
	elog(WARNING, "worker hubert5");
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

	elog(WARNING, "worker hubert6");
	/* Early initialization */
	BaseInit();
	elog(WARNING, "worker hubert7");
	/*
	 * Create a per-backend PGPROC struct in shared memory, except in the
	 * EXEC_BACKEND case where this was done in SubPostmasterMain. We must do
	 * this before we can use LWLocks (and in the EXEC_BACKEND case we already
	 * had to do some stuff with LWLocks).
	 */
#ifndef EXEC_BACKEND
	InitProcess();
#endif
	elog(WARNING, "worker hubert8");
	InitPostgres(NULL, InvalidOid, NULL, InvalidOid, NULL, false);

	elog(WARNING, "worker hubert10");
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

	/*
	 * In emergency mode, just start a worker (unless shutdown was requested)
	 * and go away.
	 */
//	if (!DiskQuotaingActive())
//	{
//		if (!got_SIGTERM)
//			do_start_worker();
//		proc_exit(0);			/* done */
//	}

	DiskQuotaShmem->dq_launcherpid = MyProcPid;

	elog(WARNING, "worker hubert12");

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
// max number of workers
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

    elog(WARNING, "%s ...", __func__);
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
 *					  AUTOVACUUM WORKER CODE
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

	elog(WARNING, "Start worker wuhao 1");
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
	elog(WARNING, "Start worker wuhao 1");
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

	elog(WARNING, "Start worker wuhao 4");
	/*
	 * Get the info about the database we're going to work on.
	 */
	LWLockAcquire(DiskQuotaLock, LW_EXCLUSIVE);
	elog(WARNING, "Start worker wuhao 5");
	/*
	 * beware of startingWorker being INVALID; this should normally not
	 * happen, but if a worker fails after forking and before this, the
	 * launcher might have decided to remove it from the queue and start
	 * again.
	 */
	if (DiskQuotaShmem->dq_startingWorker != NULL)
	{
        myWorkItem = DiskQuotaShmem->dq_startingWorker;
		elog(WARNING, "Start worker wuhao 6");
		dbid = myWorkItem->dqw_database;
        myWorkItem->dqw_state = WIS_RUNNING;
		elog(WARNING, "Start worker wuhao dbid:%u", dbid);

		/* insert into the running list */
		elog(WARNING, "Start worker wuhao 6.1");

		/*
		 * remove from the "starting" pointer, so that the launcher can start
		 * a new worker if required
		 */
		DiskQuotaShmem->dq_startingWorker = NULL;
		LWLockRelease(DiskQuotaLock);

		elog(WARNING, "Start worker wuhao 6.2");
		on_shmem_exit(FreeWorkerInfo, 0);

		/* wake up the launcher */
		if (DiskQuotaShmem->dq_launcherpid != 0)
			kill(DiskQuotaShmem->dq_launcherpid, SIGUSR2);
	}
	else
	{
		elog(WARNING, "Start worker wuhao 8");
		/* no worker entry for me, go away */
		elog(WARNING, "diskquota worker started without a worker entry");
		dbid = InvalidOid;
		LWLockRelease(DiskQuotaLock);
	}

    elog(WARNING, "dbid = %d", (int)dbid);

	if (OidIsValid(dbid))
	{
		char		dbname[NAMEDATALEN];

		/*
		 * Report diskquota startup to the stats collector.  We deliberately do
		 * this before InitPostgres, so that the last_diskquota_time will get
		 * updated even if the connection attempt fails.  This is to prevent
		 * diskquota from getting "stuck" repeatedly selecting an unopenable
		 * database, rather than making any progress on stuff it can connect
		 * to.
		 */
		 //todo hubert 
		//pgstat_report_diskquota(dbid);

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
    if (!SplitIdentifierString(_guc_dq_database_list, ',', &dblist))
    {
        elog(FATAL, "cann't get database list from guc:'%s'", _guc_dq_database_list);
        return NULL;
    }
	return dblist;
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

	Relation	classRel;
	HeapTuple	tuple;
	HeapScanDesc relScan;

	HASH_SEQ_STATUS status;

	
    elog(WARNING, "enter <%s>", __func__);
	/*
	 * StartTransactionCommand and CommitTransactionCommand will automatically
	 * switch to other contexts.  We need this one to keep the list of
	 * relations to vacuum/analyze across transactions.
	 */
	diskquotaMemCxt = AllocSetContextCreate(TopMemoryContext,
										  "disk quota worker",
										  ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(diskquotaMemCxt);


	/* Prepare the hash table */
	if (pgStatTabHash == NULL)
	{
		HASHCTL ctl;

		memset(&ctl, 0, sizeof(ctl));

		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(DiskQuotaStatHashEntry);

		pgStatTabHash = hash_create("disk quota Table State Entry lookup hash table",
		                            NUM_WORKITEMS,
		                            &ctl,
		                            HASH_ELEM | HASH_BLOBS);
	}

	if (pgActiveabHash == NULL)
	{
		HASHCTL ctl;

		memset(&ctl, 0, sizeof(ctl));

		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(DiskQuotaActiveHashEntry);

		pgActiveabHash = hash_create("disk quota Active Table Entry lookup hash table",
		                             MAX_WORKING_TABLE,
		                             &ctl,
		                             HASH_ELEM | HASH_BLOBS);
	}

	/* TODO: This need to update from quota setting catalog dynamically */
	/* Start a transaction so our commands have one to play into. */
	StartTransactionCommand();

	
	classRel = heap_open(RelationRelationId, AccessShareLock);


	relScan = heap_beginscan_catalog(classRel, 0, NULL);

	/*
	 * On the first pass, we collect main tables to vacuum, and also the main
	 * table relid to TOAST relid mapping.
	 */
	while ((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		Form_pg_class classForm = (Form_pg_class) GETSTRUCT(tuple);
		Oid			relid;
		DiskQuotaStatHashEntry *entry;
		bool found;

		if (classForm->relkind != RELKIND_RELATION &&
			classForm->relkind != RELKIND_MATVIEW)
			continue;

		relid = HeapTupleGetOid(tuple);

		entry = hash_search(pgStatTabHash, &relid, HASH_ENTER, &found);

		if (!found)
		{
			memset(&entry->t_entry, 0, sizeof(entry->t_entry));
		}
		else
		{
			elog(ERROR,"cloud not init table: db id=%u, relid = %u, table is existing in hash table",
				MyDatabaseId, relid);
		}


		elog(LOG,"diskquota worker processing table: db id=%u, relid = %u", MyDatabaseId, relid);
	}

	heap_endscan(relScan);
	heap_close(classRel, AccessShareLock);

	
	/* Finally close out the last transaction. */
	CommitTransactionCommand();

	/* Check the table status from  */
	while(true) {
		DiskQuotaStatHashEntry *hash_entry;

		hash_seq_init(&status, pgStatTabHash);

		while ((hash_entry = (DiskQuotaStatHashEntry *) hash_seq_search(&status)) != NULL)
		{

			bool found_entry;
			PgStat_StatTabEntry *stat_entry;


			stat_entry = pgstat_fetch_stat_tabentry(hash_entry->t_id);
			if (stat_entry == NULL) {
				elog(LOG, "table relation %d is not found in pg_stat", hash_entry->t_id);
				continue;
			}

			if (stat_entry->tuples_inserted != hash_entry->t_entry.tuples_inserted ||
				stat_entry->tuples_updated != hash_entry->t_entry.tuples_updated ||
				stat_entry->tuples_deleted != hash_entry->t_entry.tuples_deleted ||
				stat_entry->autovac_vacuum_count !=  hash_entry->t_entry.autovac_vacuum_count ||
				stat_entry->vacuum_count !=  hash_entry->t_entry.vacuum_count)
			{
				DiskQuotaActiveHashEntry *active_entry;

				/* Update the entry */
				hash_entry->t_entry.tuples_inserted = stat_entry->tuples_inserted;
				hash_entry->t_entry.tuples_updated = stat_entry->tuples_updated;
				hash_entry->t_entry.tuples_deleted = stat_entry->tuples_deleted;
				hash_entry->t_entry.autovac_vacuum_count = stat_entry->autovac_vacuum_count;
				hash_entry->t_entry.vacuum_count = stat_entry->vacuum_count;

				/* Add this entry to active hash table if not exist */
				active_entry = hash_search(pgActiveabHash, &hash_entry->t_id, HASH_ENTER, &found_entry);

				if (!found_entry) {
					active_entry->t_refcount = 1;
				} else {
					active_entry->t_refcount++;
				}

				elog(LOG, "add table relation %d is in to active queue, ref count is %ld",
				     hash_entry->t_id, active_entry->t_refcount);

			} else {
				/* TODO: should do this in consumer */
				hash_search(pgActiveabHash, &hash_entry->t_id, HASH_REMOVE, &found_entry);
				elog(LOG, "delete table relation %d is in to active queue",
				     hash_entry->t_id);
			}
		}

		if (got_SIGTERM)
		{
			break;
		}

		sleep(REFRESH_QUOTA_TIME);
		elog(LOG,"checking diskquota periodically");

	}
}

static void
pull_pg_stat(void)
{

}

static void
init_tbl_stat_hashtable(Oid databaseOid)
{

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

	DiskQuotaShmem = (DiskQuotaShmemStruct *)
		ShmemInitStruct("DiskQuota Data",
						DiskQuotaShmemSize(),
						&found);

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
