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
#include "catalog/namespace.h"
#include "catalog/pg_database.h"
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


/*
 * GUC parameters
 */
bool		diskquota_start_daemon = true;
int			diskquota_max_workers;
int			diskquota_work_mem = -1;
int			diskquota_naptime;
int			diskquota_vac_thresh;
double		diskquota_vac_scale;
int			diskquota_anl_thresh;
double		diskquota_anl_scale;
int			diskquota_freeze_max_age;
int			diskquota_multixact_freeze_max_age;

int			diskquota_vac_cost_delay;
int			diskquota_vac_cost_limit;

int			Log_diskquota_min_duration = -1;

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

/* Comparison points for determining whether freeze_max_age is exceeded */
static TransactionId recentXid;
static MultiXactId recentMulti;

/* Memory context for long-lived data */
static MemoryContext diskquotaMemCxt;

/* struct to keep track of databases in launcher */
typedef struct dql_dbase
{
	Oid			adl_datid;		/* hash key -- must be first */
	TimestampTz adl_next_worker;
	int			adl_score;
	dlist_node	adl_node;
} dql_dbase;

/* struct to keep track of databases in worker */
typedef struct dqw_dbase
{
	Oid			adw_datid;
	char	   *adw_name;
	TransactionId adw_frozenxid;
	MultiXactId adw_minmulti;
	PgStat_StatDBEntry *adw_entry;
} dqw_dbase;

/* struct to keep track of tables to vacuum and/or analyze, after rechecking */
typedef struct diskquota_table
{
	Oid			at_relid;
	int			at_vacoptions;	/* bitmask of DiskQuotaOption */
	int			at_vacuum_cost_delay;
	int			at_vacuum_cost_limit;
	bool		at_dobalance;
	bool		at_sharedrel;
	char	   *at_relname;
	char	   *at_nspname;
	char	   *at_datname;
} diskquota_table;

/*-------------
 * This struct holds information about a single worker's whereabouts.  We keep
 * an array of these in shared memory, sized according to
 * diskquota_max_workers.
 *
 * wi_links		entry into free list or running list
 * wi_dboid		OID of the database this worker is supposed to work on
 * wi_tableoid	OID of the table currently being vacuumed, if any
 * wi_sharedrel flag indicating whether table is marked relisshared
 * wi_proc		pointer to PGPROC of the running worker, NULL if not started
 * wi_launchtime Time at which this worker was launched
 * wi_cost_*	DiskQuota cost-based delay parameters current in this worker
 *
 * All fields are protected by DiskQuotaLock, except for wi_tableoid and
 * wi_sharedrel which are protected by vacuumScheduleLock (note these
 * two fields are read-only for everyone except that worker itself).
 *-------------
 */
typedef struct WorkerInfoData
{
	dlist_node	wi_links;
	Oid			wi_dboid;
	Oid			wi_tableoid;
	PGPROC	   *wi_proc;
	TimestampTz wi_launchtime;
	bool		wi_dobalance;
	bool		wi_sharedrel;
	int			wi_cost_delay;
	int			wi_cost_limit;
	int			wi_cost_limit_base;
} WorkerInfoData;

typedef struct WorkerInfoData *WorkerInfo;

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
}			DiskQuotaSignal;

/*
 * vacuum workitem array, stored in DiskQuotaShmem->dq_workItems.  This
 * list is mostly protected by DiskQuotaLock, except that if an item is
 * marked 'active' other processes must not modify the work-identifying
 * members.
 */
typedef struct DiskQuotaWorkItem
{
	DiskQuotaWorkItemType dqw_type;
	bool		dqw_used;		/* below data is valid */
	bool		dqw_active;		/* being processed */
	Oid			dqw_database;
	Oid			dqw_relation;
	BlockNumber dqw_blockNumber;
} DiskQuotaWorkItem;

#define NUM_WORKITEMS	256

/*-------------
 * The main diskquota shmem struct.  On shared memory we store this main
 * struct and the array of WorkerInfo structs.  This struct keeps:
 *
 * dq_signal		set by other processes to indicate various conditions
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
	sig_atomic_t dq_signal[DiskQuotaNumSignals];
	pid_t		dq_launcherpid;
	dlist_head	dq_freeWorkers;
	dlist_head	dq_runningWorkers;
	WorkerInfo	dq_startingWorker;
	DiskQuotaWorkItem dq_workItems[NUM_WORKITEMS];
} DiskQuotaShmemStruct;

static DiskQuotaShmemStruct *DiskQuotaShmem;

/*
 * the database list (of dql_dbase elements) in the launcher, and the context
 * that contains it
 */
static dlist_head DatabaseList = DLIST_STATIC_INIT(DatabaseList);
static MemoryContext DatabaseListCxt = NULL;

/* Pointer to my own WorkerInfo, valid on each worker */
static WorkerInfo MyWorkerInfo = NULL;

/* PID of launcher, valid only in worker while shutting down */
int			DiskquotaLauncherPid = 0;

#ifdef EXEC_BACKEND
static pid_t dqlauncher_forkexec(void);
static pid_t dqworker_forkexec(void);
#endif
NON_EXEC_STATIC void DiskQuotaWorkerMain(int argc, char *argv[]) pg_attribute_noreturn();
NON_EXEC_STATIC void DiskQuotaLauncherMain(int argc, char *argv[]) pg_attribute_noreturn();

static Oid	do_start_worker(void);
static void launcher_determine_sleep(bool canlaunch, bool recursing,
						 struct timeval *nap);
static void launch_worker(TimestampTz now);
static List *get_database_list(void);
static void rebuild_database_list(Oid newdb);
static int	db_comparator(const void *a, const void *b);

static void do_diskquota(void);
static void FreeWorkerInfo(int code, Datum arg);

static void dq_sighup_handler(SIGNAL_ARGS);
static void dql_sigusr2_handler(SIGNAL_ARGS);
static void dql_sigterm_handler(SIGNAL_ARGS);
static void diskquota_refresh_stats(void);

static void launcher_init_disk_quota();
static void launcher_monitor_disk_quota();

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
			elog(WARNING, "worker hubert1");
			InitPostmasterChild();
			elog(WARNING, "worker hubert2");
			/* Close the postmaster's sockets */
			ClosePostmasterPorts(false);
			elog(WARNING, "worker hubert3");
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
/*
 *
 */
static void 
launcher_init_disk_quota()
{
	List	*dblist;
	ListCell	*cell;
	dqw_dbase	*dqdb;
	MemoryContext tmpcxt,
				oldcxt;
	/*
     * Create and switch to a temporary context to avoid leaking the memory
     * allocated for the database list.
     */
    tmpcxt = AllocSetContextCreate(CurrentMemoryContext,
                                   "Start worker tmp cxt",
                                   ALLOCSET_DEFAULT_SIZES);
    oldcxt = MemoryContextSwitchTo(tmpcxt);

    /* Get a list of databases */
    dblist = get_database_list();

	foreach(cell, dblist)
	{
		dqw_dbase  *tmp = lfirst(cell);
		dqdb = tmp;
		    /* Found a database -- process it */
    	if (dqdb != NULL)
		{
			WorkerInfo  worker;
			dlist_node *wptr;

			LWLockAcquire(DiskQuotaLock, LW_EXCLUSIVE);

			/*
			 * Get a worker entry from the freelist.  We checked above, so there
			 * really should be a free slot.
			 */
			wptr = dlist_pop_head_node(&DiskQuotaShmem->dq_freeWorkers);

			worker = dlist_container(WorkerInfoData, wi_links, wptr);
			worker->wi_dboid = dqdb->adw_datid;
			elog(WARNING, "dbid launcer hubert:%u", worker->wi_dboid);
			worker->wi_proc = NULL;
			worker->wi_launchtime = GetCurrentTimestamp();

			DiskQuotaShmem->dq_startingWorker = worker;

			LWLockRelease(DiskQuotaLock);

			SendPostmasterSignal(PMSIGNAL_START_DISKQUOTA_WORKER);

			//retval = dqdb->adw_datid;
		}
		break;
	}
	MemoryContextSwitchTo(oldcxt);
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

		/* don't leave dangling pointers to freed memory */
		DatabaseListCxt = NULL;
		dlist_init(&DatabaseList);

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
	if (!DiskQuotaingActive())
	{
		if (!got_SIGTERM)
			do_start_worker();
		proc_exit(0);			/* done */
	}

	DiskQuotaShmem->dq_launcherpid = MyProcPid;

	/*
	 * Create the initial database list.  The invariant we want this list to
	 * keep is that it's ordered by decreasing next_time.  As soon as an entry
	 * is updated to a higher time, it will be moved to the front (which is
	 * correct because the only operation is to add diskquota_naptime to the
	 * entry, and time always increases).
	 */
	rebuild_database_list(InvalidOid);
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

/*
 * Determine the time to sleep, based on the database list.
 *
 * The "canlaunch" parameter indicates whether we can start a worker right now,
 * for example due to the workers being all busy.  If this is false, we will
 * cause a long sleep, which will be interrupted when a worker exits.
 */
static void
launcher_determine_sleep(bool canlaunch, bool recursing, struct timeval *nap)
{
	/*
	 * We sleep until the next scheduled vacuum.  We trust that when the
	 * database list was built, care was taken so that no entries have times
	 * in the past; if the first entry has too close a next_worker value, or a
	 * time in the past, we will sleep a small nominal time.
	 */
	if (!canlaunch)
	{
		nap->tv_sec = diskquota_naptime;
		nap->tv_usec = 0;
	}
	else if (!dlist_is_empty(&DatabaseList))
	{
		TimestampTz current_time = GetCurrentTimestamp();
		TimestampTz next_wakeup;
		dql_dbase  *dqdb;
		long		secs;
		int			usecs;

		dqdb = dlist_tail_element(dql_dbase, adl_node, &DatabaseList);

		next_wakeup = dqdb->adl_next_worker;
		TimestampDifference(current_time, next_wakeup, &secs, &usecs);

		nap->tv_sec = secs;
		nap->tv_usec = usecs;
	}
	else
	{
		/* list is empty, sleep for whole diskquota_naptime seconds  */
		nap->tv_sec = diskquota_naptime;
		nap->tv_usec = 0;
	}

	/*
	 * If the result is exactly zero, it means a database had an entry with
	 * time in the past.  Rebuild the list so that the databases are evenly
	 * distributed again, and recalculate the time to sleep.  This can happen
	 * if there are more tables needing vacuum than workers, and they all take
	 * longer to vacuum than diskquota_naptime.
	 *
	 * We only recurse once.  rebuild_database_list should always return times
	 * in the future, but it seems best not to trust too much on that.
	 */
	if (nap->tv_sec == 0 && nap->tv_usec == 0 && !recursing)
	{
		rebuild_database_list(InvalidOid);
		launcher_determine_sleep(canlaunch, true, nap);
		return;
	}

	/* The smallest time we'll allow the launcher to sleep. */
	if (nap->tv_sec <= 0 && nap->tv_usec <= MIN_DISKQUOTA_SLEEPTIME * 1000)
	{
		nap->tv_sec = 0;
		nap->tv_usec = MIN_DISKQUOTA_SLEEPTIME * 1000;
	}

	/*
	 * If the sleep time is too large, clamp it to an arbitrary maximum (plus
	 * any fractional seconds, for simplicity).  This avoids an essentially
	 * infinite sleep in strange cases like the system clock going backwards a
	 * few years.
	 */
	if (nap->tv_sec > MAX_DISKQUOTA_SLEEPTIME)
		nap->tv_sec = MAX_DISKQUOTA_SLEEPTIME;
}

/*
 * Build an updated DatabaseList.  It must only contain databases that appear
 * in pgstats, and must be sorted by next_worker from highest to lowest,
 * distributed regularly across the next diskquota_naptime interval.
 *
 * Receives the Oid of the database that made this list be generated (we call
 * this the "new" database, because when the database was already present on
 * the list, we expect that this function is not called at all).  The
 * preexisting list, if any, will be used to preserve the order of the
 * databases in the diskquota_naptime period.  The new database is put at the
 * end of the interval.  The actual values are not saved, which should not be
 * much of a problem.
 */
static void
rebuild_database_list(Oid newdb)
{
	List	   *dblist;
	ListCell   *cell;
	MemoryContext newcxt;
	MemoryContext oldcxt;
	MemoryContext tmpcxt;
	HASHCTL		hctl;
	int			score;
	int			nelems;
	HTAB	   *dbhash;
	dlist_iter	iter;

	/* use fresh stats */
	diskquota_refresh_stats();

	newcxt = AllocSetContextCreate(diskquotaMemCxt,
								   "AV dblist",
								   ALLOCSET_DEFAULT_SIZES);
	tmpcxt = AllocSetContextCreate(newcxt,
								   "tmp AV dblist",
								   ALLOCSET_DEFAULT_SIZES);
	oldcxt = MemoryContextSwitchTo(tmpcxt);

	/*
	 * Implementing this is not as simple as it sounds, because we need to put
	 * the new database at the end of the list; next the databases that were
	 * already on the list, and finally (at the tail of the list) all the
	 * other databases that are not on the existing list.
	 *
	 * To do this, we build an empty hash table of scored databases.  We will
	 * start with the lowest score (zero) for the new database, then
	 * increasing scores for the databases in the existing list, in order, and
	 * lastly increasing scores for all databases gotten via
	 * get_database_list() that are not already on the hash.
	 *
	 * Then we will put all the hash elements into an array, sort the array by
	 * score, and finally put the array elements into the new doubly linked
	 * list.
	 */
	hctl.keysize = sizeof(Oid);
	hctl.entrysize = sizeof(dql_dbase);
	hctl.hcxt = tmpcxt;
	dbhash = hash_create("db hash", 20, &hctl,	/* magic number here FIXME */
						 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	/* start by inserting the new database */
	score = 0;
	if (OidIsValid(newdb))
	{
		dql_dbase  *db;
		PgStat_StatDBEntry *entry;

		/* only consider this database if it has a pgstat entry */
		entry = pgstat_fetch_stat_dbentry(newdb);
		if (entry != NULL)
		{
			/* we assume it isn't found because the hash was just created */
			db = hash_search(dbhash, &newdb, HASH_ENTER, NULL);

			/* hash_search already filled in the key */
			db->adl_score = score++;
			/* next_worker is filled in later */
		}
	}

	/* Now insert the databases from the existing list */
	dlist_foreach(iter, &DatabaseList)
	{
		dql_dbase  *dqdb = dlist_container(dql_dbase, adl_node, iter.cur);
		dql_dbase  *db;
		bool		found;
		PgStat_StatDBEntry *entry;

		/*
		 * skip databases with no stat entries -- in particular, this gets rid
		 * of dropped databases
		 */
		entry = pgstat_fetch_stat_dbentry(dqdb->adl_datid);
		if (entry == NULL)
			continue;

		db = hash_search(dbhash, &(dqdb->adl_datid), HASH_ENTER, &found);

		if (!found)
		{
			/* hash_search already filled in the key */
			db->adl_score = score++;
			/* next_worker is filled in later */
		}
	}

	/* finally, insert all qualifying databases not previously inserted */
	dblist = get_database_list();
	foreach(cell, dblist)
	{
		dqw_dbase  *dqdb = lfirst(cell);
		dql_dbase  *db;
		bool		found;
		PgStat_StatDBEntry *entry;

		/* only consider databases with a pgstat entry */
		entry = pgstat_fetch_stat_dbentry(dqdb->adw_datid);
		if (entry == NULL)
			continue;

		db = hash_search(dbhash, &(dqdb->adw_datid), HASH_ENTER, &found);
		/* only update the score if the database was not already on the hash */
		if (!found)
		{
			/* hash_search already filled in the key */
			db->adl_score = score++;
			/* next_worker is filled in later */
		}
	}
	nelems = score;

	/* from here on, the allocated memory belongs to the new list */
	MemoryContextSwitchTo(newcxt);
	dlist_init(&DatabaseList);

	if (nelems > 0)
	{
		TimestampTz current_time;
		int			millis_increment;
		dql_dbase  *dbary;
		dql_dbase  *db;
		HASH_SEQ_STATUS seq;
		int			i;

		/* put all the hash elements into an array */
		dbary = palloc(nelems * sizeof(dql_dbase));

		i = 0;
		hash_seq_init(&seq, dbhash);
		while ((db = hash_seq_search(&seq)) != NULL)
			memcpy(&(dbary[i++]), db, sizeof(dql_dbase));

		/* sort the array */
		qsort(dbary, nelems, sizeof(dql_dbase), db_comparator);

		/*
		 * Determine the time interval between databases in the schedule. If
		 * we see that the configured naptime would take us to sleep times
		 * lower than our min sleep time (which launcher_determine_sleep is
		 * coded not to allow), silently use a larger naptime (but don't touch
		 * the GUC variable).
		 */
		millis_increment = 1000.0 * diskquota_naptime / nelems;
		if (millis_increment <= MIN_DISKQUOTA_SLEEPTIME)
			millis_increment = MIN_DISKQUOTA_SLEEPTIME * 1.1;

		current_time = GetCurrentTimestamp();

		/*
		 * move the elements from the array into the dllist, setting the
		 * next_worker while walking the array
		 */
		for (i = 0; i < nelems; i++)
		{
			dql_dbase  *db = &(dbary[i]);

			current_time = TimestampTzPlusMilliseconds(current_time,
													   millis_increment);
			db->adl_next_worker = current_time;

			/* later elements should go closer to the head of the list */
			dlist_push_head(&DatabaseList, &db->adl_node);
		}
	}

	/* all done, clean up memory */
	if (DatabaseListCxt != NULL)
		MemoryContextDelete(DatabaseListCxt);
	MemoryContextDelete(tmpcxt);
	DatabaseListCxt = newcxt;
	MemoryContextSwitchTo(oldcxt);
}

/* qsort comparator for dql_dbase, using adl_score */
static int
db_comparator(const void *a, const void *b)
{
	if (((const dql_dbase *) a)->adl_score == ((const dql_dbase *) b)->adl_score)
		return 0;
	else
		return (((const dql_dbase *) a)->adl_score < ((const dql_dbase *) b)->adl_score) ? 1 : -1;
}

/*
 * do_start_worker
 *
 * Bare-bones procedure for starting an diskquota worker from the launcher.
 * It determines what database to work on, sets up shared memory stuff and
 * signals postmaster to start the worker.  It fails gracefully if invoked when
 * diskquota_workers are already active.
 *
 * Return value is the OID of the database that the worker is going to process,
 * or InvalidOid if no worker was actually started.
 */
static Oid
do_start_worker(void)
{
	List	   *dblist;
	ListCell   *cell;
	TransactionId xidForceLimit;
	MultiXactId multiForceLimit;
	bool		for_xid_wrap;
	bool		for_multi_wrap;
	dqw_dbase  *dqdb;
	TimestampTz current_time;
	bool		skipit = false;
	Oid			retval = InvalidOid;
	MemoryContext tmpcxt,
				oldcxt;

	/* return quickly when there are no free workers */
	LWLockAcquire(DiskQuotaLock, LW_SHARED);
	if (dlist_is_empty(&DiskQuotaShmem->dq_freeWorkers))
	{
		LWLockRelease(DiskQuotaLock);
		return InvalidOid;
	}
	LWLockRelease(DiskQuotaLock);

	/*
	 * Create and switch to a temporary context to avoid leaking the memory
	 * allocated for the database list.
	 */
	tmpcxt = AllocSetContextCreate(CurrentMemoryContext,
								   "Start worker tmp cxt",
								   ALLOCSET_DEFAULT_SIZES);
	oldcxt = MemoryContextSwitchTo(tmpcxt);

	/* use fresh stats */
	diskquota_refresh_stats();

	/* Get a list of databases */
	dblist = get_database_list();

	/*
	 * Determine the oldest datfrozenxid/relfrozenxid that we will allow to
	 * pass without forcing a vacuum.  (This limit can be tightened for
	 * particular tables, but not loosened.)
	 */
	recentXid = ReadNewTransactionId();
	xidForceLimit = recentXid - diskquota_freeze_max_age;
	/* ensure it's a "normal" XID, else TransactionIdPrecedes misbehaves */
	/* this can cause the limit to go backwards by 3, but that's OK */
	if (xidForceLimit < FirstNormalTransactionId)
		xidForceLimit -= FirstNormalTransactionId;

	/* Also determine the oldest datminmxid we will consider. */
	recentMulti = ReadNextMultiXactId();
	multiForceLimit = recentMulti - MultiXactMemberFreezeThreshold();
	if (multiForceLimit < FirstMultiXactId)
		multiForceLimit -= FirstMultiXactId;

	/*
	 * Choose a database to connect to.  We pick the database that was least
	 * recently auto-vacuumed, or one that needs vacuuming to prevent Xid
	 * wraparound-related data loss.  If any db at risk of Xid wraparound is
	 * found, we pick the one with oldest datfrozenxid, independently of
	 * diskquota times; similarly we pick the one with the oldest datminmxid
	 * if any is in MultiXactId wraparound.  Note that those in Xid wraparound
	 * danger are given more priority than those in multi wraparound danger.
	 *
	 * Note that a database with no stats entry is not considered, except for
	 * Xid wraparound purposes.  The theory is that if no one has ever
	 * connected to it since the stats were last initialized, it doesn't need
	 * vacuuming.
	 *
	 * XXX This could be improved if we had more info about whether it needs
	 * vacuuming before connecting to it.  Perhaps look through the pgstats
	 * data for the database's tables?  One idea is to keep track of the
	 * number of new and dead tuples per database in pgstats.  However it
	 * isn't clear how to construct a metric that measures that and not cause
	 * starvation for less busy databases.
	 */
	dqdb = NULL;
	for_xid_wrap = false;
	for_multi_wrap = false;
	current_time = GetCurrentTimestamp();
	foreach(cell, dblist)
	{
		dqw_dbase  *tmp = lfirst(cell);
		dlist_iter	iter;

		/* Check to see if this one is at risk of wraparound */
		if (TransactionIdPrecedes(tmp->adw_frozenxid, xidForceLimit))
		{
			if (dqdb == NULL ||
				TransactionIdPrecedes(tmp->adw_frozenxid,
									  dqdb->adw_frozenxid))
				dqdb = tmp;
			for_xid_wrap = true;
			continue;
		}
		else if (for_xid_wrap)
			continue;			/* ignore not-at-risk DBs */
		else if (MultiXactIdPrecedes(tmp->adw_minmulti, multiForceLimit))
		{
			if (dqdb == NULL ||
				MultiXactIdPrecedes(tmp->adw_minmulti, dqdb->adw_minmulti))
				dqdb = tmp;
			for_multi_wrap = true;
			continue;
		}
		else if (for_multi_wrap)
			continue;			/* ignore not-at-risk DBs */

		/* Find pgstat entry if any */
		tmp->adw_entry = pgstat_fetch_stat_dbentry(tmp->adw_datid);

		/*
		 * Skip a database with no pgstat entry; it means it hasn't seen any
		 * activity.
		 */
		if (!tmp->adw_entry)
			continue;

		/*
		 * Also, skip a database that appears on the database list as having
		 * been processed recently (less than diskquota_naptime seconds ago).
		 * We do this so that we don't select a database which we just
		 * selected, but that pgstat hasn't gotten around to updating the last
		 * diskquota time yet.
		 */
		skipit = false;

		dlist_reverse_foreach(iter, &DatabaseList)
		{
			dql_dbase  *dbp = dlist_container(dql_dbase, adl_node, iter.cur);

			if (dbp->adl_datid == tmp->adw_datid)
			{
				/*
				 * Skip this database if its next_worker value falls between
				 * the current time and the current time plus naptime.
				 */
				if (!TimestampDifferenceExceeds(dbp->adl_next_worker,
												current_time, 0) &&
					!TimestampDifferenceExceeds(current_time,
												dbp->adl_next_worker,
												diskquota_naptime * 1000))
					skipit = true;

				break;
			}
		}
		if (skipit)
			continue;

		/*
		 * Remember the db with oldest diskquota time.  (If we are here, both
		 * tmp->entry and db->entry must be non-null.)
		 */
	//	if (dqdb == NULL ||
	//		tmp->adw_entry->last_diskquota_time < dqdb->adw_entry->last_diskquota_time)
	//		dqdb = tmp;
	}

	/* Found a database -- process it */
	if (dqdb != NULL)
	{
		WorkerInfo	worker;
		dlist_node *wptr;

		LWLockAcquire(DiskQuotaLock, LW_EXCLUSIVE);

		/*
		 * Get a worker entry from the freelist.  We checked above, so there
		 * really should be a free slot.
		 */
		wptr = dlist_pop_head_node(&DiskQuotaShmem->dq_freeWorkers);

		worker = dlist_container(WorkerInfoData, wi_links, wptr);
		worker->wi_dboid = dqdb->adw_datid;
		worker->wi_proc = NULL;
		worker->wi_launchtime = GetCurrentTimestamp();

		DiskQuotaShmem->dq_startingWorker = worker;

		LWLockRelease(DiskQuotaLock);

		SendPostmasterSignal(PMSIGNAL_START_DISKQUOTA_WORKER);

		retval = dqdb->adw_datid;
	}
	else if (skipit)
	{
		/*
		 * If we skipped all databases on the list, rebuild it, because it
		 * probably contains a dropped database.
		 */
		rebuild_database_list(InvalidOid);
	}

	MemoryContextSwitchTo(oldcxt);
	MemoryContextDelete(tmpcxt);

	return retval;
}

/*
 * launch_worker
 *
 * Wrapper for starting a worker from the launcher.  Besides actually starting
 * it, update the database list to reflect the next time that another one will
 * need to be started on the selected database.  The actual database choice is
 * left to do_start_worker.
 *
 * This routine is also expected to insert an entry into the database list if
 * the selected database was previously absent from the list.
 */
static void
launch_worker(TimestampTz now)
{
	Oid			dbid;
	dlist_iter	iter;

	dbid = do_start_worker();
	if (OidIsValid(dbid))
	{
		bool		found = false;

		/*
		 * Walk the database list and update the corresponding entry.  If the
		 * database is not on the list, we'll recreate the list.
		 */
		dlist_foreach(iter, &DatabaseList)
		{
			dql_dbase  *dqdb = dlist_container(dql_dbase, adl_node, iter.cur);

			if (dqdb->adl_datid == dbid)
			{
				found = true;

				/*
				 * add diskquota_naptime seconds to the current time, and use
				 * that as the new "next_worker" field for this database.
				 */
				dqdb->adl_next_worker =
					TimestampTzPlusMilliseconds(now, diskquota_naptime * 1000);

				dlist_move_head(&DatabaseList, iter.cur);
				break;
			}
		}

		/*
		 * If the database was not present in the database list, we rebuild
		 * the list.  It's possible that the database does not get into the
		 * list anyway, for example if it's a database that doesn't have a
		 * pgstat entry, but this is not a problem because we don't want to
		 * schedule workers regularly into those in any case.
		 */
		if (!found)
			rebuild_database_list(dbid);
	}
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
		elog(WARNING, "Start worker wuhao 6");
		MyWorkerInfo = DiskQuotaShmem->dq_startingWorker;
		dbid = MyWorkerInfo->wi_dboid;
		elog(WARNING, "Start worker wuhao dbid:%u", dbid);
		MyWorkerInfo->wi_proc = MyProc;

		/* insert into the running list */
		elog(WARNING, "Start worker wuhao 6.1");
		dlist_push_head(&DiskQuotaShmem->dq_runningWorkers,
						&MyWorkerInfo->wi_links);

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

		/* And do an appropriate amount of work */
		recentXid = ReadNewTransactionId();
		recentMulti = ReadNextMultiXactId();

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
	if (MyWorkerInfo != NULL)
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

		dlist_delete(&MyWorkerInfo->wi_links);
		MyWorkerInfo->wi_dboid = InvalidOid;
		MyWorkerInfo->wi_tableoid = InvalidOid;
		MyWorkerInfo->wi_sharedrel = false;
		MyWorkerInfo->wi_proc = NULL;
		MyWorkerInfo->wi_launchtime = 0;
		MyWorkerInfo->wi_dobalance = false;
		MyWorkerInfo->wi_cost_delay = 0;
		MyWorkerInfo->wi_cost_limit = 0;
		MyWorkerInfo->wi_cost_limit_base = 0;
		dlist_push_head(&DiskQuotaShmem->dq_freeWorkers,
						&MyWorkerInfo->wi_links);
		/* not mine anymore */
		MyWorkerInfo = NULL;

		/*
		 * now that we're inactive, cause a rebalancing of the surviving
		 * workers
		 */
		DiskQuotaShmem->dq_signal[DiskQuotaRebalance] = true;
		LWLockRelease(DiskQuotaLock);
	}
}


/*
 * get_database_list
 *		Return a list of all databases found in pg_database.
 *
 * The list and associated data is allocated in the caller's memory context,
 * which is in charge of ensuring that it's properly cleaned up afterwards.
 *
 * Note: this is the only function in which the diskquota launcher uses a
 * transaction.  Although we aren't attached to any particular database and
 * therefore can't access most catalogs, we do have enough infrastructure
 * to do a seqscan on pg_database.
 */
static List *
get_database_list(void)
{
	List	   *dblist = NIL;
	Relation	rel;
	HeapScanDesc scan;
	HeapTuple	tup;
	MemoryContext resultcxt;

	/* This is the context that we will allocate our output data in */
	resultcxt = CurrentMemoryContext;

	/*
	 * Start a transaction so we can access pg_database, and get a snapshot.
	 * We don't have a use for the snapshot itself, but we're interested in
	 * the secondary effect that it sets RecentGlobalXmin.  (This is critical
	 * for anything that reads heap pages, because HOT may decide to prune
	 * them even if the process doesn't attempt to modify any tuples.)
	 */
	StartTransactionCommand();
	(void) GetTransactionSnapshot();

	rel = heap_open(DatabaseRelationId, AccessShareLock);
	scan = heap_beginscan_catalog(rel, 0, NULL);

	while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
	{
		Form_pg_database pgdatabase = (Form_pg_database) GETSTRUCT(tup);
		dqw_dbase  *dqdb;
		MemoryContext oldcxt;

		/*
		 * Allocate our results in the caller's context, not the
		 * transaction's. We do this inside the loop, and restore the original
		 * context at the end, so that leaky things like heap_getnext() are
		 * not called in a potentially long-lived context.
		 */
		oldcxt = MemoryContextSwitchTo(resultcxt);

		dqdb = (dqw_dbase *) palloc(sizeof(dqw_dbase));

		dqdb->adw_datid = HeapTupleGetOid(tup);
		dqdb->adw_name = pstrdup(NameStr(pgdatabase->datname));
		dqdb->adw_frozenxid = pgdatabase->datfrozenxid;
		dqdb->adw_minmulti = pgdatabase->datminmxid;
		/* this gets set later: */
		dqdb->adw_entry = NULL;

		dblist = lappend(dblist, dqdb);
		MemoryContextSwitchTo(oldcxt);
	}

	heap_endscan(scan);
	heap_close(rel, AccessShareLock);

	CommitTransactionCommand();

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
	TupleDesc   pg_class_desc;
	
	/*
	 * StartTransactionCommand and CommitTransactionCommand will automatically
	 * switch to other contexts.  We need this one to keep the list of
	 * relations to vacuum/analyze across transactions.
	 */
	diskquotaMemCxt = AllocSetContextCreate(TopMemoryContext,
										  "disk quota worker",
										  ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(diskquotaMemCxt);
	

	/* Start a transaction so our commands have one to play into. */
	StartTransactionCommand();

	
	classRel = heap_open(RelationRelationId, AccessShareLock);

	/* create a copy so we can use it after closing pg_class */
	pg_class_desc = CreateTupleDescCopy(RelationGetDescr(classRel));


	relScan = heap_beginscan_catalog(classRel, 0, NULL);

	/*
	 * On the first pass, we collect main tables to vacuum, and also the main
	 * table relid to TOAST relid mapping.
	 */
	while ((tuple = heap_getnext(relScan, ForwardScanDirection)) != NULL)
	{
		Form_pg_class classForm = (Form_pg_class) GETSTRUCT(tuple);
		Oid			relid;

		if (classForm->relkind != RELKIND_RELATION &&
			classForm->relkind != RELKIND_MATVIEW)
			continue;

		relid = HeapTupleGetOid(tuple);
		elog(LOG,"diskquota worker processing table: db id=%u, relid = %u", MyDatabaseId, relid);
	}

	heap_endscan(relScan);
	heap_close(classRel, AccessShareLock);

	
	/* Finally close out the last transaction. */
	CommitTransactionCommand();
	while(true) {
		sleep(30);
		elog(LOG,"checking diskquota periodically");
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

	/*
	 * Need the fixed struct and the array of WorkerInfoData.
	 */
	size = sizeof(DiskQuotaShmemStruct);
	size = MAXALIGN(size);
	size = add_size(size, mul_size(diskquota_max_workers,
								   sizeof(WorkerInfoData)));
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
		WorkerInfo	worker;
		int			i;

		Assert(!found);

		DiskQuotaShmem->dq_launcherpid = 0;
		dlist_init(&DiskQuotaShmem->dq_freeWorkers);
		dlist_init(&DiskQuotaShmem->dq_runningWorkers);
		DiskQuotaShmem->dq_startingWorker = NULL;
		memset(DiskQuotaShmem->dq_workItems, 0,
			   sizeof(DiskQuotaWorkItem) * NUM_WORKITEMS);

		worker = (WorkerInfo) ((char *) DiskQuotaShmem +
							   MAXALIGN(sizeof(DiskQuotaShmemStruct)));

		/* initialize the WorkerInfo free list */
		for (i = 0; i < diskquota_max_workers; i++)
			dlist_push_head(&DiskQuotaShmem->dq_freeWorkers,
							&worker[i].wi_links);
	}
	else
		Assert(found);
}

/*
 * diskquota_refresh_stats
 *		Refresh pgstats data for an diskquota process
 *
 * Cause the next pgstats read operation to obtain fresh data, but throttle
 * such refreshing in the diskquota launcher.  This is mostly to avoid
 * rereading the pgstats files too many times in quick succession when there
 * are many databases.
 *
 * Note: we avoid throttling in the diskquota worker, as it would be
 * counterproductive in the recheck logic.
 */
static void
diskquota_refresh_stats(void)
{
	if (IsDiskQuotaLauncherProcess())
	{
		static TimestampTz last_read = 0;
		TimestampTz current_time;

		current_time = GetCurrentTimestamp();

		if (!TimestampDifferenceExceeds(last_read, current_time,
										STATS_READ_DELAY))
			return;

		last_read = current_time;
	}

	pgstat_clear_snapshot();
}
