/*-------------------------------------------------------------------------
 *
 * diskquota.h
 *	  header file for integrated diskquota daemon
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/postmaster/diskquota.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DISKQUOTA_H
#define DISKQUOTA_H

#include "nodes/parsenodes.h"
#include "storage/block.h"

/* GUC variables */
extern char *guc_dq_database_list;
extern bool diskquota_start_daemon;
extern int	diskquota_max_workers;
extern int	diskquota_work_mem;
extern int	diskquota_naptime;
extern int	diskquota_vac_thresh;
extern double diskquota_vac_scale;
extern int	diskquota_anl_thresh;
extern double diskquota_anl_scale;
extern int	diskquota_freeze_max_age;
extern int	diskquota_multixact_freeze_max_age;
extern int	diskquota_vac_cost_delay;
extern int	diskquota_vac_cost_limit;

/* diskquota launcher PID, only valid when worker is shutting down */
extern int	DiskquotaLauncherPid;

extern int	Log_diskquota_min_duration;

/* Status inquiry functions */
extern bool DiskQuotaingActive(void);
extern bool IsDiskQuotaLauncherProcess(void);
extern bool IsDiskQuotaWorkerProcess(void);

#define IsAnyDiskQuotaProcess() \
	(IsDiskQuotaLauncherProcess() || IsDiskQuotaWorkerProcess())

/* Functions to start diskquota process, called from postmaster */
extern void diskquota_init(void);
extern int	StartDiskQuotaLauncher(void);
extern int	StartDiskQuotaWorker(void);

/* called from postmaster when a worker could not be forked */
extern void DiskQuotaWorkerFailed(void);

/* diskquota cost-delay balancer */
extern void DiskQuotaUpdateDelay(void);

#ifdef EXEC_BACKEND
extern void DiskQuotaLauncherMain(int argc, char *argv[]) pg_attribute_noreturn();
extern void DiskQuotaWorkerMain(int argc, char *argv[]) pg_attribute_noreturn();
extern void DiskquotaWorkerIAm(void);
extern void DiskquotaLauncherIAm(void);
#endif


/* shared memory stuff */
extern Size DiskQuotaShmemSize(void);
extern void DiskQuotaShmemInit(void);

extern bool CheckTableQuota(RangeTblEntry *rte);
#endif							/* DISKQUOTA_H */
