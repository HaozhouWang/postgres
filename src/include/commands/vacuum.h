/*-------------------------------------------------------------------------
 *
 * vacuum.h
 *	  header file for postgres vacuum cleaner and statistics analyzer
 *
 *
 * Portions Copyright (c) 1996-2003, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/commands/vacuum.h,v 1.49 2004/02/12 23:41:04 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef VACUUM_H
#define VACUUM_H

#include <time.h>
#include <sys/time.h>

#ifdef HAVE_GETRUSAGE
#include <sys/resource.h>
#else
#include "rusagestub.h"
#endif

#include "access/htup.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_type.h"
#include "nodes/parsenodes.h"
#include "utils/rel.h"


/*----------
 * ANALYZE builds one of these structs for each attribute (column) that is
 * to be analyzed.  The struct and subsidiary data are in anl_context,
 * so they live until the end of the ANALYZE operation.
 *
 * The type-specific typanalyze function is passed a pointer to this struct
 * and must return TRUE to continue analysis, FALSE to skip analysis of this
 * column.  In the TRUE case it must set the compute_stats and minrows fields,
 * and can optionally set extra_data to pass additional info to compute_stats.
 *
 * The compute_stats routine will be called after sample rows have been
 * gathered.  Aside from this struct, it is passed:
 *		attnum: attribute number within the supplied tuples
 *		tupDesc: tuple descriptor for the supplied tuples
 *		totalrows: estimated total number of rows in relation
 *		rows: an array of the sample tuples
 *		numrows: the number of sample tuples
 * Note that the passed attnum and tupDesc could possibly be different from
 * what one would expect by looking at the pg_attribute row.  It is important
 * to use these values for extracting attribute values from the given rows
 * (and not for any other purpose).
 *
 * compute_stats should set stats_valid TRUE if it is able to compute
 * any useful statistics.  If it does, the remainder of the struct holds
 * the information to be stored in a pg_statistic row for the column.  Be
 * careful to allocate any pointed-to data in anl_context, which will NOT
 * be CurrentMemoryContext when compute_stats is called.
 *----------
 */
typedef struct VacAttrStats
{
	/*
	 * These fields are set up by the main ANALYZE code before invoking
	 * the type-specific typanalyze function.
	 */
	Form_pg_attribute attr;		/* copy of pg_attribute row for column */
	Form_pg_type attrtype;		/* copy of pg_type row for column */
	MemoryContext anl_context;	/* where to save long-lived data */

	/*
	 * These fields must be filled in by the typanalyze routine,
	 * unless it returns FALSE.
	 */
	void (*compute_stats) (struct VacAttrStats *stats, int attnum,
						   TupleDesc tupDesc, double totalrows,
						   HeapTuple *rows, int numrows);
	int			minrows;		/* Minimum # of rows wanted for stats */
	void	   *extra_data;		/* for extra type-specific data */

	/*
	 * These fields are to be filled in by the compute_stats routine.
	 * (They are initialized to zero when the struct is created.)
	 */
	bool		stats_valid;
	float4		stanullfrac;	/* fraction of entries that are NULL */
	int4		stawidth;		/* average width of column values */
	float4		stadistinct;	/* # distinct values */
	int2		stakind[STATISTIC_NUM_SLOTS];
	Oid			staop[STATISTIC_NUM_SLOTS];
	int			numnumbers[STATISTIC_NUM_SLOTS];
	float4	   *stanumbers[STATISTIC_NUM_SLOTS];
	int			numvalues[STATISTIC_NUM_SLOTS];
	Datum	   *stavalues[STATISTIC_NUM_SLOTS];

	/*
	 * These fields are private to the main ANALYZE code and should not
	 * be looked at by type-specific functions.
	 */
	int			tupattnum;		/* attribute number within tuples */
} VacAttrStats;


/* State structure for vac_init_rusage/vac_show_rusage */
typedef struct VacRUsage
{
	struct timeval tv;
	struct rusage ru;
} VacRUsage;

/* Default statistics target (GUC parameter) */
extern int	default_statistics_target;


/* in commands/vacuum.c */
extern void vacuum(VacuumStmt *vacstmt);
extern void vac_open_indexes(Relation relation, int *nindexes,
				 Relation **Irel);
extern void vac_close_indexes(int nindexes, Relation *Irel);
extern void vac_update_relstats(Oid relid,
					BlockNumber num_pages,
					double num_tuples,
					bool hasindex);
extern void vacuum_set_xid_limits(VacuumStmt *vacstmt, bool sharedRel,
					  TransactionId *oldestXmin,
					  TransactionId *freezeLimit);
extern bool vac_is_partial_index(Relation indrel);
extern void vac_init_rusage(VacRUsage *ru0);
extern const char *vac_show_rusage(VacRUsage *ru0);
extern void vacuum_delay_point(void);

/* in commands/vacuumlazy.c */
extern void lazy_vacuum_rel(Relation onerel, VacuumStmt *vacstmt);

/* in commands/analyze.c */
extern void analyze_rel(Oid relid, VacuumStmt *vacstmt);

#endif   /* VACUUM_H */
