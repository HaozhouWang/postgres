/*-------------------------------------------------------------------------
 *
 * dbsize.h
 *	  Definitions for dbsize functions.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/dbsize.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DBSIZE_H
#define DBSIZE_H

int64 calculate_total_relation_size_by_oid(Oid relOid);
#endif							/* DBSIZE_H */
