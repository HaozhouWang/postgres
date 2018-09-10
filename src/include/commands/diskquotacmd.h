/*-------------------------------------------------------------------------
 *
 * foreign.h
 *	  support for disk quota in different level.
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 *
 * src/include/foreign/foreign.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef POSTGRES_DISKQUOTA_H
#define POSTGRES_DISKQUOTA_H

#include "postgres.h"
#include "nodes/parsenodes.h"


extern void CreateDiskQuota(CreateDiskQuotaStmt *stmt);
extern void DropDiskQuota(DropDiskQuotaStmt *stmt);


/* catalog access function */
extern char *GetDiskQuotaName(Oid quotaid);
extern Oid GetDiskQuotaOidByName(const char *name);


#endif //POSTGRES_DISKQUOTA_H
