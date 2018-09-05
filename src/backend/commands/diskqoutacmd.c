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

#include "commands/diskquotacmd.h"

void CreateDiskQuota(CreateDiskQuotaStmt *stmt)
{
	elog(WARNING, "Receive Disk Quota Create, %s, %d", stmt->quotaname, stmt->dbobjtype);

}

void DropDiskQuota(DropDiskQuotaStmt *stmt)
{
	elog(WARNING, "Receive Disk Quota Delete, %s", stmt->quotaname);
}

char *GetDiskQuotaName(Oid quotaid)
{
	return "none";
}

Oid GetDiskQuotaOidByName(const char *name, bool missing_ok)
{
	return (Oid) 0;
}