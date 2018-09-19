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

#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_diskquota.h"
#include "commands/diskquotacmd.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

void CreateDiskQuota(CreateDiskQuotaStmt *stmt)
{
	Relation	disk_quota_rel;
	Datum		quota_values[Natts_pg_diskquota];
	bool		quota_nulls[Natts_pg_diskquota];
	Oid         db_object_oid = InvalidOid;
	Oid         disk_quota_oid = InvalidOid;
	HeapTuple	tuple;
	Oid			ownerId;
	ListCell   *cell;
	bool        quota_set = false;


	disk_quota_rel = heap_open(DiskQuotaRelationId, RowExclusiveLock);


	/* Must be super user */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to create disk quota \"%s\"",
						stmt->quotaname),
				 errhint("Must be superuser to create a disk quota.")));

	/* For dependency only */
	ownerId = GetUserId();

	/* Check that there is no disk quota entry with the same name */
	if (OidIsValid(GetDiskQuotaOidByName(stmt->quotaname)))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("disk quota entry \"%s\" already exists",
						stmt->quotaname)));

	/*
	 * Insert tuple into pg_diskquota
	 */
	memset(quota_values, 0, sizeof(quota_values));
	memset(quota_nulls, false, sizeof(quota_nulls));

	quota_values[Anum_pg_diskquota_quotaname - 1] =
		DirectFunctionCall1(namein, CStringGetDatum(stmt->quotaname));

	quota_values[Anum_pg_diskquota_quotatype - 1] = Int16GetDatum((int16) stmt->dbobjtype);

	/*
	 * Search for related database object OID
	 */

	switch (stmt->dbobjtype)
	{
		case DISK_QUOTA_TABLE:
		{
			db_object_oid = RangeVarGetRelidExtended(stmt->table, NoLock, RVR_MISSING_OK, NULL, NULL);

			if (!OidIsValid(db_object_oid)){
				if (stmt->table->schemaname)
				{
					ereport(ERROR, (errmsg("could not create disk quota, TABLE '%s.%s' does not exist",
					                       stmt->table->schemaname, stmt->table->relname)));
				}
				else
				{
					ereport(ERROR, (errmsg("could not create disk quota, TABLE '%s' does not exist",
										                       stmt->table->relname)));
				}

			}
			break;
		}
		case DISK_QUOTA_SCHEMA:
		{
			db_object_oid = LookupNamespaceNoError(stmt->objname);

			if (!OidIsValid(db_object_oid))
			{

				ereport(ERROR, (errmsg("could not create disk quota, SCHEMA '%s' does not exist",
				                       stmt->objname)));

			}
			break;

		}
		case DISK_QUOTA_USER:
		{
			db_object_oid = get_role_oid(stmt->objname, true);

			if (!OidIsValid(db_object_oid))
			{
				ereport(ERROR, (errmsg("could not create disk quota, USER '%s' does not exist",
				                       stmt->objname)));

			}
			break;

		}
		default:
		{
			ereport(ERROR, (errmsg("could not create disk quota, Unknown DB OBJECT TYPE")));
		}
	}

	quota_values[Anum_pg_diskquota_quotatargetoid - 1] = ObjectIdGetDatum(db_object_oid);

	foreach(cell, stmt->options)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

		if (strcmp(def->defname, "quota") == 0 && quota_set == false)
		{
			int limitinMB;
			const char *hintmsg;
			if (!parse_int(strVal(def->arg), &limitinMB, GUC_UNIT_MB, &hintmsg))
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid value for integer option \"%s\": %s",
								def->defname, strVal(def->arg))));	
			}
			quota_values[Anum_pg_diskquota_quotalimit- 1] = limitinMB;
			quota_set = true;
		}
		else
		{
			if (quota_set)
			{
				ereport(ERROR,
						(errmsg("duplicate quota settings")));

			} else
			{
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("unknown disk quota option %s", def->defname)));
			}

		}
	}


	tuple = heap_form_tuple(disk_quota_rel->rd_att, quota_values, quota_nulls);

	disk_quota_oid = CatalogTupleInsert(disk_quota_rel, tuple);

	heap_freetuple(tuple);

	recordDependencyOnOwner(DiskQuotaRelationId, disk_quota_oid, ownerId);

	if (!quota_set) {
		ereport(ERROR,
			(errmsg("quota is not set in option"),
			 errhint("Add quota='size' in option")));
	}

	heap_close(disk_quota_rel, RowExclusiveLock);
}

void DropDiskQuota(DropDiskQuotaStmt *stmt)
{
	Relation	disk_quota_rel;
	HeapTuple	disk_quota_tuple;


	disk_quota_rel = heap_open(DiskQuotaRelationId, RowExclusiveLock);

	disk_quota_tuple = SearchSysCache1(DISKQUOTANAME, CStringGetDatum(stmt->quotaname));

	if (!HeapTupleIsValid(disk_quota_tuple)) {
		if (stmt->missing_ok)
			return;
		else
		{
			ereport(ERROR,
				(errmsg("disk quota %s does not exist", stmt->quotaname)));
		}
	}

	CatalogTupleDelete(disk_quota_rel, &disk_quota_tuple->t_self);

	ReleaseSysCache(disk_quota_tuple);

	heap_close(disk_quota_rel, RowExclusiveLock);
}

char *GetDiskQuotaName(Oid quotaid)
{
	return "none";
}

Oid GetDiskQuotaOidByName(const char *name)
{
	return GetSysCacheOid1(DISKQUOTANAME, CStringGetDatum(name));
}
