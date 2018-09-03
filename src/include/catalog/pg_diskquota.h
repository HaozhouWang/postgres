/*-------------------------------------------------------------------------
 *
 * pg_diskquota.h
 *	  definition of the "diskquota" system catalog (pg_diskquota)
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_diskquota.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_DISKQUOTA_H
#define PG_DISKQUOTA_H

#include "catalog/genbki.h"
#include "catalog/pg_diskquota_d.h"
/* ----------------
 *	pg_diskquota definition.  cpp turns this into
 *	typedef struct FormData_pg_diskquota
 * ----------------
 */
typedef enum DiskQuotaType
{
        DISKQUOTA_TYPE_UNKNOWN = 0,

        DISKQUOTA_TYPE_TABLE,

        DISKQUOTA_TYPE_SCHEMA,

	DISKQUOTA_TYPE_DATABASE,

	DISKQUOTA_TYPE_ROLE,

} DiskQuotaType;

CATALOG(pg_diskquota,6122,DiskQuotaRelationId)
{
	NameData	quotaname;		/* diskquota name */       
	int16		quotatype;		/* diskquota type name */
	Oid		quotatargetoid;		/* diskquota target db object oid*/
} FormData_pg_diskquota;

/* ----------------
 *	Form_pg_diskquota corresponds to a pointer to a tuple with
 *	the format of pg_diskquota relation.
 * ----------------
 */
typedef FormData_pg_diskquota *Form_pg_diskquota;


/* ----------------
 *      pg_diskquotacapability definition.  cpp turns this into
 *      typedef struct FormData_pg_diskquotacapability
 * ----------------
 */

typedef enum DiskQuotaLimitType
{
        DISKQUOTA_LIMIT_TYPE_UNKNOWN = 0,

        DISKQUOTA_LIMIT_TYPE_EXPECTED,		/* expected quota limit on a db object*/

	DISKQUOTA_LIMIT_TYPE_REDZONE,		/* redzone limit, warning when disk usage of a db object is high*/

} DiskQuotaLimitType;

#endif							/* PG_DISKQUOTA_H */
