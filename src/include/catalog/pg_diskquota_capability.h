/*-------------------------------------------------------------------------
 *
 * pg_diskquota_capability.h
 *	  definition of the "diskquota" system catalog (pg_diskquota_capability)
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_diskquota_capability.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_DISKQUOTA_CAPABILITY_H
#define PG_DISKQUOTA_CAPABILITY_H

#include "catalog/genbki.h"
#include "catalog/pg_diskquota_capability_d.h"

/* ----------------
 *	pg_diskquota_capability definition.  cpp turns this into
 *	typedef struct FormData_pg_diskquota_capability
 * ----------------
 */
CATALOG(pg_diskquota_capability,6123,DiskQuotaCapabilityRelationId)
{
	Oid             quotaid;     /* OID of the disk quota item with this capability  */
	int16           quotalimittype;   /* diskquota limit type id (DISKQUOTA_LIMIT_TYPE_XXX) */
	text            quotavalue;          /* diskquota limit (opaque type)  */
} FormData_pg_diskquota_capability;

/* ----------------
 *		Form_pg_diskquota_capability corresponds to a pointer to a tuple with
 *		the format of pg_diskquota_capability relation.
 * ----------------
 */
typedef FormData_pg_diskquota_capability *Form_pg_diskquota_capability;


#endif							/* PG_DISKQUOTACAPABILITY_H */
