#ifndef ACTIVE_TABLE_H
#define ACTIVE_TABLE_H

#include "storage/lwlock.h"

/* Cache to detect the active table list */
typedef struct DiskQuotaActiveTableFileEntry
{
	Oid         dbid;
	Oid         relfilenode;
	Oid         tablespaceoid;
} DiskQuotaActiveTableFileEntry;

typedef struct DiskQuotaActiveTableEntry
{
	Oid     tableoid;
	Size    tablesize;
} DiskQuotaActiveTableEntry;


extern HTAB* get_active_tables();
extern void init_active_table_hook(void);

#endif
