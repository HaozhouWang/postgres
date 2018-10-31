#ifndef DISK_QUOTA_H
#define DISK_QUOTA_H

extern bool quota_check_common(Oid reloid);

extern void init_disk_quota_enforcement(void);


#endif				/* PG_QUOTA_H */
