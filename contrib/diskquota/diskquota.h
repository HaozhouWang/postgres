#ifndef DISK_QUOTA_H
#define DISK_QUOTA_H

typedef enum
{
	NAMESPACE_QUOTA,
	ROLE_QUOTA
} QuotaType;

/* enforcement interface*/
extern void init_disk_quota_enforcement(void);

/* quota model interface*/
extern void init_disk_quota_shmem(void);
extern void init_disk_quota_model(void);
extern void refresh_disk_quota_model(bool force);
extern bool quota_check_common(Oid reloid);
extern void init_disk_quota_hook(void);

extern int   worker_spi_naptime;
extern char *worker_spi_monitored_database_list;
extern int   worker_spi_max_active_tables;

#endif				/* PG_QUOTA_H */
