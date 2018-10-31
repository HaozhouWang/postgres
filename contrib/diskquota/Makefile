# contrib/diskquota/Makefile

MODULES = diskquota

EXTENSION = diskquota
DATA = diskquota--1.0.sql
OBJS = diskquota.o enforcement.o
REGRESS = test_diskquota
REGRESS_OPTS = --temp-config=test_diskquota.conf --temp-instance=/tmp/pg_diskquota_test
ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/worker_spi
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
