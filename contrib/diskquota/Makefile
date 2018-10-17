# contrib/diskquota/Makefile

MODULE_big = diskquota

EXTENSION = diskquota
DATA = diskquota--1.0.sql
SRCDIR = ./
FILES = $(shell find $(SRCDIR) -type f -name "*.c")
OBJS = diskquota.o enforcement.o quotamodel.o activetable.o

REGRESS = dummy
REGRESS_OPTS = --temp-config=test_diskquota.conf --temp-instance=/tmp/pg_diskquota_test  --schedule=diskquota_schedule
ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/diskquota 
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
