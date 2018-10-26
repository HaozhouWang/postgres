# contrib/worker_spi/Makefile

MODULES = worker_spi

EXTENSION = worker_spi
DATA = worker_spi--1.0.sql
REGRESS = test_worker_spi
REGRESS_OPTS = --temp-config=test_worker_spi.conf --temp-install=/tmp/pg_worker_spi_test --extra-install=contrib/worker_spi --top-builddir=../../
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
