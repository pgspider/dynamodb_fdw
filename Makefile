DIRJANSSON = ./jansson
OBJS_JANSSON = 	$(DIRJANSSON)/dump.o \
				$(DIRJANSSON)/error.o \
				$(DIRJANSSON)/hashtable.o \
				$(DIRJANSSON)/hashtable_seed.o \
				$(DIRJANSSON)/load.o \
				$(DIRJANSSON)/memory.o \
				$(DIRJANSSON)/pack_unpack.o \
				$(DIRJANSSON)/strbuffer.o \
				$(DIRJANSSON)/strconv.o \
				$(DIRJANSSON)/utf.o \
				$(DIRJANSSON)/value.o

JANSSON_CFLAGS = -DHAVE_STDINT_H=1 -Wno-suggest-attribute=format

MODULE_big = dynamodb_fdw
OBJS = $(OBJS_JANSSON) shippable.o deparse.o dynamodb_query.o dynamodb_impl.o dynamodb_fdw.o connection.o option.o

PGFILEDESC = "dynamodb_fdw - foreign data wrapper for DynamoDB"

SHLIB_LINK = -lm -lstdc++ -laws-cpp-sdk-core -laws-cpp-sdk-dynamodb

EXTENSION = dynamodb_fdw
DATA = dynamodb_fdw--1.0.sql dynamodb_fdw--1.0--1.1.sql

REGRESS = server_options connection_validation dynamodb_fdw pushdown extra/delete extra/insert extra/json extra/jsonb extra/select extra/update 

# EXTRA_CLEAN = sql/parquet_fdw.sql expected/parquet_fdw.out

# dynamodb_impl.cpp requires C++ 11.
PG_CFLAGS += -I$(DIRJANSSON) $(JANSSON_CFLAGS)
PG_CXXFLAGS += -I$(DIRJANSSON) $(JANSSON_CFLAGS) -std=c++11
# override PG_CXXFLAGS and PG_CFLAGS
ifdef CCFLAGS
	override PG_CXXFLAGS += $(CCFLAGS)
	override PG_CFLAGS += $(CCFLAGS)
endif

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/dynamodb_fdw
top_builddir = ../..

# PostgreSQL uses link time optimization option which may break compilation
# (this happens on travis-ci). Redefine COMPILE.cxx.bc without this option.
COMPILE.cxx.bc = $(CLANG) -xc++ -Wno-ignored-attributes $(BITCODE_CXXFLAGS) $(CPPFLAGS) -emit-llvm -c

include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk

# A hurdle to use common compiler flags when building bytecode from C++
# files. should be not unnecessary, but src/Makefile.global omits passing those
# flags for an unnknown reason.
%.bc : %.cpp
	$(COMPILE.cxx.bc) $(CXXFLAGS) $(CPPFLAGS)  -o $@ $<
endif

ifdef REGRESS_PREFIX
REGRESS_PREFIX_SUB = $(REGRESS_PREFIX)
else
REGRESS_PREFIX_SUB = $(VERSION)
endif

REGRESS := $(addprefix $(REGRESS_PREFIX_SUB)/,$(REGRESS))
$(shell mkdir -p results/$(REGRESS_PREFIX_SUB)/extra)
