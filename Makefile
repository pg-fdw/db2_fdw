EXTENSION    = db2_fdw
EXTVERSION   = $(shell grep default_version $(EXTENSION).control | sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")
MODULE_big   = db2_fdw
OBJS         = source/db2_fdw.o\
               source/db2_deparse.o\
               source/db2_de_serialize.o\
               source/db2GetForeignPlan.o\
               source/db2GetForeignPaths.o\
               source/db2GetForeignUpperPaths.o\
               source/db2GetForeignJoinPaths.o\
               source/db2AnalyzeForeignTable.o\
               source/db2ExplainForeignScan.o\
               source/db2BeginForeignScan.o\
               source/db2IterateForeignScan.o\
               source/db2EndForeignScan.o\
               source/db2ReScanForeignScan.o\
               source/db2AddForeignUpdateTargets.o\
               source/db2PlanForeignModify.o\
               source/db2BeginForeignModifyCommon.o\
               source/db2BeginForeignModify.o\
               source/db2BeginForeignInsert.o\
               source/db2ExecForeignInsert.o\
               source/db2ExecForeignUpdate.o\
               source/db2ExecForeignDelete.o\
               source/db2ExecForeignTruncate.o\
               source/db2EndForeignModifyCommon.o\
               source/db2EndForeignModify.o\
               source/db2EndForeignInsert.o\
               source/db2ExplainForeignModify.o\
               source/db2IsForeignRelUpdatable.o\
               source/db2ImportForeignSchema.o\
               source/db2ImportForeignSchemaData.o\
               source/db2GetFdwState.o\
               source/db2GetForeignRelSize.o\
               source/db2ReAllocFree.o\
               source/db2SetHandlers.o\
               source/db2Callbacks.o\
               source/db2Debug.o\
               source/db2ServerVersion.o\
               source/db2ClientVersion.o\
               source/db2GetShareFileName.o\
               source/db2_fdw_utils.o\
               source/db2AllocEnvHdl.o\
               source/db2FreeEnvHdl.o\
               source/db2AllocConnHdl.o\
               source/db2AllocStmtHdl.o\
               source/db2FreeStmtHdl.o\
               source/db2GetSession.o\
               source/db2Describe.o\
               source/db2PrepareQuery.o\
               source/db2BindParameter.o\
               source/db2ExecuteQuery.o\
               source/db2ExecuteInsert.o\
               source/db2GetForeignModifyBatchSize.o \
               source/db2ExecForeignBatchInsert.o \
               source/db2ExecuteTruncate.o\
               source/db2FetchNext.o\
               source/db2GetLob.o\
               source/db2SetSavepoint.o\
               source/db2EndSubtransaction.o\
               source/db2EndTransaction.o\
               source/db2CloseStatement.o\
               source/db2Cancel.o\
               source/db2CheckErr.o\
               source/db2CloseConnections.o\
               source/db2Shutdown.o\
               source/db2CopyText.o\
               source/db2IsStatementOpen.o\
               source/db2PlanDirectModify.o\
               source/db2BeginDirectModify.o\
               source/db2IterateDirectModify.o\
               source/db2EndDirectModify.o\
               source/db2_utils.o
RELEASE      = $(shell grep default_version $(EXTENSION).control | sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")
DATA         = $(wildcard sql/*--*.sql)
DOCS         = $(wildcard doc/*.md)
TESTS        = $(wildcard test/sql/*.sql)
REGRESS      = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test
#
# Uncoment the MODULES line if you are adding C files
# to your extention.
#
#MODULES         = $(patsubst %.c,%,$(wildcard src/*.c))
PG_CPPFLAGS     = -fPIC -I$(DB2_HOME)/include -I./include
SHLIB_LINK      = -fPIC -L$(DB2_HOME)/lib64 -L$(DB2_HOME)/bin  -ldb2
PG_CONFIG      ?= pg_config
PGXS           := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
PG_MAJOR       := $(firstword $(subst ., ,$(VERSION)))
GPG_PASSPHRASE := $(shell cat ~/.gnupg/rpm-passphrase)

#checkin: clean
#	git remote set-url origin git@github.com:Living-Mainframe/db2_fdw.git
#	git add --all
#	git commit -m "postgres 17 "
#	git commit -m "`date`"
#	git push -u origin master

#reset:
#	git reset --hard origin/master

archive:
	git archive --format zip --prefix=db2_fdw-$(RELEASE)/ --output ../db2_fdw-$(RELEASE).zip master

rpms:
#	mkdir -p ~/rpms
	sed -e "s/GPG_PASSPHRASE/$(GPG_PASSPHRASE)/" -e "s/PG_MAJOR/$(PG_MAJOR)/" -e "s/FDW_RELEASE/$(RELEASE)/" ./config/makemacros >~/.rpmmacros
	rpmbuild -ba --sign ./$(EXTENSION).spec
	nexusupld.sh -s ~/rpmbuild/RPMS/x86_64/postgresql$(PG_MAJOR)-db2_fdw-$(RELEASE)-1.el8.x86_64.rpm -p /db2_fdw/el8/x86_64/Package
	rm -rf ~/rpmbuild
	rm -rf ~/.rpmmacros
