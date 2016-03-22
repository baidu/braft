CHECK_CC = cgcc
CHECK_CC_FLAGS = '$(CHECK_CC) -Wbitwise -Wno-return-void -no-compile $(ARCH)'

# Define a common prefix where binaries and docs install
#PREFIX ?= /usr

ROOTDIR = $(shell pwd)
DESTDIR ?= $(ROOTDIR)/output
PREFIX = $(DESTDIR)
BEC_EBS = 1

# Export VERSION and PREFIX so sub-make knows about them
export VERSION PREFIX DESTDIR

# Export the feature switches so sub-make knows about them
export ISCSI_RDMA
export CEPH_RBD
export GLFS_BD
export BEC_EBS

.PHONY: all
all: programs install-programs conf install-conf scripts install-scripts

# Targets for the /usr/sbin utilities
.PHONY: programs install-programs clean-programs
programs:
	$(MAKE) -C usr

install-programs:
	$(MAKE) -C usr install

clean-programs:
	$(MAKE) -C usr clean

# Targets for man pages and other documentation
.PHONY: doc install-doc clean-doc
doc:
	$(MAKE) -C doc

install-doc:
	$(MAKE) -C doc install

clean-doc:
	$(MAKE) -C doc clean

# Targets for scripts
.PHONY: scripts install-scripts clean-scripts
scripts:
	$(MAKE) -C scripts

install-scripts:
	$(MAKE) -C scripts install

clean-scripts:
	$(MAKE) -C scripts clean


# Targets for configuration stubs
.PHONY: conf install-conf clean-conf
conf:
	$(MAKE) -C conf

install-conf:
	$(MAKE) -C conf install

clean-conf:
	$(MAKE) -C conf clean

.PHONY: install
install: install-programs install-doc install-conf install-scripts

.PHONY: rpm
rpm:
	@./scripts/build-pkg.sh rpm

.PHONY: deb
deb:
	@./scripts/build-pkg.sh deb

.PHONY: clean
clean-pkg:
	rm -fr pkg

.PHONY: clean
clean: clean-programs clean-doc clean-conf clean-scripts clean-pkg

.PHONY:check
check: ARCH=$(shell sh script/checkarch.sh)
check:
	CC=$(CHECK_CC_FLAGS) $(MAKE) all

.PHONY:check32
check32: override ARCH=-m32
check32:
	CC=$(CHECK_CC_FLAGS) $(MAKE) all

.PHONY:check64
check64: override ARCH=-m64
check64:
	CC=$(CHECK_CC_FLAGS) $(MAKE) all

cscope:
	find -name '*.[ch]' > cscope.files
	cscope -bq
