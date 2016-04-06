sbindir ?= $(PREFIX)/sbin
bindir ?= $(PREFIX)/bin
libdir ?= $(PREFIX)/lib
executor_conf ?= $(PREFIX)/conf

CFLAGS += -D__STDC_LIMIT_MACROS
ifneq ($(shell test -e /usr/include/linux/signalfd.h && echo 1),)
CFLAGS += -DUSE_SIGNALFD
endif

ifneq ($(shell test -e /usr/include/sys/timerfd.h && echo 1),)
CFLAGS += -DUSE_TIMERFD
endif

TGTD_OBJS += $(addprefix iscsi/, conn.o param.o session.o \
		iscsid.o target.o chap.o sha1.o md5.o transport.o iscsi_tcp.o \
		isns.o)

ifneq ($(CEPH_RBD),)
MODULES += bs_rbd.so
endif

ifneq ($(GLFS_BD),)
MODULES += bs_glfs.so
endif

ifneq ($(BEC_EBS),)
MODULES += bs_bec_ebs.so
endif

#ifneq ($(shell test -e /usr/include/sys/eventfd.h && test -e /usr/include/libaio.h && echo 1),)
#CFLAGS += -DUSE_EVENTFD
#TGTD_OBJS += bs_aio.o
#LIBS += -laio
#endif

ifneq ($(ISCSI_RDMA),)
TGTD_OBJS += iscsi/iser.o iscsi/iser_text.o
LIBS += -libverbs -lrdmacm
endif

#INCLUDES += -I. -I../../../../thirdsrc/tcmalloc/output/include
INCLUDES += -I. -I../..

CFLAGS += -D_GNU_SOURCE
CFLAGS += $(INCLUDES)
ifneq ($(DEBUG),)
CFLAGS += -g -O0 -ggdb -rdynamic
else
CFLAGS += -g -O2 -fno-strict-aliasing
endif
CFLAGS += -Wall -Wstrict-prototypes -fPIC
CFLAGS += -DTGT_VERSION=\"$(VERSION)$(EXTRAVERSION)\"
CFLAGS += -DBSDIR=\"./lib/backing-store\"
#CFLAGS += -fsanitize=address -fno-omit-frame-pointer
CFLAGS += -fno-omit-frame-pointer

#LIBS +=
#       ../../thirdsrc/tcmalloc/output/lib/libprofiler.a \
#		../../thirdsrc/tcmalloc/output/lib/libtcmalloc.a \
#		../../thirdsrc/tcmalloc/output/lib/libtcmalloc_and_profiler.a \
#		../../thirdsrc/tcmalloc/output/lib/libtcmalloc_debug.a \
#		../../thirdsrc/tcmalloc/output/lib/libtcmalloc_minimal.a \
#	    ../../thirdsrc/tcmalloc/output/lib/libtcmalloc_minimal_debug.a 
#LIBS += -lpthread -ldl -lstdc++ -lrt -lssl -lm
LIBS += -lpthread -ldl -lrt -lssl -lm -lbz2 -lcrypto
#LIBS += -lpthread -ldl -lrt -lssl -lm -lasan

PROGRAMS += tgtd tgtadm tgtimg
TGTD_OBJS += tgtd.o mgmt.o target.o scsi.o log.o driver.o util.o work.o \
		concat_buf.o parser.o spc.o sbc.o mmc.o osd.o scc.o smc.o \
		ssc.o libssc.o bs_rdwr.o bs_ssc.o \
		bs_null.o bs_sg.o bs.o libcrc32c.o bs_sheepdog.o

TGTD_DEP = $(TGTD_OBJS:.o=.d)

LDFLAGS = -Wl,-E,-rpath=./lib

.PHONY:all
all: $(PROGRAMS) $(MODULES)

tgtd: $(TGTD_OBJS)
	echo $(CC) $^ -o $@ $(LIBS)
	$(CC) $^ -o $@ $(LDFLAGS) $(LIBS)

-include $(TGTD_DEP)

TGTADM_OBJS = tgtadm.o concat_buf.o
TGTADM_DEP = $(TGTADM_OBJS:.o=.d)

tgtadm: $(TGTADM_OBJS)
#	$(CC) $^ -o $@
	$(CC) $^ -o $@ #-lasan

-include $(TGTADM_DEP)

TGTIMG_OBJS = tgtimg.o libssc.o libcrc32c.o
TGTIMG_DEP = $(TGTIMG_OBJS:.o=.d)

tgtimg: $(TGTIMG_OBJS)
#	$(CC) $^ -o $@
	$(CC) $^ -o $@ #-lasan

-include $(TGTIMG_DEP)

%.o: %.c
	$(CC)  -c $(CFLAGS) $*.c -o $*.o
	@$(CC) -MM $(CFLAGS) -MF $*.d -MT $*.o $*.c

%.so: %.c
	$(CC) -shared $(CFLAGS) $*.c -o $*.so

bs_rbd.so: bs_rbd.c
	$(CC) -shared $(CFLAGS) bs_rbd.c -o bs_rbd.so -lrados -lrbd

bs_glfs.so: bs_glfs.c
	$(CC) -I/usr/include/glusterfs/api -shared $(CFLAGS) bs_glfs.c -o bs_glfs.so -lgfapi

bs_bec_ebs.so: bs_bec_ebs.c
	$(CC) -I../.. -shared $(CFLAGS) bs_bec_ebs.c -Xlinker "-(" -lrt -lssl -ldl -lcrypto -lpthread -L../../ -lexecutor -Xlinker "-)" -o bs_bec_ebs.so

LIB_EXECUTOR=../../libexecutor.so
EXECUTOR_CONF=../../ebs.flags

.PHONY: install
install: $(PROGRAMS) $(MODULES)
	install -d -m 755 $(sbindir)
	install -m 755 $(PROGRAMS) $(sbindir)
	install -d 755 $(bindir)
	install -m 755 control $(bindir)
ifneq ($(MODULES),)
	install -d -m 755 $(libdir)/backing-store
	install -m 755 $(MODULES) $(libdir)/backing-store
	install -m 755 $(LIB_EXECUTOR) $(libdir)
endif
	install -d -m 755 $(executor_conf)
	install -m 755  $(EXECUTOR_CONF) $(executor_conf)

.PHONY: clean
clean:
	rm -f *.[od] *.so $(PROGRAMS) iscsi/*.[od] ibmvio/*.[od] fc/*.[od]
