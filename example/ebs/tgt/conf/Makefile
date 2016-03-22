confdir ?= $(PREFIX)/etc

EXAMPLES = targets.conf.example targets.conf.vtl.L700 targets.conf.vtl.MSL2024

.PHONY: all
all:

.PHONY: install
install:
	install -d -m 755 $(confdir)/tgt
	if [ ! -f $(confdir)/tgt/targets.conf ] ; then \
		install -m 644 targets.conf $(confdir)/tgt ; \
	fi
	install -d -m 755 $(confdir)/tgt/examples
	for f in $(EXAMPLES) ; do \
		install -m 644 examples/$$f $(confdir)/tgt/examples ;\
	done
	install -d $(confdir)/tgt/conf.d

.PHONY: clean
clean:

