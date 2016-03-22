sbindir ?= $(PREFIX)/sbin

SCRIPTS += tgt-setup-lun tgt-admin

.PHONY: all
all:

.PHONY: install
install: $(SCRIPTS)
	install -d -m 755 $(sbindir)
	install -m 755 $(SCRIPTS) $(sbindir)

.PHONY: clean
clean:

