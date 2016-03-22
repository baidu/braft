mandir ?= $(PREFIX)/share/man
docdir ?= $(PREFIX)/share/doc/tgt

MANPAGES = manpages/tgtadm.8 manpages/tgt-admin.8 manpages/tgtimg.8 \
		manpages/tgt-setup-lun.8 manpages/tgtd.8

DOCS = README.iscsi README.iser \
	    README.lu_configuration README.mmc tmf.txt \
	    README.rbd

XSLTPROC = /usr/bin/xsltproc
XMLMAN = manpages/tgtd.8 manpages/tgtadm.8 manpages/tgtimg.8 \
		manpages/tgt-admin.8 manpages/targets.conf.5 \
		manpages/tgt-setup-lun.8
XMLHTML = htmlpages/tgtd.8.html htmlpages/tgtadm.8.html \
		htmlpages/tgtimg.8.html htmlpages/tgt-admin.8.html \
		htmlpages/targets.conf.5.html htmlpages/tgt-setup-lun.8.html

.PHONY:all
all: xmlman xmlhtml

.PHONY: install
install: $(MANPAGES) $(DOCS)
	install -d -m 755 $(mandir)/man8
	install -m 644 $(MANPAGES) $(mandir)/man8
	install -d -m 755 $(docdir)
	install -m 644 $(DOCS) $(docdir)
	install -d -m 755 $(docdir)/html
	install -m 644 $(XMLHTML) $(docdir)/html

.PHONY: clean
clean:
	rm -f $(XMLMAN) $(XMLHTML)
	-rm -f manpages htmlpages

manpages/tgtd.8: tgtd.8.xml
	-test -z "$(XSLTPROC)" || $(XSLTPROC) -o $@ http://docbook.sourceforge.net/release/xsl/current/manpages/docbook.xsl $<

htmlpages/tgtd.8.html: tgtd.8.xml
	-test -z "$(XSLTPROC)" || $(XSLTPROC) -o $@ http://docbook.sourceforge.net/release/xsl/current/html/docbook.xsl $<

manpages/tgtadm.8: tgtadm.8.xml
	-test -z "$(XSLTPROC)" || $(XSLTPROC) -o $@ http://docbook.sourceforge.net/release/xsl/current/manpages/docbook.xsl $<

htmlpages/tgtadm.8.html: tgtadm.8.xml
	-test -z "$(XSLTPROC)" || $(XSLTPROC) -o $@ http://docbook.sourceforge.net/release/xsl/current/html/docbook.xsl $<

manpages/tgt-admin.8: tgt-admin.8.xml
	-test -z "$(XSLTPROC)" || $(XSLTPROC) -o $@ http://docbook.sourceforge.net/release/xsl/current/manpages/docbook.xsl $<

htmlpages/tgt-admin.8.html: tgt-admin.8.xml
	-test -z "$(XSLTPROC)" || $(XSLTPROC) -o $@ http://docbook.sourceforge.net/release/xsl/current/html/docbook.xsl $<

manpages/tgtimg.8: tgtimg.8.xml
	-test -z "$(XSLTPROC)" || $(XSLTPROC) -o $@ http://docbook.sourceforge.net/release/xsl/current/manpages/docbook.xsl $<

htmlpages/tgtimg.8.html: tgtimg.8.xml
	-test -z "$(XSLTPROC)" || $(XSLTPROC) -o $@ http://docbook.sourceforge.net/release/xsl/current/html/docbook.xsl $<

manpages/targets.conf.5: targets.conf.5.xml
	-test -z "$(XSLTPROC)" || $(XSLTPROC) -o $@ http://docbook.sourceforge.net/release/xsl/current/manpages/docbook.xsl $<

htmlpages/targets.conf.5.html: targets.conf.5.xml
	-test -z "$(XSLTPROC)" || $(XSLTPROC) -o $@ http://docbook.sourceforge.net/release/xsl/current/html/docbook.xsl $<

manpages/tgt-setup-lun.8: tgt-setup-lun.8.xml
	-test -z "$(XSLTPROC)" || $(XSLTPROC) -o $@ http://docbook.sourceforge.net/release/xsl/current/manpages/docbook.xsl $<

htmlpages/tgt-setup-lun.8.html: tgt-setup-lun.8.xml
	-test -z "$(XSLTPROC)" || $(XSLTPROC) -o $@ http://docbook.sourceforge.net/release/xsl/current/html/docbook.xsl $<

xmlman: $(XMLMAN)

xmlhtml: $(XMLHTML)

