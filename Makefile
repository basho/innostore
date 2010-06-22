INNOSTORE_TAG		= $(shell hg identify -t)

all:    # Innostore only works with the SMP runtime, force it on uniprocessors
	ERL_FLAGS="-smp enable" ./rebar compile eunit verbose=1

clean: distclean
	./rebar clean

install:
	./rebar install

distdir:
	$(if $(findstring tip,$(INNOSTORE_TAG)),$(error "You can't generate a release tarball from tip"))
	mkdir distdir
	hg clone -u $(INNOSTORE_TAG) . distdir/innostore-clone
	cd distdir/innostore-clone; \
	hg archive ../$(INNOSTORE_TAG)

dist $(INNOSTORE_TAG).tar.gz: distdir
	cd distdir; \
	tar czf ../$(INNOSTORE_TAG).tar.gz $(INNOSTORE_TAG)

distclean:
	rm -rf $(INNOSTORE_TAG).tar.gz distdir

