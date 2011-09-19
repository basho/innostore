
# make sure there's a target location to install to
ifeq ($(RIAK),)
RIAK = "$(OTPROOT)"
endif

# get the latest tag from git
INNOSTORE_TAG = $(shell git describe --tags | head -n 1)
INSTALL_ERR = "$(RIAK) doesn't exist or isn't writable."
TARGET_ERR = "Error: no target location defined - set RIAK or OTPROOT"
INSTALL_MSG = "Installing to $(RIAK)/lib..."

all:    # Innostore only works with the SMP runtime, force it on uniprocessors
	ERL_FLAGS="-smp enable" ./rebar compile eunit verbose=1

clean: distclean
	./rebar clean

install:
	@[ "$(RIAK)" == "" ] && echo "$(TARGET_ERR)" && exit 2 || echo "$(INSTALL_MSG)"
	@[ -w "$(RIAK)" ] && cp -r `pwd` "$(RIAK)/lib" || echo "$(INSTALL_ERR)"
	@:

distdir:
	$(if $(findstring tip,$(INNOSTORE_TAG)),$(error "You can't generate a release tarball from tip"))
	mkdir distdir
	git clone "git://github.com/basho/innostore" distdir/innostore-clone
	cd distdir/innostore-clone; \
	git checkout $(INNOSTORE_TAG); \
	git archive --format=tar  -o ../$(INNOSTORE_TAG) $(INNOSTORE_TAG)

dist $(INNOSTORE_TAG).tar.gz: distdir
	cd distdir; \
	tar czf ../$(INNOSTORE_TAG).tar.gz $(INNOSTORE_TAG)

distclean:
	rm -rf $(INNOSTORE_TAG).tar.gz distdir

