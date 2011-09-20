
ifeq ($(RIAK),)
RIAK = "$(OTPROOT)"
endif

REPO            ?= innostore
INNOSTORE_TAG    = $(shell git describe --tags)
RELEASE_NAME     = $(REPO)-$(INNOSTORE_TAG)
INSTALL_ERR      = "$(RIAK) doesn't exist or isn't writable."
TARGET_ERR       = "Error: no target location defined - set RIAK or OTPROOT"
INSTALL_MSG      = "Installing to $(RIAK)/lib..."

.PHONY: package pkgclean distclean

all: compile

compile:
	./rebar compile

clean:
	./rebar clean

deps:
	./rebar get-deps

install:
	@[ "$(RIAK)" == "" ] && echo "$(TARGET_ERR)" && exit 2 || echo "$(INSTALL_MSG)"
	@[ -w "$(RIAK)" ] && cp -r `pwd` "$(RIAK)/lib" || echo "$(INSTALL_ERR)"
	@:

test:
	./rebar eunit

# Release tarball creation
# Generates a tarball that includes all the deps sources so no checkouts are necessary
archivegit = git archive --format=tar --prefix=$(1)/ HEAD | (cd $(2) && tar xf -)
archive = $(call archivegit,$(1),$(2))

buildtar = mkdir distdir && \
		 git clone . distdir/$(REPO)-clone && \
		 cd distdir/$(REPO)-clone && \
		 git checkout $(INNOSTORE_TAG) && \
		 $(call archive,$(RELEASE_NAME),..) && \
		 mkdir ../$(RELEASE_NAME)/deps && \
		 make deps; \
		 for dep in deps/*; do cd $${dep} && $(call archive,$${dep},../../../$(RELEASE_NAME)); cd ../..; done

distdir:
	$(if $(INNOSTORE_TAG), $(call buildtar), $(error "You can't generate a release tarball from a non-tagged revision. Run 'git checkout <tag>', then 'make dist'"))

dist $(RELEASE_NAME).tar.gz: distdir
	cd distdir; \
	tar czf ../$(RELEASE_NAME).tar.gz $(RELEASE_NAME)

distclean: ballclean

ballclean:
	rm -rf $(RELEASE_NAME).tar.gz distdir

package: dist
	$(MAKE) -C package package

pkgclean:
	$(MAKE) -C package pkgclean

export INNOSTORE_TAG REPO RELEASE_NAME 
