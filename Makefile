REPO            ?= innostore
INNOSTORE_TAG    = $(shell git describe --tags)
RELEASE_NAME     = $(REPO)-$(INNOSTORE_TAG)

.PHONY: package pkgclean

all: compile

compile:
	./rebar compile

clean:
	./rebar clean

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

dist $(REPO)-$(INNOSTORE_TAG).tar.gz: distdir
	cd distdir; \
	tar czf ../$(RELEASE_NAME).tar.gz $(RELEASE_NAME)

ballclean:
	rm -rf $(RELEASE_NAME).tar.gz distdir

package: dist
	$(MAKE) -C package package

pkgclean:
	$(MAKE) -C package pkgclean

export INNOSTORE_TAG REPO RELEASE_NAME 
