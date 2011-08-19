INNOSTORE_TAG = $(shell git describe --tags)

.PHONY: rel deps package pkgclean

all: deps compile test

compile:
	./rebar compile

clean:
	./rebar clean

test:
	./rebar eunit

install:
	./rebar install

# Release tarball creation
# Generates a tarball that includes all the deps sources so no checkouts are necessary
archivegit = git archive --format=tar --prefix=$(1)/ HEAD | (cd $(2) && tar xf -)
archivehg = hg archive $(2)/$(1)
archive = if [ -d ".git" ]; then \
		$(call archivegit,$(1),$(2)); \
	    else \
		$(call archivehg,$(1),$(2)); \
	    fi

buildtar = mkdir distdir && \
		 git clone . distdir/$(REPO)-clone && \
		 cd distdir/$(REPO)-clone && \
		 git checkout $(INNOSTORE_TAG) && \
		 $(call archive,$(INNOSTORE_TAG),..) && \
		 mkdir ../$(INNOSTORE_TAG)/deps && \
		 make deps; \
		 for dep in deps/*; do cd $${dep} && $(call archive,$${dep},../../../$(INNOSTORE_TAG)); cd ../..; done
					 
distdir:
	$(if $(INNOSTORE_TAG), $(call buildtar), $(error "You can't generate a release tarball from a non-tagged revision. Run 'git checkout <tag>', then 'make dist'"))

dist $(INNOSTORE_TAG).tar.gz: distdir
	cd distdir; \
	tar czf ../$(INNOSTORE_TAG).tar.gz $(INNOSTORE_TAG)

ballclean:
	rm -rf $(INNOSTORE_TAG).tar.gz distdir

package: dist
	$(MAKE) -C package package

pkgclean:
	$(MAKE) -C package pkgclean

export INNOSTORE_TAG PKG_VERSION REPO REVISION
