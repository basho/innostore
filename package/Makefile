## Generate different PKGNAME's for each platform
OS		= $(shell uname -s)
KERNEL		= $(shell uname -r)
ifeq ($(OS),Linux)
PLATFORM        = $(shell uname -m)
PKGER		= $(shell cat /etc/redhat-release 2> /dev/null)
ifeq ($(PKGER),)
# Debian
PKGNAME         = $(RELEASE_NAME)-$(RELEASE)-deb-$(PLATFORM).tar.gz
else
# Redhat
PKGNAME         = $(RELEASE_NAME)-$(RELEASE)-rh-$(PLATFORM).tar.gz
endif
endif
ifeq ($(OS),SunOS)
# Solaris
PKG             = BASHO$(REPO)
PLATFORM        = $(shell uname -p)
SOLARIS_VER    ?= $(shell echo "$(KERNEL)" | sed -e 's/^5\.//')
DISTRO		= $(shell awk '{ if (NR==1) print $$1; };' /etc/release)
PKGNAME         = $(PKG)-$(INNOSTORE_TAG)-$(RELEASE)-$(DISTRO)$(SOLARIS_VER)-$(PLATFORM).tar.gz
endif
ifeq ($(OS),Darwin)
PLATFORM        = $(shell uname -m)
PKGNAME         = $(RELEASE_NAME)-$(RELEASE)-osx-$(PLATFORM).tar.gz
endif

BUILDDIR = builddir
PKGDIR   = packages

$(RELEASE_NAME).tar.gz: ../$(RELEASE_NAME).tar.gz
	ln -s $< $@

pkgclean:
	rm -rf $(RELEASE_NAME).tar.gz ${BUILDDIR} ${PKGDIR}

pkgcheck:
	$(if $(INNOSTORE_TAG),,$(error "You can't generate a release tarball from a non-tagged revision. Run 'git checkout <tag>', then 'make dist'"))
	$(if $(RELEASE),,$(error "You must provide a package release number via RELEASE= on the command line"))
	@echo "Packaging \"$(INNOSTORE_TAG)\""

build: buildrel
	@echo "Building package $(PKGNAME)"
	mkdir -p ${PKGDIR}
	cd $(BUILDDIR)/$(RELEASE_NAME) && \
		mkdir -p $(RELEASE_NAME) && \
		cp -R ebin $(RELEASE_NAME) && \
		cp -R priv $(RELEASE_NAME) && \
		cp -R src  $(RELEASE_NAME) && \
		tar -czf ../../${PKGDIR}/$(PKGNAME) $(RELEASE_NAME)

buildrel: $(BUILDDIR)/$(RELEASE_NAME)
	cd $^ && $(MAKE)

$(BUILDDIR)/$(RELEASE_NAME): $(BUILDDIR) $(RELEASE_NAME).tar.gz
	tar xz -C $(BUILDDIR) -f $(RELEASE_NAME).tar.gz

$(BUILDDIR):
	mkdir -p $@

package: pkgcheck build



