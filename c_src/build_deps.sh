#!/bin/bash

set -e

INNO_VSN=1.0.3.5325

if [ `basename $PWD` != "c_src" ]; then
    pushd c_src
fi

BASEDIR="$PWD"

case "$1" in
    clean)
        rm -rf innodb embedded_innodb-$INNO_VSN
        ;;

    *)
        tar -xzf embedded_innodb-$INNO_VSN.tar.gz
        for x in patches/*; do
            patch -p0 < $x
        done

        # Rebar sets up a custom env that we really don't want
        unset CFLAGS CXXFLAGS LDFLAGS

        # Use the ERLANG_TARGET var to setup some additional flags as necessary
        if [[ $ERLANG_TARGET =~ darwin9.*-64$ ]]; then
            echo "Setting flags for Leopard w/ 64-bit Erlang..."
            export CFLAGS="-m64"
            export LDFLAGS="-arch x86_64"

        elif [[ $ERLANG_TARGET =~ darwin10.*-32$ ]]; then
            echo "Setting flags for Snow Leopard w/ 32-bit Erlang..."
            export CFLAGS="-m32"
            export LDFLAGS="-arch i386"

        elif [[ $ERLANG_TARGET =~ solaris.*-64$ ]]; then
            echo "Setting flags for Solaris w/ 64-bit Erlang..."
            export CFLAGS="-D_REENTRANT -m64"
        fi

        (cd embedded_innodb-$INNO_VSN && \
            ./configure --disable-shared --enable-static --with-pic \
                        --prefix=$BASEDIR/innodb && \
            make && make install)

        ;;
esac

