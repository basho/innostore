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

        (cd embedded_innodb-$INNO_VSN && \
            ./configure --disable-shared --enable-static \
                        --prefix=$BASEDIR/innodb && \
            make && make install)

        ;;
esac
        