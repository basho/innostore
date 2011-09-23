#!/bin/bash

set -e

INNO_VSN=1.0.6.6750

if [ `basename $PWD` != "c_src" ]; then
    pushd c_src
fi

BASEDIR="$PWD"

case "$1" in
    clean)
        rm -rf innodb embedded_innodb-$INNO_VSN
        ;;

    *)
        test -f innodb/lib/libinnodb.a && exit 0

        tar -xzf embedded_innodb-$INNO_VSN.tar.gz
        for x in patches/*; do
            echo $x
            patch -p0 < $x
        done

        (cd embedded_innodb-$INNO_VSN && \
            ./configure --disable-shared --enable-static --with-pic \
                        --prefix=$BASEDIR/innodb && \
            make && make install)

        ;;
esac

