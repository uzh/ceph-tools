#!/bin/bash

RUNS=1
PROG="$(basename $0)"
NUMCLIENTS=16
RUNTIME=$[5*60]

usage () {
cat <<EOF
Usage: $PROG -n N [-r N] [-c N]

Runs FIO tests on SC hosts using a predefined set of parameters

Options:

  --help, -h            Print this help text.
  -n, --test-number N   Test number    
  -r, --runs N          Number of runs
  -c, --clients N       Number of clients (must be multiple of 4)
  -t, --runtime N       Runtime in seconds
EOF
}

die () {
  rc="$1"
  shift
  (echo -n "$PROG: ERROR: ";
      if [ $# -gt 0 ]; then echo "$@"; else cat; fi) 1>&2
  exit $rc
}

short_opts='hvn:r:c:t:'
long_opts='help,verbose,test-number:,runs:,clients:,runtime:'

getopt -T > /dev/null
rc=$?
if [ "$rc" -eq 4 ]; then
    # GNU getopt
    args=$(getopt --name "$PROG" --shell sh -l "$long_opts" -o "$short_opts" -- "$@")
    if [ $? -ne 0 ]; then
        die 1 "Type '$PROG --help' to get usage information."
    fi
    # use 'eval' to remove getopt quoting
    eval set -- $args
else
    # old-style getopt, use compatibility syntax
    args=$(getopt "$short_opts" "$@")
    if [ $? -ne 0 ]; then
        die 1 "Type '$PROG --help' to get usage information."
    fi
    set -- $args
fi

while [ $# -gt 0 ]; do
    case "$1" in
        --help|-h) usage; exit 0 ;;
        -v|--verbose) VERBOSITY=$[VERBOSITY+1];;
        -n|--test-number)
            shift
            TESTNUM=$1
            ;;
        -r|--runs)
            shift
            RUNS=$1
            ;;
        -c|--clients)
            shift
            NUMCLIENTS=$1
            ;;
        -t|--runtime)
            shift
            RUNTIME=$1
            ;;
        --) shift; break ;;
    esac
    shift
done

[ -z "$TESTNUM" ] && die 1 "Missing required option '-n'"

BASETESTDIR=fio-test-$TESTNUM

top=$(printf '%02d' $[$NUMCLIENTS / 4])
PDSH="pdsh -w node-k5-01-[01-${top}],node-k4-01-[01-${top}],node-l3-01-[01-${top}],node-l6-01-[01-${top}]"

rest=$[$NUMCLIENTS % 4]
if [ ${rest} -ne 0 ]; then
    echo "Number of clients is not a multiple of 4; rounding down to nearest multiple of 4." 1>&2
fi

#OSDPDSH="pdsh -w osd-k2-[06,11,16,21,26,31,36],osd-k3-30,osd-k5-[06,11],osd-k6-30,osd-k7-[06,11,16,21,26,31,36],osd-l2-[06,11,16,21,26,31,36],osd-l3-[30,35],osd-l4-[06,11],osd-l6-30,osd-l7-[06,11,16,21,26,31],vhp-k6-[38-39],vhp-l6-[37-38],vhp-l7-39"
OSDPDSH="pdsh -w osd-k2-[06,11,16,21,26,31,36],osd-k3-30,osd-k5-[06,11],osd-k6-30,osd-k7-[06,16,21,26,31,36],osd-l2-[06,11,16,21,26,31,36],osd-l3-[30,35],osd-l4-[06,11],osd-l6-30,osd-l7-[06,11,16,21,26,31]"

SIZE=$[1024*1024]
SMALLSIZE=$[10*1024]
POOLS="cinder"
BSIZES="4k 4m"
IODEPTHS="64"
TESTS="randwrite randread"

NPOOLS=$(echo $POOLS | wc -w)
NBSIZES=$(echo $BSIZES | wc -w)
NIODEPTHS=$(echo $IODEPTHS | wc -w)
NTESTS=$(echo $TESTS | wc -w)
N_TOTAL=$[NPOOLS*NBSIZES*NIODEPTHS*NTESTS*RUNS]
TIME_TOTAL=$[RUNTIME*N_TOTAL]

# ceph osd crush rule create-simple osd-l osd-l host firstn
#
# ceph osd pool create cinder 16384 16384  replicated row-k
#
# ceph osd crush rule create-simple vhp vhp host firstn
#
# ceph osd pool create vhp 1024 1024  replicated vhp-k
#
# ceph osd pool set vhp size 2
#
# ceph auth caps client.cinder mon "allow r" osd "allow class-read object_prefix rbd_children, allow rwx pool=cinder, allow rwx pool=vhp, allow rwx pool=cinder-l, allow rwx pool=vhp-l"


FIO="/root/fio-rbd"
MAINDIR=$PWD

COLLECTLOPTS="-i 1:10:30 --runtime $[RUNTIME+15]s --plot --subsys cCdDJnNmMZ --sep ,"

CURTEST=0
for RUN in $(seq 1 $RUNS)
do
    TESTDIR=${BASETESTDIR}.$RUN
    cd $MAINDIR
    [ -d $TESTDIR ] || mkdir -p $TESTDIR
    cd $TESTDIR
    $PDSH "[ -d /root/$TESTDIR ] || mkdir -p /root/$TESTDIR"
    $OSDPDSH "[ -d /root/$TESTDIR ] || mkdir -p /root/$TESTDIR"

    for TEST in $TESTS
    do
        for BS in $BSIZES
        do
            for IODEPTH in $IODEPTHS
            do
                for POOL in $POOLS
                do
                    CACHE=$(grep '^rbd_cache ' /etc/ceph/ceph.conf | sed 's/.*= //')
                    [ $CACHE = "true" ] && cachestring='cache' || cachestring='nocache'
                    BASEOUT="fio-test.p:${POOL}.bs:${BS}.iodepth:${IODEPTH}.${TEST}.${cachestring}"
                    OUT="/root/$TESTDIR/$BASEOUT"

                    if [ -f $BASEOUT ]; then
                        echo "Skipping test as file $(pwd)/$BASEOUT is present"
                        continue
                    fi

                    # Create cinder volume, if needed
                    CMDOPTS="-p $POOL -n client.cinder"
                    if [ "$POOL" = "vhp" ]; then
                        $PDSH -f 5 "rbd $CMDOPTS info test-fio-\$(hostname -s) >& /dev/null || rbd $CMDOPTS create  --image-format 2 --size $SMALLSIZE test-fio-\$(hostname -s)"
                    else
                        $PDSH -f 5 "rbd $CMDOPTS info test-fio-\$(hostname -s) >& /dev/null || rbd $CMDOPTS create  --image-format 2 --size $SIZE test-fio-\$(hostname -s)"
                    fi
                    # Drop cache
                    $PDSH 'echo 3 > /proc/sys/vm/drop_caches'
                    $OSDPDSH 'echo 3 > /proc/sys/vm/drop_caches'

                    $PDSH "killall collectl"
                    $OSDPDSH "killall collectl"
                    echo "$[N_TOTAL-CURTEST]/$N_TOTAL tests to run, $[TIME_TOTAL-$[CURTEST*RUNTIME]] seconds"
                    $PDSH -f 100 "echo -e \"[global]\nioengine=rbd\nclientname=cinder\npool=$POOL\nrbdname=test-fio-\$(hostname -s)\nrw=$TEST\nbs=$BS\ntime_based\nruntime=$RUNTIME\n\n[rbd_iodepth]\niodepth=$IODEPTH\n\" > ${OUT}.fio"

                    echo "Running $TEST test on pool $POOL with bs=$BS, iodepth=$IODEPTH for $RUNTIME seconds ($cachestring)"
                    $PDSH -f 100 "collectl $COLLECTLOPTS -f $OUT.collectl >& /dev/null" & 
                    $OSDPDSH -f 100 "collectl $COLLECTLOPTS -f $OUT.collectl >& /dev/null" &
                    $PDSH -f 100 "cd $TESTDIR; $FIO ${OUT}.fio --minimal |tail -1 > ${OUT}.out "
                    wait
                    touch $BASEOUT
                    CURTEST=$[CURTEST+1]
                done
            done
        done
    done
done
