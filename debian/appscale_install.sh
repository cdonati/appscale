set -u
set -e

cd `dirname $0`/..
if [ -z "$APPSCALE_HOME_RUNTIME" ]; then
    export APPSCALE_HOME_RUNTIME=`pwd`
fi

if [ -z "${2-}" ]; then
    DESTDIR=""
else
    DESTDIR=$2
fi
APPSCALE_HOME=${DESTDIR}${APPSCALE_HOME_RUNTIME}
CURL_OPTS="-s"

. debian/appscale_install_functions.sh

echo "Install AppScale into ${APPSCALE_HOME}"
echo "APPSCALE_HOME in runtime=${APPSCALE_HOME_RUNTIME}"

# Let's make sure we got at least one input.
if [ -z "$1" ]; then
    echo "ERROR: need to have at least one target!"
    exit 1
fi

case "$1" in
    # At this time we cannot simply install pieces of AppScale, and the
    # space saving is minimal. So we install all the components.
    all|core|cassandra)
        # Scratch install of appscale including post script.
        installappscaleprofile
        . /etc/profile.d/appscale.sh
        echo "timing: upgrade pip: $(date)"
        upgradepip
        echo "timing: install gems: $(date)"
        installgems
        echo "timing: postinstallhaproxy: $(date)"
        postinstallhaproxy
        echo "timing: postinstallnginx: $(date)"
        postinstallnginx
        echo "timing: installPIL: $(date)"
        installPIL
        echo "timing: installpythonmemcache: $(date)"
        installpythonmemcache
        echo "timing: installlxml: $(date)"
        installlxml
        echo "timing: installxmpppy: $(date)"
        installxmpppy
        echo "timing: installjavajdk: $(date)"
        installjavajdk
        echo "timing: installphp54: $(date)"
        installphp54
        echo "timing: installappserverjava: $(date)"
        installappserverjava
        echo "timing: installtornado: $(date)"
        installtornado
        echo "timing: installpycrypto: $(date)"
        installpycrypto
        echo "timing: installflexmock: $(date)"
        installflexmock
        echo "timing: installpycapnp: $(date)"
        installpycapnp
        echo "timing: installpyyaml: $(date)"
        installpyyaml
        echo "timing: nstall zookeeper: $(date)"
        installzookeeper
        echo "timing: nstall postinstallzk: $(date)"
        postinstallzookeeper
        echo "timing: install installcassandra: $(date)"
        installcassandra
        echo "timing: install postinstallcass: $(date)"
        postinstallcassandra
        echo "timing: install postinstallrabbitmq: $(date)"
        postinstallrabbitmq
        echo "timing: install installsolr: $(date)"
        installsolr
        echo "timing: install installservice: $(date)"
        installservice
        echo "timing: install postinstallservice: $(date)"
        postinstallservice
        echo "timing: install postinstallmonit: $(date)"
        postinstallmonit
        echo "timing: install postinstallejabberd: $(date)"
        postinstallejabberd
        echo "timing: install sethosts: $(date)"
        sethosts
        echo "timing: install setulimits: $(date)"
        setulimits
        echo "timing: install increaseconnections: $(date)"
        increaseconnections
        echo "timing: installversion: $(date)"
        installVersion
        echo "timing: installrequests: $(date)"
        installrequests
        echo "timing: installpyopenssl: $(date)"
        installpyopenssl
        echo "timing: postinstallrsyslog: $(date)"
        postinstallrsyslog
        echo "timing: installpsutil: $(date)"
        installpsutil
        echo "timing: installapiclient: $(date)"
        installapiclient
        echo "timing: installgosdk: $(date)"
        installgosdk
        echo "timing: installacc: $(date)"
        installacc
        echo "timing: installcommon: $(date)"
        installcommon
        echo "timing: installadminserver: $(date)"
        installadminserver
        echo "timing: installhermes: $(date)"
        installhermes
        echo "timing: installtaskqueue: $(date)"
        installtaskqueue
        echo "timing: installdatastore: $(date)"
        installdatastore
        echo "timing: preplogserver: $(date)"
        preplogserver
        echo "timing: prepdashboard: $(date)"
        prepdashboard
        echo "timing: fetchclientjars: $(date)"
        fetchclientjars
        ;;
esac
