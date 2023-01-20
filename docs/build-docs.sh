if [ $# -eq 0 ]; then
    echo
    echo "Build a specified version of the DJ docs"
    echo
    echo "Syntax: ./build-docs.sh <VERSION> <SET_AS_LATEST>"
    echo
    echo "VERSION: The version of the docs site to build, i.e. 0.1.0"
    echo "SET_AS_LATEST: Additionally build the specified version as the default/latest docs site, i.e. true"
    echo
    exit 1
fi

VERSION=$1         # 0.1.0
SET_AS_LATEST=$2   # false
BASE_URL=$3        # http://localhost:55555

if [ "$SET_AS_LATEST" = true ] ; then
    hugo -d public --contentDir content/${VERSION}/ --configDir config --baseUrl $BASE_URL --cleanDestinationDir
fi

# Build the versions site (sits outside of docs sites for each version)
hugo -d public/versions --contentDir content/versions/ --configDir config --baseUrl $BASE_URL --environment versions --cleanDestinationDir

# Build the docs site for specified version
hugo -d public/${VERSION} --contentDir content/${VERSION}/ --configDir config --baseUrl "${BASE_URL}${VERSION}" --cleanDestinationDir