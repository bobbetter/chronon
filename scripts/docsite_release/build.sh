#!/bin/bash

# Builds a tar file with
#   1. Chronon spark jar that can drive all workflows
#   2. Chronon doc site
#   3. Test repo that can be used by init
set -euxo pipefail

BRANCH="$(git rev-parse --abbrev-ref HEAD)"
if [[ "$BRANCH" != "main" ]]; then
  echo "$(tput bold) You are not on main branch!"
  echo "$(tput sgr0) Are you sure you want to release? (y to continue)"
  read response
  if [[ "$response" != "y" ]]; then
    echo "Not releasing then. Wise choice."
    exit 0
  fi
fi

thrift --gen py -out api/python/ai/chronon api/thrift/common.thrift
thrift --gen py -out api/python/ai/chronon api/thrift/api.thrift

DOC_BUILD=docs/build
VIRTUAL_ENV=${DOC_BUILD}/sphinx

# Cleanup old artifacts
rm -rf ${DOC_BUILD}

# Setup Virtualenv for Sphinx with all its dependencies
virtualenv ${VIRTUAL_ENV}
source ${VIRTUAL_ENV}/bin/activate
pip install -r docs/sphinx-requirements.txt

# Install the repo's Chronon python API
rm -rf api/python/dist/
python -m build api/python
pip install api/python/dist/chronon_ai*.tar.gz

# Run the Sphinx build
${VIRTUAL_ENV}/bin/sphinx-build -b html docs/source/ ${DOC_BUILD}/html

# Exit the virtualenv
deactivate

sbt +spark/assembly
SBT_JAR_12=$(ls -rt spark/target/scala-2.12/ | grep ".*assembly.*\.jar$" |tail -n 1 | awk '{print $(NF)}')

rm -rf releases
mkdir releases
mkdir -p releases/jar_scala_12
mv ${DOC_BUILD}/html/* releases/
tar -zcf releases/repo.tar.gz -C api/python/test/sample .
mv "spark/target/scala-2.12/${SBT_JAR_12}" releases/jar_scala_12/
cp init.sh releases/init.sh
cp docker-compose.yml releases/docker-compose.yml

echo "Wrote release artifacts into ./releases"
tree -L 1 releases

