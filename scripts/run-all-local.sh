#!/bin/bash

# This file runs both the python and the scala code using reasonable defaults
# for production assuming a deployment on ec2 using the deploy scripts in the
# repo.
#
# This script assumes that if you have virtualenvwrapper installed, its
# script is located at /usr/local/bin/virtualenvwrapper.sh. If you aren't using
# virtualenvs, this script will also work.
#
# Usage: ./run-all-local.sh [-s] [MAIN_CLASS]
# Arguments:
#   -s: Run the crowd server with ssl
#   MAIN_CLASS: the scala application class to run. If not specified, sbt will
#     prompt the user to choose.

# Enable SSL
if [ "$1" == "-s" ]
then
    export SSL=1
    main_class="$2"
else
    export SSL=0
    main_class="$1"
fi

# Activate the virtualenv if it exists.
if [ -e "/usr/local/bin/virtualenvwrapper.sh" ]
then
    source /usr/local/bin/virtualenvwrapper.sh
    workon sampleclean
fi

# Start the crowd server
pushd src/main/python/crowd_server
./run.sh $@
popd

# Run the scala code
if [ -z "$main_class" ]
then
    ./sbt/sbt run
else
    ./sbt/sbt "run-main $main_class"
fi
