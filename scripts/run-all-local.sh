#!/bin/bash

# This file runs the scala code using reasonable defaults
# for production assuming a deployment on ec2 using the deploy scripts in the
# repo.
#
# Usage: ./run-all-local.sh [-s] [MAIN_CLASS]
# Arguments:
#   -s: Communicate with AMPCrowd over ssl
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

# Run the scala code
if [ -z "$main_class" ]
then
    ./sbt/sbt run
else
    ./sbt/sbt "run-main $main_class"
fi
