#!/bin/bash

# This file runs the scala code using reasonable defaults
# for production assuming a deployment on ec2 using the deploy scripts in the
# repo.
# Arguments:
#   -s: Communicate with AMPCrowd over ssl

# Enable SSL
if [ "$1" == "-s" ]
then
    export SSL=1
else
    export SSL=0
fi

# Build the scala code
./sbt/sbt assembly

# Run the scala code
/root/spark/bin/spark-submit --class sampleclean.SCDriver --master `cat /root/spark-ec2/cluster-url` --driver-memory 2G target/scala-2.10/SampleClean-Spark-1.0-assembly-1.0.jar
