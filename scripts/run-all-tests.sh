#!/bin/bash

echo "Moving Config"
cp src/main/resources/hive-site.xml.unittests src/main/resources/hive-site.xml

echo "Running tests"
sbt/sbt test

echo "Clearing all temp files"
rm -r /tmp/hive-*



