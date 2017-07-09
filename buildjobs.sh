#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR

# Build Movement Jobs
cd movements && \
sbt package && \
mkdir -p $DIR/resources/jars && \
target=$DIR/resources/jars/movements.jar
mv target/scala-2.11/movements_2.11-0.1.0.jar $target
echo "Moved files to $target"