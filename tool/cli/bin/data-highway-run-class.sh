#!/usr/bin/env bash
# Copyright (C) 2016-2019 Expedia Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# set this flag to true to debug this script
DEBUG=false

EXEC_CLASS="$1"
shift
NUM_ARGUMENTS="$#"
ARGUMENTS="$@"

if [ "$DEBUG" = true ]; then
    set -x

    printf "  main class           = \"${EXEC_CLASS}\" \n"
    printf "  number of arguments  = \"${NUM_ARGUMENTS}\" \n"
    printf "  arguments            = \"${ARGUMENTS}\" \n"
fi

if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

EXEC_JAR=$(dirname $0)/../target/road-tool-cli-*-jar-with-dependencies.jar

exec $JAVA -cp ${EXEC_JAR} ${EXEC_CLASS} "$@"


if [ "$DEBUG" = true ]; then
    set +x
fi
