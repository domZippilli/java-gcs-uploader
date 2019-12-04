#!/usr/bin/env bash

# Copyright 2019 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

# Display script usage
function usage(){
    echo >&2
    echo "Usage: $0 BUCKET_NAME" >&2
    echo >&2
    echo "Uploads ../sampleblob to BUCKET_NAME with a series of different chunk sizes." >&2
    echo >&2
    echo "Arguments:" >&2
    echo "  BUCKET_NAME     The bucket to use for the tests." >&2
    echo >&2
}


BUCKET=${1?$(usage)}
OBJECT="../sampleblob"
UPLOADER="../java-gcs-uploader/target/java-gcs-uploader-1.0-SNAPSHOT-jar-with-dependencies.jar"

for i in 1 2 5 10 15 20 25 30 35 40 50 60 70 80 90 100; do
    for _ in {1..3}; do
        $UPLOADER $BUCKET $OBJECT $i
        sleep 2
    done
done