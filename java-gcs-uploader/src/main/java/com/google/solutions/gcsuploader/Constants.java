/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.solutions.gcsuploader;

public class Constants {
    public static final int CHUNK_SIZE = 15 * 1000 * 1000;

    public static final long SLICED_THRESHOLD = CHUNK_SIZE * 4;
    public static final int MAX_SLICES = 8; // No more than 32 due to simple implementation.

    public static final int SIMULTANEOUS_FILES = Runtime.getRuntime().availableProcessors();
    public static final int UPLOAD_THREADS = Runtime.getRuntime().availableProcessors() * 4;
}