/**
 * Copyright (C) 2016-2019 Expedia Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Previous versions of Truck Park staged files locally first, then initiated a multipart upload with
 * {@link com.amazonaws.services.s3.transfer.TransferManager#upload(String, String, java.io.File)}. The drawbacks of
 * this approach are that local disk is required and uploads only start when the first file has been staged which
 * inhibits performance.
 * <p>
 * It was decided, then, to stream directly to s3. The equivalent method for this is
 * {@link com.amazonaws.services.s3.transfer.TransferManager#upload(String, String, java.io.InputStream, com.amazonaws.services.s3.model.ObjectMetadata)}.
 * When streaming this way the first minor problem is that it requires an {@link java.io.InputStream} which requires
 * extra code to buffer data between the {@link java.io.OutputStream} that we write to and TransferManager. The second
 * larger issue is that when we start writing to an OutputStream we don't know what the length will be but if no content
 * length is specified for the InputStream, then TransferManager will attempt to buffer all the stream contents in
 * memory and upload as a traditional, single part upload which is not ideal.
 * <p>
 * This implementation will buffer bytes to a {@link java.io.ByteArrayOutputStream} and compute a MD5 digest as the
 * bytes are written. When the chosen part size is reached, the bytes are wrapped in a
 * {@link java.io.ByteArrayInputStream} and the part is uploaded, asynchronously, with retries. Close will block until
 * all part uploads have completed.
 */
package com.hotels.road.s3.io;
