package com.github.zeldigas.awsasync.core

import com.amazonaws.*
import com.amazonaws.annotation.SdkInternalApi
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.CanHandleNullCredentials
import com.amazonaws.auth.Signer
import com.amazonaws.event.ProgressEventType
import com.amazonaws.event.ProgressInputStream
import com.amazonaws.event.ProgressListener
import com.amazonaws.event.SDKProgressPublisher.publishProgress
import com.amazonaws.event.SDKProgressPublisher.publishRequestContentLength
import com.amazonaws.handlers.CredentialsRequestHandler
import com.amazonaws.handlers.HandlerContextKey
import com.amazonaws.handlers.RequestHandler2
import com.amazonaws.http.ExecutionContext
import com.amazonaws.http.HttpResponse
import com.amazonaws.http.HttpResponseHandler
import com.amazonaws.http.response.AwsResponseHandlerAdapter
import com.amazonaws.http.settings.HttpClientSettings
import com.amazonaws.internal.*
import com.amazonaws.internal.auth.SignerProviderContext
import com.amazonaws.metrics.RequestMetricCollector
import com.amazonaws.util.*
import com.amazonaws.util.IOUtils.closeQuietly
import org.apache.commons.logging.LogFactory
import org.asynchttpclient.AsyncHttpClient
import org.asynchttpclient.Dsl
import java.io.FileInputStream
import java.io.IOException
import java.io.InputStream
import java.util.*
import java.util.concurrent.CompletableFuture

class AmazonHttpClientAsync private constructor(
        val config: ClientConfiguration,
        val requestMetricCollector: RequestMetricCollector?,
        val httpClientSettings: HttpClientSettings,
        httpClient: AsyncHttpClient? = null
) {

    companion object {
        const val HEADER_USER_AGENT = "User-Agent"
        const val HEADER_SDK_TRANSACTION_ID = "amz-sdk-invocation-id"
        const val HEADER_SDK_RETRY_INFO = "amz-sdk-retry"
        /**
         * Logger for more detailed debugging information, that might not be as useful for end users
         * (ex: HTTP client configuration, etc).
         */
        internal val log = LogFactory.getLog(AmazonHttpClientAsync::class.java)
        /**
         * Logger providing detailed information on requests/responses. Users can enable this logger to
         * get access to AWS request IDs for responses, individual requests and parameters sent to AWS,
         * etc.
         */
        @SdkInternalApi
        val requestLog = LogFactory.getLog("com.amazonaws.request")

        /**
         * When throttled retries are enabled, each retry attempt will consume this much capacity.
         * Successful retry attempts will release this capacity back to the pool while failed retries
         * will not.  Successful initial (non-retry) requests will always release 1 capacity unit to the
         * pool.
         */
        private val THROTTLED_RETRY_COST = 5

        init {
            // Customers have reported XML parsing issues with the following
            // JVM versions, which don't occur with more recent versions, so
            // if we detect any of these, give customers a heads up.
            // https://bugs.openjdk.java.net/browse/JDK-8028111
            val problematicJvmVersions = listOf("1.6.0_06", "1.6.0_13", "1.6.0_17", "1.6.0_65", "1.7.0_45")
            val jvmVersion = System.getProperty("java.version")
            if (problematicJvmVersions.contains(jvmVersion)) {
                log.warn("Detected a possible problem with the current JVM version (" + jvmVersion +
                        ").  " +
                        "If you experience XML parsing problems using the SDK, try upgrading to a more recent JVM update.")
            }
        }
    }

    /**
     * Cache of metadata for recently executed requests for diagnostic purposes
     */
    private val responseMetadataCache: MetadataCache = if (config.cacheResponseMetadata)
        ResponseMetadataCache(config.responseMetadataCacheSize)
    else
        NullResponseMetadataCache()

    /**
     * Used to generate UUID's for client transaction id. This gives a higher probability of id
     * clashes but is more performant then using [UUID.randomUUID] which uses SecureRandom
     * internally.
     */
    private val random = Random()
    private val asyncClient: AsyncHttpClient = httpClient ?: Dsl.asyncHttpClient();

    constructor(config: ClientConfiguration,
                requestMetricCollector: RequestMetricCollector,
                useBrowserCompatibleHostNameVerifier: Boolean,
                calculateCRC32FromCompressedData: Boolean) : this(
            config,
            requestMetricCollector,
            HttpClientSettings.adapt(config, useBrowserCompatibleHostNameVerifier, calculateCRC32FromCompressedData)
    )

    /**
     * Executes the request and returns the result.

     * @param request              The AmazonWebServices request to send to the remote server
     * *
     * @param responseHandler      A response handler to accept a successful response from the
     * *                             remote server
     * *
     * @param errorResponseHandler A response handler to accept an unsuccessful response from the
     * *                             remote server
     * *
     * @param executionContext     Additional information about the context of this web service
     * *                             call
     * *
     */
    @Deprecated("Use {@link #requestExecutionBuilder()} to configure and execute a HTTP request.")
    fun <T> execute(request: Request<*>,
                    responseHandler: HttpResponseHandler<AmazonWebServiceResponse<T>>,
                    errorResponseHandler: HttpResponseHandler<AmazonServiceException>,
                    executionContext: ExecutionContext): CompletableFuture<Response<T>> {
        val adaptedRespHandler = AwsResponseHandlerAdapter<T>(
                getNonNullResponseHandler<AmazonWebServiceResponse<T>>(responseHandler),
                request,
                executionContext.awsRequestMetrics,
                responseMetadataCache)
        return requestExecutionBuilder()
                .request(request)
                .requestConfig(AmazonWebServiceRequestAdapter(request.originalRequest))
                .errorResponseHandler(AwsErrorResponseHandler(errorResponseHandler, executionContext.awsRequestMetrics))
                .executionContext(executionContext)
                .execute(adaptedRespHandler)
    }

    /**
     * Ensures the response handler is not null. If it is this method returns a dummy response
     * handler.

     * @return Either original response handler or dummy response handler.
     */
    private fun <T> getNonNullResponseHandler(
            responseHandler: HttpResponseHandler<T>?): HttpResponseHandler<T> {
        if (responseHandler != null) {
            return responseHandler
        } else {
            // Return a Dummy, No-Op handler
            return object : HttpResponseHandler<T> {

                @Throws(Exception::class)
                override fun handle(response: HttpResponse): T? {
                    return null
                }

                override fun needsConnectionLeftOpen(): Boolean {
                    return false
                }
            }
        }
    }

    /**
     * @return A builder used to configure and execute a HTTP request.
     */
    fun requestExecutionBuilder(): RequestExecutionBuilder {
        return RequestExecutionBuilderImpl()
    }

    /**
     * Interface to configure a request execution and execute the request.
     */
    interface RequestExecutionBuilder {

        /**
         * Fluent setter for [Request]

         * @param request Request object
         * *
         * @return This builder for method chaining.
         */
        fun request(request: Request<*>): RequestExecutionBuilder

        /**
         * Fluent setter for the error response handler

         * @param errorResponseHandler Error response handler
         * *
         * @return This builder for method chaining.
         */
        fun errorResponseHandler(
                errorResponseHandler: HttpResponseHandler<out SdkBaseException>): RequestExecutionBuilder

        /**
         * Fluent setter for the execution context

         * @param executionContext Execution context
         * *
         * @return This builder for method chaining.
         */
        fun executionContext(executionContext: ExecutionContext?): RequestExecutionBuilder

        /**
         * Fluent setter for [RequestConfig]

         * @param requestConfig Request config object
         * *
         * @return This builder for method chaining.
         */
        fun requestConfig(requestConfig: RequestConfig?): RequestExecutionBuilder

        /**
         * Executes the request with the given configuration.

         * @param responseHandler Response handler that outputs the actual result type which is
         * *                        preferred going forward.
         * *
         * @param <Output>        Result type
         * *
         * @return Unmarshalled result type.
        </Output> */
        fun <Output> execute(responseHandler: HttpResponseHandler<Output>?): CompletableFuture<Response<Output>>

        /**
         * Executes the request with the given configuration; not handling response.

         * @return Void response
         */
        fun execute(): CompletableFuture<Response<Unit>>

    }

    inner class RequestExecutionBuilderImpl : RequestExecutionBuilder {
        private lateinit var request: Request<*>
        private var requestConfig: RequestConfig? = null
        private var errorResponseHandler: HttpResponseHandler<out SdkBaseException>? = null
        private var executionContext: ExecutionContext? = ExecutionContext()

        override fun request(request: Request<*>): RequestExecutionBuilder {
            this.request = request
            return this
        }

        override fun errorResponseHandler(
                errorResponseHandler: HttpResponseHandler<out SdkBaseException>): RequestExecutionBuilder {
            this.errorResponseHandler = errorResponseHandler
            return this
        }

        override fun executionContext(
                executionContext: ExecutionContext?): RequestExecutionBuilder {
            this.executionContext = executionContext
            return this
        }

        override fun requestConfig(requestConfig: RequestConfig?): RequestExecutionBuilder {
            this.requestConfig = requestConfig
            return this
        }

        override fun <Output> execute(responseHandler: HttpResponseHandler<Output>?): CompletableFuture<Response<Output>> {
            val config = requestConfig ?: AmazonWebServiceRequestAdapter(request.originalRequest)

            if (executionContext == null) {
                throw SdkClientException(
                        "Internal SDK Error: No execution context parameter specified.")
            }

            return RequestExecutor(request,
                    config,
                    getNonNullResponseHandler(errorResponseHandler),
                    getNonNullResponseHandler(responseHandler),
                    executionContext!!,
                    getRequestHandlers()
            ).execute()
        }

        override fun execute(): CompletableFuture<Response<Unit>> {
            return execute<Unit>(null)
        }

        private fun getRequestHandlers(): List<RequestHandler2> {
            val requestHandler2s = executionContext?.requestHandler2s ?: return emptyList()
            return requestHandler2s
        }
    }

    private inner class RequestExecutor<Output>(
            val request: Request<*>,
            val requestConfig: RequestConfig,
            val errorResponseHandler: HttpResponseHandler<out SdkBaseException>,
            val responseHandler: HttpResponseHandler<Output>,
            val executionContext: ExecutionContext,
            val requestHandler2s: List<RequestHandler2>
    ) {

        val awsRequestMetrics: AWSRequestMetrics = executionContext.awsRequestMetrics

        /**
         * Executes the request and returns the result.
         */
        internal fun execute(): CompletableFuture<Response<Output>> {
            runBeforeRequestHandlers()
            setSdkTransactionId(request)
            setUserAgent(request)

            val listener = requestConfig.progressListener
            // add custom headers
            request.headers.putAll(config.headers)
            request.headers.putAll(requestConfig.customRequestHeaders)
            // add custom query parameters
            mergeQueryParameters(requestConfig.customQueryParameters)
            var response: Response<Output>? = null

            val toBeClosed: InputStream? = beforeRequest()
            publishProgress(listener, ProgressEventType.CLIENT_REQUEST_STARTED_EVENT)
            return executeHelper(request, toBeClosed?.let { ReleasableInputStream.wrap(it).disableClose<ReleasableInputStream>() })
                    .thenApply { response ->
                        publishProgress(listener, ProgressEventType.CLIENT_REQUEST_SUCCESS_EVENT)
                        awsRequestMetrics.timingInfo.endTiming()
                        afterResponse<Output>(response)
                        response
                    }.exceptionally { e ->
                val cause = e.cause
                if (cause is AmazonClientException) {
                    publishProgress(listener, ProgressEventType.CLIENT_REQUEST_FAILED_EVENT)
                    afterError(null, cause)
                }
                throw e
            }.whenComplete { _, _ -> closeQuietly(toBeClosed, log) }
        }

        private fun runBeforeRequestHandlers() {
            val credentials = getCredentialsFromContext()
            request.addHandlerContext(HandlerContextKey.AWS_CREDENTIALS, credentials)
            // Apply any additional service specific request handlers that need to be run
            for (requestHandler2 in requestHandler2s) {
                // If the request handler is a type of CredentialsRequestHandler, then set the credentials in the request handler.
                if (requestHandler2 is CredentialsRequestHandler) {
                    requestHandler2.setCredentials(credentials)
                }
                requestHandler2.beforeRequest(request)
            }
        }

        /**
         * Returns the credentials from the execution if exists. Else returns null.
         */
        private fun getCredentialsFromContext(): AWSCredentials? {
            val credentialsProvider = executionContext.credentialsProvider

            var credentials: AWSCredentials? = null
            if (credentialsProvider != null) {
                awsRequestMetrics.startEvent(AWSRequestMetrics.Field.CredentialsRequestTime)
                try {
                    credentials = credentialsProvider.credentials
                } finally {
                    awsRequestMetrics.endEvent(AWSRequestMetrics.Field.CredentialsRequestTime)
                }
            }
            return credentials
        }

        /**
         * Create a client side identifier that will be sent with the initial request and each
         * retry.
         */
        private fun setSdkTransactionId(request: Request<*>) {
            request.addHeader(HEADER_SDK_TRANSACTION_ID,
                    UUID(random.nextLong(), random.nextLong()).toString())
        }

        /**
         * Sets a User-Agent for the specified request, taking into account any custom data.
         */
        private fun setUserAgent(request: Request<*>) {
            val opts = requestConfig.requestClientOptions
            if (opts != null) {
                request.addHeader(HEADER_USER_AGENT, RuntimeHttpUtils
                        .getUserAgent(config, opts.getClientMarker(RequestClientOptions.Marker.USER_AGENT)))
            } else {
                request.addHeader(HEADER_USER_AGENT, RuntimeHttpUtils.getUserAgent(config, null))
            }
        }

        /**
         * Merge query parameters into the given request.
         */
        private fun mergeQueryParameters(params: Map<String, List<String>>) {
            val existingParams = request.parameters
            for ((pName, pValues) in params) {
                existingParams.put(pName, CollectionUtils.mergeLists(existingParams[pName], pValues))
            }
        }

        /**
         * Publishes the "request content length" event, and returns an input stream, which will be
         * made mark-and-resettable if possible, for progress tracking purposes.

         * @return an input stream, which will be made mark-and-resettable if possible, for progress
         * * tracking purposes; or null if the request doesn't have an input stream
         */
        private fun beforeRequest(): InputStream? {
            val listener = requestConfig.progressListener
            reportContentLength(listener)
            if (request.content == null) {
                return null
            }
            return ProgressInputStream.inputStreamForRequest(request.content.makeResettable().buffer(), listener)
        }

        private fun InputStream.makeResettable(): InputStream {
            if (!markSupported()) {
                // try to wrap the content input stream to become
                // mark-and-resettable for signing and retry purposes.
                if (this is FileInputStream) {
                    try {
                        // ResettableInputStream supports mark-and-reset without
                        // memory buffering
                        return ResettableInputStream(this)
                    } catch (e: IOException) {
                        if (log.isDebugEnabled) {
                            log.debug("For the record; ignore otherwise", e)
                        }
                    }

                }
            }
            return this
        }

        /**
         * Buffer input stream if possible.

         * @param content Input stream to buffer
         * *
         * @return SdkBufferedInputStream if possible, otherwise original input stream.
         */
        private fun InputStream.buffer(): InputStream = when (markSupported()) {
            true -> this
            false -> SdkBufferedInputStream(this)
        }

        /**
         * If content length is present on the request, report it to the progress listener.

         * @param listener Listener to notify.
         */
        private fun reportContentLength(listener: ProgressListener) {
            val headers = request.headers
            val contentLengthStr = headers["Content-Length"]
            if (contentLengthStr != null) {
                try {
                    val contentLength = contentLengthStr.toLong()
                    publishRequestContentLength(listener, contentLength)
                } catch (e: NumberFormatException) {
                    log.warn("Cannot parse the Content-Length header of the request.")
                }

            }
        }

        fun executeHelper(request: Request<*>, content: InputStream?): CompletableFuture<Response<Output>> {
            val credentials = getCredentialsFromContext()
            signRequest(credentials, request, executionContext)
            return asyncClient.prepareRequest(create(request, httpClientSettings, content))
                    .execute().toCompletableFuture()
                    .thenApply { it.toAmazonResponse(request) }
                    .thenApply { Response(handleResponse(it), it) }
        }

        private fun signRequest(credentials: AWSCredentials?, request: Request<*>, executionContext: ExecutionContext) {
            val signer = newSigner(request, executionContext)
            if (signer != null && (credentials != null || signer is CanHandleNullCredentials)) {
                awsRequestMetrics.startEvent(AWSRequestMetrics.Field.RequestSigningTime)
                try {
                    signer.sign(request, credentials)
                } finally {
                    awsRequestMetrics.endEvent(AWSRequestMetrics.Field.RequestSigningTime)
                }
            }
        }

        private fun <T> afterResponse(response: Response<T>) {
            for (handler2 in requestHandler2s) {
                handler2.afterResponse(request, response)
            }
        }

        @Throws(InterruptedException::class)
        private fun afterError(response: Response<*>?,
                               e: AmazonClientException) {
            for (handler2 in requestHandler2s) {
                handler2.afterError(request, response, e)
            }
        }

        internal fun newSigner(request: Request<*>, execContext: ExecutionContext): Signer? {
            val signerProviderContext = SignerProviderContext
                    .builder()
                    .withRequest(request)
                    .withRequestConfig(requestConfig)
            val signerURI = request.endpoint
            return execContext.getSigner(signerProviderContext.withUri(signerURI).build())
        }

        /**
         * Handles a successful response from a service call by unmarshalling the results using the
         * specified response handler.

         * @return The contents of the response, unmarshalled using the specified response handler.
         * *
         * @throws IOException If any problems were encountered reading the response contents from
         * *                     the HTTP method object.
         */
        @Throws(IOException::class, InterruptedException::class)
        private fun handleResponse(httpResponse: HttpResponse): Output {
            val listener = requestConfig.progressListener
            try {
                val awsResponse: Output
                awsRequestMetrics.startEvent(AWSRequestMetrics.Field.ResponseProcessingTime)
                publishProgress(listener, ProgressEventType.HTTP_RESPONSE_STARTED_EVENT)
                try {
                    awsResponse = responseHandler.handle(beforeUnmarshalling(httpResponse))
                } finally {
                    awsRequestMetrics.endEvent(AWSRequestMetrics.Field.ResponseProcessingTime)
                }
                publishProgress(listener, ProgressEventType.HTTP_RESPONSE_COMPLETED_EVENT)

                return awsResponse
            } catch (e: CRC32MismatchException) {
                throw e
            } catch (e: IOException) {
                throw e
            } catch (e: AmazonClientException) {
                throw e // simply rethrow rather than further wrapping it
            } catch (e: InterruptedException) {
                throw e
            } catch (e: Exception) {
                val errorMessage = "Unable to unmarshall response (" + e.message + "). Response Code: "
                (+httpResponse.statusCode).toString() + ", Response Text: " +
                        httpResponse.statusText
                throw SdkClientException(errorMessage, e)
            }

        }

        /**
         * Run [RequestHandler2.beforeUnmarshalling] callback

         * @param origHttpResponse Original [HttpResponse]
         * *
         * @return [HttpResponse] object to pass to unmarshaller. May have been modified or
         * * replaced by the request handlers
         */
        private fun beforeUnmarshalling(origHttpResponse: HttpResponse): HttpResponse {
            var toReturn = origHttpResponse
            for (requestHandler in requestHandler2s) {
                toReturn = requestHandler.beforeUnmarshalling(request, toReturn)
            }
            return toReturn
        }


    }

    private fun org.asynchttpclient.Response.toAmazonResponse(request: Request<*>): HttpResponse {
        val httpResponse = AsyncHttpResponse(request)
        if (hasResponseBody()) {
            httpResponse.content = CRC32ChecksumCalculatingInputStream(responseBodyAsStream)
        }

        httpResponse.statusCode = statusCode
        httpResponse.statusText = statusText
        headers.forEach { (k, v) -> httpResponse.addHeader(k, v) }
        return httpResponse
    }

    fun shutdown() {
        asyncClient.close()
    }

}

internal class AsyncHttpResponse(request: Request<*>) : HttpResponse(request, null) {
    override fun getCRC32Checksum(): Long {
        return (content as? CRC32ChecksumCalculatingInputStream)?.crC32Checksum ?: 0L
    }
}
