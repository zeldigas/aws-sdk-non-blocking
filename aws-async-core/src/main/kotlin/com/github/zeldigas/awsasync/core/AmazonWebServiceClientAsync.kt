package com.github.zeldigas.awsasync.core

import com.amazonaws.AmazonWebServiceClient
import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.client.AwsSyncClientParams
import com.amazonaws.handlers.RequestHandler2
import com.amazonaws.metrics.RequestMetricCollector
import java.util.concurrent.CopyOnWriteArrayList

open class AmazonWebServiceClientAsync(clientParams: AwsSyncClientParams, disableStrictHostNameVerification: Boolean = false)
    : AmazonWebServiceClient(clientParams) {

    protected val asyncClient: AmazonHttpClientAsync = AmazonHttpClientAsync(clientParams.clientConfiguration,
            clientParams.requestMetricCollector, disableStrictHostNameVerification,
            calculateCRC32FromCompressedData())

    override fun getRequestMetricsCollector(): RequestMetricCollector? = asyncClient.requestMetricCollector

    override fun shutdown() {
        asyncClient.shutdown();
        super.shutdown()
    }
}

class AwsSyncClientParamsImpl(
        private val _clientConfiguration: ClientConfiguration,
        private val _requestHandlers: MutableList<RequestHandler2> = CopyOnWriteArrayList<RequestHandler2>(),
        private val _metricCollector: RequestMetricCollector?,
        private val _credentialsProvider: AWSCredentialsProvider? = null
) : AwsSyncClientParams() {
    override fun getRequestMetricCollector(): RequestMetricCollector? = _metricCollector

    override fun getClientConfiguration(): ClientConfiguration = _clientConfiguration

    override fun getCredentialsProvider(): AWSCredentialsProvider? = _credentialsProvider

    override fun getRequestHandlers(): MutableList<RequestHandler2> = _requestHandlers
}
