package com.github.zeldigas.awsasync.dynamodb

import com.amazonaws.*
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.client.AwsSyncClientParams
import com.amazonaws.handlers.HandlerChainFactory
import com.amazonaws.http.ExecutionContext
import com.amazonaws.http.HttpResponseHandler
import com.amazonaws.metrics.RequestMetricCollector
import com.amazonaws.protocol.json.JsonClientMetadata
import com.amazonaws.protocol.json.JsonErrorResponseMetadata
import com.amazonaws.protocol.json.JsonErrorShapeMetadata
import com.amazonaws.protocol.json.JsonOperationMetadata
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB.ENDPOINT_PREFIX
import com.amazonaws.services.dynamodbv2.model.*
import com.amazonaws.services.dynamodbv2.model.transform.*
import com.amazonaws.transform.JsonUnmarshallerContext
import com.amazonaws.transform.Marshaller
import com.amazonaws.transform.Unmarshaller
import com.amazonaws.util.AWSRequestMetrics
import com.amazonaws.util.CredentialUtils
import com.github.zeldigas.awsasync.core.AmazonWebServiceClientAsync
import com.github.zeldigas.awsasync.core.AwsSyncClientParamsImpl
import java.util.concurrent.CompletableFuture

class AmazonDynamoDBAsyncImpl(
        params: AwsSyncClientParams
) : AmazonDynamoDBAsync, AmazonWebServiceClientAsync(params) {

    companion object {
        private const val DEFAULT_SIGNING_NAME = "dynamodb"
    }

    val awsCredentialsProvider: AWSCredentialsProvider = params.credentialsProvider

    private val protocolFactory = com.amazonaws.protocol.json.SdkJsonProtocolFactory(
            JsonClientMetadata()
                    .withProtocolVersion("1.0")
                    .withSupportsCbor(false)
                    .withSupportsIon(false)
                    .addErrorMetadata(
                            JsonErrorShapeMetadata().withErrorCode("ItemCollectionSizeLimitExceededException").withModeledClass(
                                    ItemCollectionSizeLimitExceededException::class.java))
                    .addErrorMetadata(
                            JsonErrorShapeMetadata().withErrorCode("ResourceInUseException").withModeledClass(
                                    ResourceInUseException::class.java))
                    .addErrorMetadata(
                            JsonErrorShapeMetadata().withErrorCode("ResourceNotFoundException").withModeledClass(
                                    ResourceNotFoundException::class.java))
                    .addErrorMetadata(
                            JsonErrorShapeMetadata().withErrorCode("ProvisionedThroughputExceededException").withModeledClass(
                                    ProvisionedThroughputExceededException::class.java))
                    .addErrorMetadata(
                            JsonErrorShapeMetadata().withErrorCode("ConditionalCheckFailedException").withModeledClass(
                                    ConditionalCheckFailedException::class.java))
                    .addErrorMetadata(
                            JsonErrorShapeMetadata().withErrorCode("InternalServerError").withModeledClass(
                                    InternalServerErrorException::class.java))
                    .addErrorMetadata(
                            JsonErrorShapeMetadata().withErrorCode("LimitExceededException").withModeledClass(
                                    LimitExceededException::class.java))
                    .withBaseServiceExceptionClass(AmazonDynamoDBException::class.java))
/*, */
    constructor(awsCredentialsProvider: AWSCredentialsProvider = DefaultAWSCredentialsProviderChain.getInstance(),
                clientConfiguration: ClientConfiguration,
                requestMetricCollector: RequestMetricCollector? = null) :
            this(AwsSyncClientParamsImpl(clientConfiguration, _metricCollector = requestMetricCollector, _credentialsProvider = awsCredentialsProvider))

    init {
        serviceNameIntern = DEFAULT_SIGNING_NAME;
        endpointPrefix = ENDPOINT_PREFIX;
        // calling this.setEndPoint(...) will also modify the signer accordingly
        setEndpoint("https://dynamodb.us-east-1.amazonaws.com");
        val chainFactory: HandlerChainFactory = HandlerChainFactory();
        requestHandler2s.addAll(chainFactory.newRequestHandlerChain("/com/amazonaws/services/dynamodbv2/request.handlers"));
        requestHandler2s.addAll(chainFactory.newRequestHandler2Chain("/com/amazonaws/services/dynamodbv2/request.handler2s"));
        requestHandler2s.addAll(chainFactory.globalHandlers);
    }

    override fun batchGetItem(batchGetItemRequest: BatchGetItemRequest): CompletableFuture<BatchGetItemResult> {
        return execute(batchGetItemRequest, BatchGetItemRequestProtocolMarshaller(protocolFactory), BatchGetItemResultJsonUnmarshaller())
    }

    override fun getItem(getItemRequest: GetItemRequest): CompletableFuture<GetItemResult> {
        return execute(getItemRequest, GetItemRequestProtocolMarshaller(protocolFactory), GetItemResultJsonUnmarshaller())
    }

    override fun batchWriteItem(batchWriteItemRequest: BatchWriteItemRequest): CompletableFuture<BatchWriteItemResult> {
        return execute(batchWriteItemRequest, BatchWriteItemRequestProtocolMarshaller(protocolFactory), BatchWriteItemResultJsonUnmarshaller())
    }

    override fun deleteItem(deleteItemRequest: DeleteItemRequest): CompletableFuture<DeleteItemResult> {
        return execute(deleteItemRequest, DeleteItemRequestProtocolMarshaller(protocolFactory), DeleteItemResultJsonUnmarshaller())
    }

    override fun scan(scanRequest: ScanRequest): CompletableFuture<ScanResult> {
        return execute(scanRequest, ScanRequestProtocolMarshaller(protocolFactory), ScanResultJsonUnmarshaller())
    }

    override fun updateItem(updateItemRequest: UpdateItemRequest): CompletableFuture<UpdateItemResult> {
        return execute(updateItemRequest, UpdateItemRequestProtocolMarshaller(protocolFactory), UpdateItemResultJsonUnmarshaller())
    }

    override fun updateTable(updateTableRequest: UpdateTableRequest): CompletableFuture<UpdateTableResult> {
        return execute(updateTableRequest, UpdateTableRequestProtocolMarshaller(protocolFactory), UpdateTableResultJsonUnmarshaller())
    }

    override fun tagResource(tagResourceRequest: TagResourceRequest): CompletableFuture<TagResourceResult> {
        return execute(tagResourceRequest, TagResourceRequestProtocolMarshaller(protocolFactory), TagResourceResultJsonUnmarshaller())
    }

    override fun untagResource(untagResourceRequest: UntagResourceRequest): CompletableFuture<UntagResourceResult> {
        return execute(untagResourceRequest, UntagResourceRequestProtocolMarshaller(protocolFactory), UntagResourceResultJsonUnmarshaller())
    }

    override fun updateTimeToLive(updateTimeToLiveRequest: UpdateTimeToLiveRequest): CompletableFuture<UpdateTimeToLiveResult> {
        return execute(updateTimeToLiveRequest, UpdateTimeToLiveRequestProtocolMarshaller(protocolFactory), UpdateTimeToLiveResultJsonUnmarshaller())
    }

    private fun <T : AmazonWebServiceRequest, RM : ResponseMetadata, R : AmazonWebServiceResult<RM>>
            execute(r: T,
                    marshaller: Marshaller<Request<T>, T>,
                    unmarshaller: Unmarshaller<R, JsonUnmarshallerContext>): CompletableFuture<R> {
        val dynamoRequest = beforeClientExecution(r)
        val executionContext = createExecutionContext(dynamoRequest)
        val awsRequestMetrics = executionContext.awsRequestMetrics
        awsRequestMetrics.startEvent(AWSRequestMetrics.Field.ClientExecuteTime)
        val request: Request<T>
        val response: CompletableFuture<Response<R>>

        awsRequestMetrics.startEvent(AWSRequestMetrics.Field.RequestMarshallTime)
        try {
            request = marshaller.marshall(beforeMarshalling(dynamoRequest))
            // Binds the request metrics to the current request.
            request.awsRequestMetrics = awsRequestMetrics
        } finally {
            awsRequestMetrics.endEvent(AWSRequestMetrics.Field.RequestMarshallTime)
        }

        val responseHandler = protocolFactory.createResponseHandler(JsonOperationMetadata()
                .withPayloadJson(true).withHasStreamingSuccessResponse(false), unmarshaller)
        response = invoke<R, T>(request, responseHandler, executionContext)

        response.thenAccept { r -> endClientExecution(awsRequestMetrics, request, r) }

        return response.thenApply { r -> r.awsResponse }
    }

    /**
     * Normal invoke with authentication. Credentials are required and may be overriden at the request level.
     */
    private fun <X, Y : AmazonWebServiceRequest> invoke(request: Request<Y>, responseHandler: HttpResponseHandler<AmazonWebServiceResponse<X>>,
                                                        executionContext: ExecutionContext): CompletableFuture<Response<X>> {

        executionContext.credentialsProvider = CredentialUtils.getCredentialsProvider(request.originalRequest, awsCredentialsProvider)

        return doInvoke<X, Y>(request, responseHandler, executionContext)
    }

    /**
     * Invoke the request using the http client. Assumes credentials (or lack thereof) have been configured in the
     * ExecutionContext beforehand.
     */
    private fun <X, Y : AmazonWebServiceRequest> doInvoke(request: Request<Y>, responseHandler: HttpResponseHandler<AmazonWebServiceResponse<X>>,
                                                          executionContext: ExecutionContext): CompletableFuture<Response<X>> {
        request.endpoint = endpoint
        request.timeOffset = timeOffset

        val errorResponseHandler = protocolFactory.createErrorResponseHandler(JsonErrorResponseMetadata())

        return asyncClient.execute(request, responseHandler, errorResponseHandler, executionContext)
    }

}