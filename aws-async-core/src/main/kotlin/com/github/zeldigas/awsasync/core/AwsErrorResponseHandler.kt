package com.github.zeldigas.awsasync.core

import com.amazonaws.AmazonServiceException
import com.amazonaws.annotation.SdkInternalApi
import com.amazonaws.http.HttpResponse
import com.amazonaws.http.HttpResponseHandler
import com.amazonaws.util.AWSRequestMetrics

@SdkInternalApi
internal class AwsErrorResponseHandler(private val delegate: HttpResponseHandler<AmazonServiceException>,
                                       private val awsRequestMetrics: AWSRequestMetrics) : HttpResponseHandler<AmazonServiceException> {

    @Throws(Exception::class)
    override fun handle(response: HttpResponse): AmazonServiceException {
        val ase = handleAse(response)
        ase.statusCode = response.statusCode
        ase.serviceName = response.request.serviceName
        awsRequestMetrics.addPropertyWith(AWSRequestMetrics.Field.AWSRequestID, ase.requestId)
                .addPropertyWith(AWSRequestMetrics.Field.AWSErrorCode, ase.errorCode)
                .addPropertyWith(AWSRequestMetrics.Field.StatusCode, ase.statusCode)
        return ase
    }

    @Throws(Exception::class)
    private fun handleAse(response: HttpResponse): AmazonServiceException {
        val statusCode = response.statusCode
        try {
            return delegate.handle(response)
        } catch (e: InterruptedException) {
            throw e
        } catch (e: Exception) {
            // If the errorResponseHandler doesn't work, then check for error responses that don't have any content
            if (statusCode == 413) {
                val exception = AmazonServiceException("Request entity too large")
                exception.serviceName = response.request.serviceName
                exception.statusCode = statusCode
                exception.errorType = AmazonServiceException.ErrorType.Client
                exception.errorCode = "Request entity too large"
                return exception
            } else if (statusCode in 500..599) {
                val exception = AmazonServiceException(response.statusText)
                exception.serviceName = response.request.serviceName
                exception.statusCode = statusCode
                exception.errorType = AmazonServiceException.ErrorType.Service
                exception.errorCode = response.statusText
                return exception
            } else {
                throw e
            }
        }

    }

    override fun needsConnectionLeftOpen(): Boolean {
        return delegate.needsConnectionLeftOpen()
    }
}