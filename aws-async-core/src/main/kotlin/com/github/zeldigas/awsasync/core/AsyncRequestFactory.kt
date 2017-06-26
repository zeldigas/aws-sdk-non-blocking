package com.github.zeldigas.awsasync.core

import com.amazonaws.Request
import com.amazonaws.SdkClientException
import com.amazonaws.http.HttpMethodName
import com.amazonaws.http.settings.HttpClientSettings
import com.amazonaws.util.FakeIOException
import com.amazonaws.util.SdkHttpUtils
import org.apache.http.HttpHeaders
import org.asynchttpclient.RequestBuilder
import java.io.InputStream
import java.net.URI
import java.util.*

private val ignoreHeaders = Arrays.asList(HttpHeaders.CONTENT_LENGTH, HttpHeaders.HOST)

private const val DEFAULT_ENCODING = "UTF-8"

@Throws(FakeIOException::class)
fun create(request: Request<*>, settings: HttpClientSettings, content: InputStream?): RequestBuilder {

    var builder = RequestBuilder()

    builder.apply {
        setUrl(SdkHttpUtils.appendUri(request.endpoint.toString(), request.resourcePath, true))
        setMethod(request.httpMethod.name)
        val requestIsPost = request.httpMethod == HttpMethodName.POST
        val requestHasNoPayload = content != null
        val putParamsInUri = !requestIsPost || requestHasNoPayload
        if (putParamsInUri) {
            setQueryParams(request.parameters)
        } else {
            setFormParams(request.parameters)
        }

        when (request.httpMethod) {
            HttpMethodName.GET, HttpMethodName.HEAD, HttpMethodName.OPTIONS, HttpMethodName.DELETE -> Unit
            HttpMethodName.POST, HttpMethodName.PATCH, HttpMethodName.PUT -> addBody(content)
            else -> throw SdkClientException("Unknown HTTP method name: " + request.httpMethod)
        }
    }

    addHeadersToRequest(builder, request)
    addRequestConfig(builder, request, settings)

    return builder
}

private fun addRequestConfig(base: RequestBuilder,
                             request: Request<*>,
                             settings: HttpClientSettings) {
    //only async client 2.1.x
    //base.setReadTimeout(settings.socketTimeout)
    base.setRequestTimeout(settings.connectionTimeout)
            .setLocalAddress(settings.localAddress)


    /*
         * Enable 100-continue support for PUT operations, since this is
         * where we're potentially uploading large amounts of data and want
         * to find out as early as possible if an operation will fail. We
         * don't want to do this for all operations since it will cause
         * extra latency in the network interaction.
         */
    if (HttpMethodName.PUT == request.httpMethod && settings.isUseExpectContinue) {
        request.addHeader("Expect", "100-continue")
    }
}

private fun RequestBuilder.addBody(content: InputStream?) {
    if (content != null) {
        setBody(content)
    }
}


/**
 * Configures the headers in the specified Apache HTTP request.
 */
private fun addHeadersToRequest(httpRequest: RequestBuilder, request: Request<*>) {

    httpRequest.addHeader(HttpHeaders.HOST, getHostHeaderValue(request.endpoint))

    // Copy over any other headers already in our request
    for ((key, value) in request.headers) {
        /*
             * HttpClient4 fills in the Content-Length header and complains if
             * it's already present, so we skip it here. We also skip the Host
             * header to avoid sending it twice, which will interfere with some
             * signing schemes.
             */
        if (!ignoreHeaders.contains(key)) {
            httpRequest.addHeader(key, value)
        }
    }

    /* Set content type and encoding */
    val built = httpRequest.build()
    if (HttpHeaders.CONTENT_TYPE !in built.headers) {
        httpRequest.addHeader(HttpHeaders.CONTENT_TYPE,
                "application/x-www-form-urlencoded; " +
                        "charset=" + DEFAULT_ENCODING.toLowerCase())
    }
}

private fun getHostHeaderValue(endpoint: URI): String {
    /*
         * Apache HttpClient omits the port number in the Host header (even if
         * we explicitly specify it) if it's the default port for the protocol
         * in use. To ensure that we use the same Host header in the request and
         * in the calculated string to sign (even if Apache HttpClient changed
         * and started honoring our explicit host with endpoint), we follow this
         * same behavior here and in the QueryString signer.
         */
    return if (SdkHttpUtils.isUsingNonDefaultPort(endpoint))
        endpoint.host + ":" + endpoint.port
    else
        endpoint.host
}