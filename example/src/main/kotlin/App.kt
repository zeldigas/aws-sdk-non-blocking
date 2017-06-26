package com.github.zeldigas.awsasync.example

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.metrics.RequestMetricCollector
import com.amazonaws.regions.Region
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.GetItemResult
import com.github.zeldigas.awsasync.dynamodb.AmazonDynamoDBAsyncImpl
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture

class App

fun main(args: Array<String>) {
    val table = args[0]
    val region = args[1]
    val hashKey = args[2]
    val rangeKey = args[3]

    val log = LoggerFactory.getLogger(App::class.java)
    val provider = DefaultAWSCredentialsProviderChain()
    val testClient = AmazonDynamoDBAsyncImpl(provider, ClientConfiguration(), RequestMetricCollector.NONE)
    testClient.setRegion(Region.getRegion(Regions.fromName(region)))
    val results = mutableListOf<CompletableFuture<GetItemResult>>()

    log.info("Sending requests to dynamo")
    for (i in 1..50) {
        results += testClient.getItem(table, mapOf(
                "Artist" to AttributeValue(hashKey),
                "Album" to AttributeValue(rangeKey)
        )).whenComplete { item, ex -> log.info("Item ${i}: {}", item) }
    }
    log.info("Waiting for all requests to complete")
    CompletableFuture.allOf(*results.toTypedArray()).join()
    log.info("Shutting down client")
    testClient.shutdown()
}

