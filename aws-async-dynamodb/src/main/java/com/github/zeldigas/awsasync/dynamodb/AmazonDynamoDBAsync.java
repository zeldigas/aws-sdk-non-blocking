package com.github.zeldigas.awsasync.dynamodb;

import com.amazonaws.services.dynamodbv2.model.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface AmazonDynamoDBAsync {
    @Nonnull
    CompletableFuture<GetItemResult> getItem(@Nonnull GetItemRequest getItemRequest);

    @Nonnull
    default CompletableFuture<GetItemResult> getItem(@Nonnull String tableName, @Nonnull Map<String, AttributeValue> key) {
        return getItem(new GetItemRequest().withTableName(tableName).withKey(key));
    }

    @Nonnull
    default CompletableFuture<GetItemResult> getItem(@Nonnull String tableName, @Nonnull Map<String, AttributeValue> key, @Nullable Boolean consistentRead) {
        return getItem(new GetItemRequest().withTableName(tableName).withKey(key).withConsistentRead(consistentRead));
    }

    @Nonnull
    CompletableFuture<BatchGetItemResult> batchGetItem(@Nonnull BatchGetItemRequest batchGetItemRequest);

    @Nonnull
    default CompletableFuture<BatchGetItemResult> batchGetItem(@Nonnull Map<String, KeysAndAttributes> requestItems, @Nonnull String returnConsumedCapacity) {
        return batchGetItem(new BatchGetItemRequest().withRequestItems(requestItems).withReturnConsumedCapacity(returnConsumedCapacity));
    }

    @Nonnull
    default CompletableFuture<BatchGetItemResult> batchGetItem(@Nonnull Map<String, KeysAndAttributes> requestItems) {
        return batchGetItem(new BatchGetItemRequest().withRequestItems(requestItems));
    }

    @Nonnull
    CompletableFuture<BatchWriteItemResult> batchWriteItem(@Nonnull BatchWriteItemRequest batchWriteItemRequest);

    @Nonnull
    default CompletableFuture<BatchWriteItemResult> batchWriteItem(@Nonnull Map<String, List<WriteRequest>> requestItems) {
        return batchWriteItem(new BatchWriteItemRequest().withRequestItems(requestItems));
    }

    @Nonnull
    CompletableFuture<DeleteItemResult> deleteItem(@Nonnull DeleteItemRequest deleteItemRequest);

    @Nonnull
    default CompletableFuture<DeleteItemResult> deleteItem(@Nonnull String tableName, @Nonnull Map<String, AttributeValue> key) {
        return deleteItem(new DeleteItemRequest().withTableName(tableName).withKey(key));
    }

    @Nonnull
    default CompletableFuture<DeleteItemResult> deleteItem(@Nonnull String tableName, @Nonnull Map<String, AttributeValue> key, @Nonnull String returnValues) {
        return deleteItem(new DeleteItemRequest().withTableName(tableName).withKey(key).withReturnValues(returnValues));
    }

    @Nonnull
    CompletableFuture<ScanResult> scan(@Nonnull ScanRequest scanRequest);

    @Nonnull
    default CompletableFuture<ScanResult> scan(@Nonnull String tableName, @Nonnull List<String> attributesToGet) {
        return scan(new ScanRequest(tableName).withAttributesToGet(attributesToGet));
    }

    @Nonnull
    default CompletableFuture<ScanResult> scan(@Nonnull String tableName, @Nonnull Map<String, Condition> scanFilter) {
        return scan(new ScanRequest(tableName).withScanFilter(scanFilter));
    }

    @Nonnull
    default CompletableFuture<ScanResult> scan(@Nonnull String tableName, @Nonnull List<String> attributesToGet, @Nonnull Map<String, Condition> scanFilter) {
        return scan(new ScanRequest(tableName).withAttributesToGet(attributesToGet).withScanFilter(scanFilter));
    }

    @Nonnull CompletableFuture<UpdateItemResult> updateItem(@Nonnull UpdateItemRequest updateItemRequest);

    default @Nonnull CompletableFuture<UpdateItemResult> updateItem(String tableName, Map<String, AttributeValue> key, Map<String, AttributeValueUpdate> attributeUpdates){
        return updateItem(new UpdateItemRequest().withTableName(tableName).withKey(key).withAttributeUpdates(attributeUpdates));
    }

    default @Nonnull CompletableFuture<UpdateItemResult> updateItem(String tableName, Map<String, AttributeValue> key, Map<String, AttributeValueUpdate> attributeUpdates,
                                                                    String returnValues){
        return updateItem(new UpdateItemRequest().withTableName(tableName).withKey(key).withAttributeUpdates(attributeUpdates).withReturnValues(returnValues));
    }

    @Nonnull CompletableFuture<UpdateTableResult> updateTable(@Nonnull UpdateTableRequest updateTableRequest);

    /**
     * Simplified method form for invoking the UpdateTable operation.
     *
     * @see #updateTable(UpdateTableRequest)
     */
    default @Nonnull CompletableFuture<UpdateTableResult> updateTable(String tableName, ProvisionedThroughput provisionedThroughput){
        return updateTable(new UpdateTableRequest().withTableName(tableName).withProvisionedThroughput(provisionedThroughput));
    }

    @Nonnull CompletableFuture<TagResourceResult> tagResource(@Nonnull TagResourceRequest tagResourceRequest);

    @Nonnull CompletableFuture<UntagResourceResult> untagResource(@Nonnull UntagResourceRequest untagResourceRequest);

    @Nonnull CompletableFuture<UpdateTimeToLiveResult> updateTimeToLive(@Nonnull UpdateTimeToLiveRequest updateTimeToLiveRequest);
}
