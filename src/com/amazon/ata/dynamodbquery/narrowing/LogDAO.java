package com.amazon.ata.dynamodbquery.narrowing;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedQueryList;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LogDAO {

    private DynamoDBMapper mapper;

    /**
     * Allows access to and manipulation of Log objects from the data store.
     * @param mapper Access to DynamoDB
     */
    public LogDAO(DynamoDBMapper mapper) {
        this.mapper = mapper;
    }

    /**
     * Uses the query() method to retrieve all the items from the LogEntries table that have a given partition key value
     * and the sort key value is between two given times.
     * @param logLevel the given partition key
     * @param startTime the given start time
     * @param endTime the given end time
     * @return the PaginatedQueryList that is returned from the query
     */
    public List<Log> getLogsBetweenTimes(String logLevel, String startTime, String endTime) {

        Map<String, AttributeValue> valueMap = new HashMap<>();
        valueMap.put(":logLevel", new AttributeValue().withS(logLevel));
        valueMap.put(":startTime", new AttributeValue().withS(startTime));
        valueMap.put(":endTime", new AttributeValue().withS(endTime));

        // Define the key condition expression.
        String keyConditionExpression = "log_level = :logLevel AND time_stamp BETWEEN :startTime AND :endTime";

        // Create the query expression.
        DynamoDBQueryExpression<Log> queryExpression = new DynamoDBQueryExpression<Log>()
                .withKeyConditionExpression(keyConditionExpression)
                .withExpressionAttributeValues(valueMap);

        // Query the table and return the results.
        return mapper.query(Log.class, queryExpression);
    }

    /**
     * Uses the query() method to retrieve all the items from the LogEntries table that have a given partition key value
     * and the sort key value that is before a given time.
     * @param logLevel the given partition key
     * @param endTime the given end time
     * @return the PaginatedQueryList that is returned from the query
     */
    public List<Log> getLogsBeforeTime(String logLevel, String endTime) {
        // Create the value map for placeholders in the query expression.
        Map<String, AttributeValue> valueMap = new HashMap<>();
        valueMap.put(":logLevel", new AttributeValue().withS(logLevel));
        valueMap.put(":endTime", new AttributeValue().withS(endTime));

        // Define the key condition expression.
        String keyConditionExpression = "log_level = :logLevel AND time_stamp < :endTime";

        // Create the query expression.
        DynamoDBQueryExpression<Log> queryExpression = new DynamoDBQueryExpression<Log>()
                .withKeyConditionExpression(keyConditionExpression)
                .withExpressionAttributeValues(valueMap);

        // Query the table and return the results.
        return mapper.query(Log.class, queryExpression);
    }

    /**
     * Uses the query() method to retrieve all the items from the LogEntries table that have a given partition key value
     * and the sort key value that is after a given time.
     * @param logLevel the given partition key
     * @param startTime the given start time
     * @return the PaginatedQueryList that is returned from the query
     */
    public List<Log> getLogsAfterTime(String logLevel, String startTime) {
        // Create the value map for placeholders in the query expression.
        Map<String, AttributeValue> valueMap = new HashMap<>();
        valueMap.put(":logLevel", new AttributeValue().withS(logLevel));
        valueMap.put(":startTime", new AttributeValue().withS(startTime));

        // Define the key condition expression.
        String keyConditionExpression = "log_level = :logLevel AND time_stamp > :startTime";

        // Create the query expression.
        DynamoDBQueryExpression<Log> queryExpression = new DynamoDBQueryExpression<Log>()
                .withKeyConditionExpression(keyConditionExpression)
                .withExpressionAttributeValues(valueMap);

        // Query the table and return the results.
        return mapper.query(Log.class, queryExpression);

    }

}
