package edu.colorado.cires.cmg.echofish.aws.lambda.zarraccumulator;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedQueryList;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedScanList;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import edu.colorado.cires.cmg.echofish.data.dynamo.FileInfoRecord;
import edu.colorado.cires.cmg.echofish.data.model.CruiseProcessingMessage;
import edu.colorado.cires.cmg.echofish.data.s3.S3Operations;
import edu.colorado.cires.cmg.echofish.data.sns.SnsNotifierFactory;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZarrAccumulatorLambdaHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(ZarrAccumulatorLambdaHandler.class);

  private final SnsNotifierFactory sns;
  private final AmazonDynamoDB client;
  private final ZarrAccumulatorLambdaConfiguration configuration;

  public ZarrAccumulatorLambdaHandler(SnsNotifierFactory sns, AmazonDynamoDB client, ZarrAccumulatorLambdaConfiguration configuration) {
    this.sns = sns;
    this.client = client;
    this.configuration = configuration;
  }

  public void handleRequest(CruiseProcessingMessage message) {

    LOGGER.info("Started Event: {}", message);

    if (message.getCruiseName() == null || message.getCruiseName().isEmpty()) {
      throw new IllegalArgumentException("cruiseName is required");
    }

    if (message.getShipName() == null || message.getShipName().isEmpty()) {
      throw new IllegalArgumentException("shipName is required");
    }

    if (message.getSensorName() == null || message.getSensorName().isEmpty()) {
      throw new IllegalArgumentException("sensorName is required");
    }

    if (isCruiseComplete(message)) {
      message.setFileName(null);
      notifyTopic(message);
      LOGGER.info("Notifying: {}", message);
    }

    LOGGER.info("Finished Event: {}", message);

  }

  private void notifyTopic(CruiseProcessingMessage message) {
    sns.createNotifier().notify(configuration.getTopicArn(), message);
  }

  private boolean isCruiseComplete(CruiseProcessingMessage message) {
    DynamoDBMapper mapper = new DynamoDBMapper(client);

    Map<String, AttributeValue> eav = new HashMap<>();
    eav.put(":cruiseName", new AttributeValue().withS(message.getCruiseName()));
    eav.put(":shipName", new AttributeValue().withS(message.getShipName()));
    eav.put(":sensorName", new AttributeValue().withS(message.getSensorName()));


    DynamoDBScanExpression queryExpression = new DynamoDBScanExpression()
        .withFilterExpression("CRUISE_NAME = :cruiseName and SHIP_NAME = :shipName and SENSOR_NAME = :sensorName")
        .withExpressionAttributeValues(eav);

    PaginatedScanList<FileInfoRecord> records = mapper.scan(FileInfoRecord.class, queryExpression, DynamoDBMapperConfig.TableNameOverride.withTableNameReplacement(configuration.getTableName()).config());
    Iterator<FileInfoRecord> it = records.iterator();
    boolean hasRecord = false;
    while (it.hasNext()) {
      hasRecord = true;
      FileInfoRecord record = it.next();
      if ("PROCESSING".equals(record.getPipelineStatus())) {
        return false;
      }
    }
    return hasRecord;
  }
}
