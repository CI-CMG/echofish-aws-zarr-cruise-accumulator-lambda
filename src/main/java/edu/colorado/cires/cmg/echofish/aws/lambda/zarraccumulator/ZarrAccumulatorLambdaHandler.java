package edu.colorado.cires.cmg.echofish.aws.lambda.zarraccumulator;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedScanList;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import edu.colorado.cires.cmg.echofish.data.dynamo.FileInfoRecord;
import edu.colorado.cires.cmg.echofish.data.dynamo.FileInfoRecord.PipelineStatus;
import edu.colorado.cires.cmg.echofish.data.model.CruiseProcessingMessage;
import edu.colorado.cires.cmg.echofish.data.sns.SnsNotifierFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

    CruiseProcessingStatus processingStatus = isCruiseComplete(message);

    if (processingStatus.isCruiseComplete()) {
      processingStatus.getCompletedFiles().forEach(this::setProcessingFileStatus);
      message.setFileName(null);
      LOGGER.info("Notifying: {}", message);
      notifyTopic(message);
    }

    LOGGER.info("Finished Event: {}", message);

  }

  private void notifyTopic(CruiseProcessingMessage message) {
    sns.createNotifier().notify(configuration.getTopicArn(), message);
  }

  private static class FileKey {

    private final String fileName;
    private final String cruiseName;

    public FileKey(String fileName, String cruiseName) {
      this.fileName = fileName;
      this.cruiseName = cruiseName;
    }

    public String getFileName() {
      return fileName;
    }

    public String getCruiseName() {
      return cruiseName;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      FileKey fileKey = (FileKey) o;
      return Objects.equals(fileName, fileKey.fileName) && Objects.equals(cruiseName, fileKey.cruiseName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(fileName, cruiseName);
    }

    @Override
    public String toString() {
      return "FileKey{" +
          "fileName='" + fileName + '\'' +
          ", cruiseName='" + cruiseName + '\'' +
          '}';
    }
  }

  private static class CruiseProcessingStatus {

    private final boolean cruiseComplete;
    private final List<FileKey> completedFiles;

    public CruiseProcessingStatus(boolean cruiseComplete, List<FileKey> completedFiles) {
      this.cruiseComplete = cruiseComplete;
      this.completedFiles = completedFiles;
    }

    public boolean isCruiseComplete() {
      return cruiseComplete;
    }

    public List<FileKey> getCompletedFiles() {
      return completedFiles;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      CruiseProcessingStatus that = (CruiseProcessingStatus) o;
      return cruiseComplete == that.cruiseComplete && Objects.equals(completedFiles, that.completedFiles);
    }

    @Override
    public int hashCode() {
      return Objects.hash(cruiseComplete, completedFiles);
    }

    @Override
    public String toString() {
      return "CruiseProcessingStatus{" +
          "cruiseComplete=" + cruiseComplete +
          ", completedFiles=" + completedFiles +
          '}';
    }
  }

  private static int getProcessingStatusOrdinal(String ps) {
    try {
      ProcessingStatus status = ProcessingStatus.valueOf(ps);
      return status.ordinal();
    } catch (IllegalArgumentException e){
      return Integer.MAX_VALUE;
    }

  }
  public enum ProcessingStatus {
    INITIALIZING_CRUISE_ZARR,
    PROCESSING_CRUISE_SPLITTER,
    SUCCESS_CRUISE_SPLITTER,
    PROCESSING_RAW_TO_ZARR,
    SUCCESS_RAW_TO_ZARR,
    PROCESSING_CREATE_EMPTY_ZARR_STORE,
    SUCCESS_CREATE_EMPTY_ZARR_STORE,
    FAILURE_RAW_TO_ZARR,
    FAILURE_CRUISE_SPLITTER,
    FAILURE_CREATE_EMPTY_ZARR_STORE,
  }

  private void setProcessingFileStatus(FileKey fileKey) {
    LOGGER.info("Updating Database: {}", fileKey);
    DynamoDBMapper mapper = new DynamoDBMapper(client);
    FileInfoRecord record = mapper.load(
        FileInfoRecord.class,
        fileKey.getFileName(),
        fileKey.getCruiseName(),
        DynamoDBMapperConfig.TableNameOverride.withTableNameReplacement(configuration.getTableName()).config());
    record.setPipelineStatus(PipelineStatus.INITIALIZING_CRUISE_ZARR);
    mapper.save(record, DynamoDBMapperConfig.TableNameOverride.withTableNameReplacement(configuration.getTableName()).config());
  }

  private CruiseProcessingStatus isCruiseComplete(CruiseProcessingMessage message) {
    DynamoDBMapper mapper = new DynamoDBMapper(client);

    Map<String, AttributeValue> eav = new HashMap<>();
    eav.put(":cruiseName", new AttributeValue().withS(message.getCruiseName()));
    eav.put(":shipName", new AttributeValue().withS(message.getShipName()));
    eav.put(":sensorName", new AttributeValue().withS(message.getSensorName()));

    DynamoDBScanExpression queryExpression = new DynamoDBScanExpression()
        .withFilterExpression("CRUISE_NAME = :cruiseName and SHIP_NAME = :shipName and SENSOR_NAME = :sensorName")
        .withExpressionAttributeValues(eav);

    List<FileKey> completedFiles = new ArrayList<>();
    List<FileKey> processingFiles = new ArrayList<>();
    boolean skip = false;

    PaginatedScanList<FileInfoRecord> records = mapper.scan(FileInfoRecord.class, queryExpression,
        DynamoDBMapperConfig.TableNameOverride.withTableNameReplacement(configuration.getTableName()).config());
    Iterator<FileInfoRecord> it = records.iterator();
//    while (!skip && it.hasNext()) {
    while (it.hasNext()) {
      FileInfoRecord record = it.next();
      LOGGER.info("filename {}, pipeline status {}", record.getFileName(), record.getPipelineStatus());
      Integer status = getProcessingStatusOrdinal(record.getPipelineStatus());
      if(status <= ProcessingStatus.PROCESSING_RAW_TO_ZARR.ordinal()){
        processingFiles.add(new FileKey(record.getFileName(), record.getCruiseName()));
      } else if(status == ProcessingStatus.SUCCESS_RAW_TO_ZARR.ordinal()) {
        completedFiles.add(new FileKey(record.getFileName(), record.getCruiseName()));
      }
    }

    boolean completed = !completedFiles.isEmpty() && processingFiles.isEmpty();
    LOGGER.info("Records: {}, completed {}", records.size(), completed);
    System.out.printf("Records: %s, completed %s, completedFiles: %s, processingFiles: %s", records.size(), completed, completedFiles.size(), processingFiles.size());

    return new CruiseProcessingStatus(completed, completedFiles);
  }
}
