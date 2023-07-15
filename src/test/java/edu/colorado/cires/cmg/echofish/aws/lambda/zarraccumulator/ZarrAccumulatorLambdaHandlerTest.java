package edu.colorado.cires.cmg.echofish.aws.lambda.zarraccumulator;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import edu.colorado.cires.cmg.echofish.data.dynamo.FileInfoRecord;
import edu.colorado.cires.cmg.echofish.data.model.CruiseProcessingMessage;
import edu.colorado.cires.cmg.echofish.data.sns.SnsNotifier;
import edu.colorado.cires.cmg.echofish.data.sns.SnsNotifierFactory;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ZarrAccumulatorLambdaHandlerTest {

  private static final String TABLE_NAME = "FILE_INFO";
  private static final String TOPIC_ARN = "MOCK_TOPIC";
  private static final Instant TIME = Instant.now();


  private AmazonDynamoDB dynamo;
  private ZarrAccumulatorLambdaHandler handler;
  private SnsNotifierFactory sns;
  private DynamoDBMapper mapper;


  @BeforeEach
  public void before() throws Exception {
    System.setProperty("sqlite4java.library.path", "native-libs");
    dynamo = DynamoDBEmbedded.create().amazonDynamoDB();
    mapper = new DynamoDBMapper(dynamo);
    sns = mock(SnsNotifierFactory.class);
    handler = new ZarrAccumulatorLambdaHandler(
        sns,
        dynamo,
        new ZarrAccumulatorLambdaConfiguration(
            TOPIC_ARN,
            TABLE_NAME
        ));
    createTable(dynamo, TABLE_NAME, "FILE_NAME", "CRUISE_NAME");
  }

  @AfterEach
  public void after() throws Exception {
    dynamo.shutdown();
  }

  @Test
  public void testComplete() throws Exception {

    FileInfoRecord record = new FileInfoRecord();
    record.setCruiseName("HB0707");
    record.setShipName("Henry_B._Bigelow");
    record.setSensorName("EK60");
    record.setPipelineStatus("SUCCESS");
    record.setPipelineTime(TIME.toString());
    record.setFileName("foo");
    mapper.save(record, DynamoDBMapperConfig.TableNameOverride.withTableNameReplacement(TABLE_NAME).config());

    record = new FileInfoRecord();
    record.setCruiseName("HB0707");
    record.setShipName("Henry_B._Bigelow");
    record.setSensorName("EK60");
    record.setPipelineStatus("FAILURE");
    record.setPipelineTime(TIME.toString());
    record.setFileName("bar");
    mapper.save(record, DynamoDBMapperConfig.TableNameOverride.withTableNameReplacement(TABLE_NAME).config());

    record = new FileInfoRecord();
    record.setCruiseName("HB0707");
    record.setShipName("Henry_B._Bigelow");
    record.setSensorName("EK60");
    record.setPipelineStatus("SUCCESS");
    record.setPipelineTime(TIME.toString());
    record.setFileName("foobar");
    mapper.save(record, DynamoDBMapperConfig.TableNameOverride.withTableNameReplacement(TABLE_NAME).config());

    record = new FileInfoRecord();
    record.setCruiseName("NOT_HB0707");
    record.setShipName("Henry_B._Bigelow");
    record.setSensorName("EK60");
    record.setPipelineStatus("PROCESSING");
    record.setPipelineTime(TIME.toString());
    record.setFileName("foobar");
    mapper.save(record, DynamoDBMapperConfig.TableNameOverride.withTableNameReplacement(TABLE_NAME).config());


    SnsNotifier snsNotifier = mock(SnsNotifier.class);
    when(sns.createNotifier()).thenReturn(snsNotifier);


    CruiseProcessingMessage message = new CruiseProcessingMessage();
    message.setCruiseName("HB0707");
    message.setShipName("Henry_B._Bigelow");
    message.setSensorName("EK60");
    message.setFileName("foo");

    handler.handleRequest(message);

    CruiseProcessingMessage expectedMessage = new CruiseProcessingMessage();
    expectedMessage.setCruiseName("HB0707");
    expectedMessage.setShipName("Henry_B._Bigelow");
    expectedMessage.setSensorName("EK60");

    verify(snsNotifier).notify(eq(TOPIC_ARN), eq(expectedMessage));
  }

  @Test
  public void testNotComplete() throws Exception {

    FileInfoRecord record = new FileInfoRecord();
    record.setCruiseName("HB0707");
    record.setShipName("Henry_B._Bigelow");
    record.setSensorName("EK60");
    record.setPipelineStatus("SUCCESS");
    record.setPipelineTime(TIME.toString());
    record.setFileName("foo");
    mapper.save(record, DynamoDBMapperConfig.TableNameOverride.withTableNameReplacement(TABLE_NAME).config());

    record = new FileInfoRecord();
    record.setCruiseName("HB0707");
    record.setShipName("Henry_B._Bigelow");
    record.setSensorName("EK60");
    record.setPipelineStatus("PROCESSING");
    record.setPipelineTime(TIME.toString());
    record.setFileName("bar");
    mapper.save(record, DynamoDBMapperConfig.TableNameOverride.withTableNameReplacement(TABLE_NAME).config());

    record = new FileInfoRecord();
    record.setCruiseName("HB0707");
    record.setShipName("Henry_B._Bigelow");
    record.setSensorName("EK60");
    record.setPipelineStatus("SUCCESS");
    record.setPipelineTime(TIME.toString());
    record.setFileName("foobar");
    mapper.save(record, DynamoDBMapperConfig.TableNameOverride.withTableNameReplacement(TABLE_NAME).config());

    record = new FileInfoRecord();
    record.setCruiseName("NOT_HB0707");
    record.setShipName("Henry_B._Bigelow");
    record.setSensorName("EK60");
    record.setPipelineStatus("PROCESSING");
    record.setPipelineTime(TIME.toString());
    record.setFileName("foobar");
    mapper.save(record, DynamoDBMapperConfig.TableNameOverride.withTableNameReplacement(TABLE_NAME).config());


    SnsNotifier snsNotifier = mock(SnsNotifier.class);
    when(sns.createNotifier()).thenReturn(snsNotifier);


    CruiseProcessingMessage message = new CruiseProcessingMessage();
    message.setCruiseName("HB0707");
    message.setShipName("Henry_B._Bigelow");
    message.setSensorName("EK60");
    message.setFileName("foo");

    handler.handleRequest(message);

    CruiseProcessingMessage expectedMessage = new CruiseProcessingMessage();
    expectedMessage.setCruiseName("HB0707");
    expectedMessage.setShipName("Henry_B._Bigelow");
    expectedMessage.setSensorName("EK60");

    verify(snsNotifier, times(0)).notify(any(), any());
  }

  @Test
  public void testNoRecords() throws Exception {

    FileInfoRecord record = new FileInfoRecord();
    record.setCruiseName("NOT_HB0707");
    record.setShipName("Henry_B._Bigelow");
    record.setSensorName("EK60");
    record.setPipelineStatus("SUCCESS");
    record.setPipelineTime(TIME.toString());
    record.setFileName("foobar");
    mapper.save(record, DynamoDBMapperConfig.TableNameOverride.withTableNameReplacement(TABLE_NAME).config());


    SnsNotifier snsNotifier = mock(SnsNotifier.class);
    when(sns.createNotifier()).thenReturn(snsNotifier);


    CruiseProcessingMessage message = new CruiseProcessingMessage();
    message.setCruiseName("HB0707");
    message.setShipName("Henry_B._Bigelow");
    message.setSensorName("EK60");
    message.setFileName("foo");

    handler.handleRequest(message);

    CruiseProcessingMessage expectedMessage = new CruiseProcessingMessage();
    expectedMessage.setCruiseName("HB0707");
    expectedMessage.setShipName("Henry_B._Bigelow");
    expectedMessage.setSensorName("EK60");

    verify(snsNotifier, times(0)).notify(any(), any());
  }

  private static CreateTableResult createTable(AmazonDynamoDB ddb, String tableName, String hashKeyName, String rangeKeyName) {
    List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
    attributeDefinitions.add(new AttributeDefinition(hashKeyName, ScalarAttributeType.S));
    attributeDefinitions.add(new AttributeDefinition(rangeKeyName, ScalarAttributeType.S));

    List<KeySchemaElement> ks = new ArrayList<>();
    ks.add(new KeySchemaElement(hashKeyName, KeyType.HASH));
    ks.add(new KeySchemaElement(rangeKeyName, KeyType.RANGE));

    ProvisionedThroughput provisionedthroughput = new ProvisionedThroughput(1000L, 1000L);

    CreateTableRequest request =
        new CreateTableRequest()
            .withTableName(tableName)
            .withAttributeDefinitions(attributeDefinitions)
            .withKeySchema(ks)
            .withProvisionedThroughput(provisionedthroughput);

    return ddb.createTable(request);
  }

}