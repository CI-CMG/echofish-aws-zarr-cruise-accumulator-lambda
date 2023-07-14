package edu.colorado.cires.cmg.echofish.aws.lambda.zarraccumulator;

public class ZarrAccumulatorLambdaConfiguration {

  private final String topicArn;
  private final String tableName;

  public ZarrAccumulatorLambdaConfiguration(String topicArn, String tableName) {
    this.topicArn = topicArn;
    this.tableName = tableName;
  }

  public String getTopicArn() {
    return topicArn;
  }

  public String getTableName() {
    return tableName;
  }
}
