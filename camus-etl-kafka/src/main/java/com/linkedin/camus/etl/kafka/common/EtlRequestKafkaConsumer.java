package com.linkedin.camus.etl.kafka.common;

import com.linkedin.camus.workallocater.CamusRequest;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class EtlRequestKafkaConsumer implements CamusRequest {
  private JobContext context = null;
  public static final long DEFAULT_OFFSET = 0;

  private String topic = "";
  private String leaderId = "";
  private int partition = 0;

  private URI uri = null;
  private long offset = DEFAULT_OFFSET;
  private long latestOffset = -1;
  private long earliestOffset = -2;


  private Properties getConnectionProperties() {
    Properties props = new Properties();
    props.put("bootstrap.servers", this.getURI());
    props.put("group.id", "camus-etl");
    props.put("enable.auto.commit", "false");
    props.put("session.timeout.ms", "30000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    return props;
  }

  private TopicPartition getTopicPartition() {
    return new TopicPartition(this.getTopic(), this.getPartition());
  }

  public EtlRequestKafkaConsumer() {
  }

  public EtlRequestKafkaConsumer(JobContext context, String topic, int partition) {
  }

  public EtlRequestKafkaConsumer(JobContext context, String topic, int partition, URI brokerUri) {
  }

  public EtlRequestKafkaConsumer(JobContext context, String topic, int partition, URI brokerUri, long offset) {
    this.context = context;
    this.topic = topic;
    this.leaderId = leaderId;
    this.uri = brokerUri;
    this.partition = partition;
    setOffset(offset);
  }

  @Override
  public long getLastOffset(long time) {
    Properties props = getConnectionProperties();

    TopicPartition topicPartition = this.getTopicPartition();
    KafkaConsumer<byte[],byte[]> kafkaConsumer = new KafkaConsumer<byte[], byte[]>(props);
    Map<TopicPartition,Long> topicPartitions = kafkaConsumer.endOffsets(Collections.singletonList(topicPartition));
    //endOffsets returns " The last offset of a partition is the offset of the upcoming message, i.e. the offset of the last available message + 1."
    //so we have to decrement by one to get the last available message
    long lastOffset = topicPartitions.get(topicPartition) - 1;

    kafkaConsumer.close();

    return lastOffset;
  }

  @Override
  public long estimateDataSize() {
    return 0;
  }

  @Override
  public void setAvgMsgSize(long size) {

  }

  @Override
  public long estimateDataSize(long endTime) {
    return 0;
  }

  @Override
  public void setLatestOffset(long latestOffset) {

  }

  @Override
  public void setEarliestOffset(long earliestOffset) {

  }

  @Override
  public void setOffset(long offset) {

  }

  @Override
  public void setURI(URI uri) {

  }

  @Override
  public String getTopic() {
    return null;
  }

  @Override
  public URI getURI() {
    return null;
  }

  @Override
  public int getPartition() {
    return 0;
  }

  @Override
  public long getOffset() {
    return 0;
  }

  @Override
  public boolean isValidOffset() {
    return false;
  }

  @Override
  public long getEarliestOffset() {
    Properties props = getConnectionProperties();

    TopicPartition topicPartition = this.getTopicPartition();
    KafkaConsumer<byte[],byte[]> kafkaConsumer = new KafkaConsumer<byte[], byte[]>(props);
    Map<TopicPartition,Long> topicPartitions = kafkaConsumer.beginningOffsets(Collections.singletonList(topicPartition));

    long earliestOffset = topicPartitions.get(topicPartition);

    kafkaConsumer.close();

    return earliestOffset;
  }

  @Override
  public long getLastOffset() {
    return 0;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {

  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {

  }
}
