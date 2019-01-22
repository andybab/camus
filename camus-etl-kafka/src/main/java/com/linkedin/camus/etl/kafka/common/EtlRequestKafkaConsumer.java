package com.linkedin.camus.etl.kafka.common;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class EtlRequestKafkaConsumer extends EtlRequest {
  private Properties getConnectionProperties() {
    Properties props = new Properties();
    props.put("bootstrap.servers", super.getURI());
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
    super();
  }

  public EtlRequestKafkaConsumer(EtlRequest other) {
    super(other);
  }

  public EtlRequestKafkaConsumer(JobContext context, String topic, String leaderId, int partition) {
    super(context, topic, leaderId, partition);
  }

  public EtlRequestKafkaConsumer(JobContext context, String topic, String leaderId, int partition, URI brokerUri) {
    super(context, topic, leaderId, partition, brokerUri);
  }

  public EtlRequestKafkaConsumer(JobContext context, String topic, String leaderId, int partition, URI brokerUri, long offset) {
    super(context, topic, leaderId, partition, brokerUri, offset);
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
  public long getEarliestOffset() {
    Properties props = getConnectionProperties();

    TopicPartition topicPartition = this.getTopicPartition();
    KafkaConsumer<byte[],byte[]> kafkaConsumer = new KafkaConsumer<byte[], byte[]>(props);
    Map<TopicPartition,Long> topicPartitions = kafkaConsumer.beginningOffsets(Collections.singletonList(topicPartition));

    long earliestOffset = topicPartitions.get(topicPartition);

    kafkaConsumer.close();

    return earliestOffset;
  }
}
