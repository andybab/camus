package com.linkedin.camus.etl.kafka.common;

import com.linkedin.camus.etl.kafka.CamusJob;
import com.linkedin.camus.workallocater.CamusRequest;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class EtlRequest implements CamusRequest {
  private JobContext context = null;
  public static final long DEFAULT_OFFSET = 0;

  private String topic = "";
  private int partition = 0;

  private String brokers = null;
  private long offset = DEFAULT_OFFSET;
  private long latestOffset = -1;
  private long earliestOffset = -2;
  private long avgMsgSize = 1024;

  private static Logger log = null;

  public static void setLogger(Logger log) {
    EtlRequest.log = log;
  }

  private Properties getConnectionProperties() {
    Properties props = new Properties();
    props.put("bootstrap.servers", this.brokers);
    props.put("group.id", "camus-etl");
    props.put("enable.auto.commit", "false");
    props.put("session.timeout.ms", "30000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put("security.protocol", CamusJob.getKafkaConsumerSecurityProtocol(context));
    props.put("sasl.mechanism", CamusJob.getKafkaConsumerSaslMechanism(context));
    props.put("sasl.jaas.config", CamusJob.getKafkaConsumerSaslJaasConfig(context));
    return props;
  }

  private TopicPartition getTopicPartition() {
    return new TopicPartition(this.getTopic(), this.getPartition());
  }

  public EtlRequest() {
    if (log == null)
      log = Logger.getLogger(getClass());
  }

  public EtlRequest(JobContext context, String topic, int partition) {
    this.context = context;
    this.topic = topic;
    this.partition = partition;
    setOffset(offset);
    this.brokers = CamusJob.getKafkaBrokers(context);

    if (log == null)
      log = Logger.getLogger(getClass());
  }

  public EtlRequest(JobContext context, String topic, int partition, String brokers) {
    this.context = context;
    this.topic = topic;
    this.brokers = brokers;
    this.partition = partition;

    setOffset(offset);

    if (log == null)
      log = Logger.getLogger(getClass());

    log.info(this);
  }

  public EtlRequest(JobContext context, String topic, int partition, String brokers, long offset) {
    this.context = context;
    this.topic = topic;
    this.brokers = brokers;
    this.partition = partition;
    setOffset(offset);

    if (log == null)
      log = Logger.getLogger(getClass());

    log.info(this);
  }

  @Override
  public long getLastOffset(long time) {
    if (this.latestOffset != -1)
      return this.latestOffset;

    Properties props = getConnectionProperties();

    TopicPartition topicPartition = this.getTopicPartition();
    KafkaConsumer<byte[],byte[]> kafkaConsumer = new KafkaConsumer<byte[], byte[]>(props);
    Map<TopicPartition,Long> topicPartitions = kafkaConsumer.endOffsets(Collections.singletonList(topicPartition));
    //endOffsets returns " The last offset of a partition is the offset of the upcoming message, i.e. the offset of the last available message + 1."
    //so we have to decrement by one to get the last available message
    long lastOffset = topicPartitions.get(topicPartition) - 1;

    kafkaConsumer.close();
    this.latestOffset = lastOffset;
    return lastOffset;
  }

  /* (non-Javadoc)
   * @see com.linkedin.camus.etl.kafka.common.CamusRequest#estimateDataSize()
   */
  @Override
  public long estimateDataSize() {
    long endOffset = getLastOffset();
    return (endOffset - offset) * avgMsgSize;
  }

  @Override
  public void setAvgMsgSize(long size) {
    this.avgMsgSize = size;
  }

  /* (non-Javadoc)
   * @see com.linkedin.camus.etl.kafka.common.CamusRequest#estimateDataSize(long)
   */
  @Override
  public long estimateDataSize(long endTime) {
    long endOffset = getLastOffset(endTime);
    return (endOffset - offset) * avgMsgSize;
  }

  @Override
  public void setLatestOffset(long latestOffset) {
    this.latestOffset = latestOffset;
  }

  @Override
  public void setEarliestOffset(long earliestOffset) {
    this.earliestOffset = earliestOffset;
  }

  @Override
  public void setOffset(long offset) {
    this.offset = offset;
  }

  @Override
  public void setURI(URI uri) {
    throw new UnsupportedOperationException("Unsuported setURI method call");
  }

  @Override
  public String getTopic() {
    return this.topic;
  }

  @Override
  public URI getURI() {
    throw new UnsupportedOperationException("Unsuported getURI method call");
  }

  @Override
  public int getPartition() {
    return this.partition;
  }

  @Override
  public long getOffset() {
    return this.offset;
  }

  @Override
  public boolean isValidOffset() {
    return this.offset >= 0;
  }

  @Override
  public long getEarliestOffset() {
    if (this.earliestOffset != -2 || this.brokers == null) {
      return this.earliestOffset;
    }
    Properties props = getConnectionProperties();

    TopicPartition topicPartition = this.getTopicPartition();
    KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<byte[], byte[]>(props);
    Map<TopicPartition, Long> topicPartitions = kafkaConsumer.beginningOffsets(Collections.singletonList(topicPartition));

    long earliestOffset = topicPartitions.get(topicPartition);

    kafkaConsumer.close();

    return earliestOffset;
  }

  @Override
  public long getLastOffset() {
    if (this.latestOffset == -1 && brokers != null)
      return getLastOffset(kafka.api.OffsetRequest.LatestTime());
    else {
      return this.latestOffset;
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    log.info("EtlRequest to be written: " + this);
    Text.writeString(out, topic);
    if (this.brokers != null)
      Text.writeString(out, this.brokers.toString());
    else
      Text.writeString(out, "");
    out.writeInt(partition);
    out.writeLong(offset);
    out.writeLong(latestOffset);
  }

  @Override
  public String toString() {
    return "EtlRequest{" +
        "topic='" + topic + '\'' +
        ", partition=" + partition +
        ", brokers='" + brokers + '\'' +
        ", offset=" + offset +
        ", latestOffset=" + latestOffset +
        ", earliestOffset=" + earliestOffset +
        ", avgMsgSize=" + avgMsgSize +
        '}';
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    topic = Text.readString(in);
    String brokersStr = Text.readString(in);

    /*if (!brokersStr.isEmpty()) //We always use broker string from configuration
      this.brokers = brokersStr;*/
    partition = in.readInt();
    offset = in.readLong();
    latestOffset = in.readLong();

    log.info("Loaded EtlRequest: " + this);
  }

  /**
   * Returns the copy of KafkaETLRequest
   */
  @Override
  public CamusRequest clone() {
    return new EtlRequest(context, topic, partition, brokers, offset);
  }
}
