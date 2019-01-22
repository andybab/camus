package com.linkedin.camus.etl.kafka.common;

import com.linkedin.camus.etl.kafka.CamusJob;
import com.linkedin.camus.etl.kafka.mapred.EtlInputFormatKafkaConsumer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

public class KafkaReader {
  private static Logger log = Logger.getLogger(KafkaReader.class);
  private KafkaConsumer<byte[], byte[]> kafkaConsumer = null;
  private Iterator<ConsumerRecord<byte[],byte[]>> messageIter = null;
  private EtlRequest kafkaRequest = null;
  private TopicPartition topicPartition = null;

  private long beginOffset;
  private long currentOffset;
  private long lastOffset;
  private long currentCount;
  private long lastFetchTime = 0;

  private TaskAttemptContext context;

  private long totalFetchTime = 0;

  /**
   * Construct using the json representation of the kafka request
   *
   * @param inputFormat
   * @param context
   * @param request
   * @param clientTimeout
   * @param fetchBufferSize
   */
  public KafkaReader(EtlInputFormatKafkaConsumer inputFormat, TaskAttemptContext context, EtlRequest request, int clientTimeout, int fetchBufferSize) throws Exception {
    //super(inputFormat, context, request, clientTimeout, fetchBufferSize);

    Properties props = new Properties();
    props.put("bootstrap.servers", CamusJob.getKafkaBrokers(context));
    props.put("group.id", CamusJob.getKafkaClientName(context));
    props.put("enable.auto.commit", "false");
    props.put("session.timeout.ms", "30000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    this.context = context;
    this.kafkaRequest = request;
    this.beginOffset = request.getOffset();
    this.currentOffset = request.getOffset();
    this.lastOffset = request.getLastOffset();
    this.currentCount = 0;
    this.totalFetchTime = 0;

    String topic = "foo";
    this.topicPartition = new TopicPartition(this.kafkaRequest.getTopic(), this.kafkaRequest.getPartition());
    //consumer.assign(Arrays.asList(topicPartition, partition1));


    this.kafkaConsumer = new KafkaConsumer<byte[], byte[]>(props);
    this.kafkaConsumer.assign(Arrays.asList(topicPartition));
    log.info("Beginning reading at offset " + beginOffset + " latest offset=" + lastOffset);
    this.fetch();
  }

  public boolean hasNext() throws IOException {
    if (currentOffset >= lastOffset) {
      return false;
    }
    if (messageIter != null && messageIter.hasNext()) {
      return true;
    } else {
      return fetch();
    }
  }

  public KafkaMessage getNext(EtlKey etlKey) throws IOException {
    if(!hasNext()) {
      return null;
    }

    ConsumerRecord<byte[], byte[]> consumerRecord = messageIter.next();

    byte[] payload = consumerRecord.value();
    byte[] key     = consumerRecord.key();

    if (payload == null) {
      log.warn("Received message with null message.payload(): " + consumerRecord);
    }

    etlKey.clear();
    etlKey.set(kafkaRequest.getTopic(), kafkaRequest.getLeaderId(), kafkaRequest.getPartition(), currentOffset,
            consumerRecord.offset() + 1, consumerRecord.checksum());

    etlKey.setMessageSize(consumerRecord.serializedKeySize() + consumerRecord.serializedValueSize());
    currentOffset = consumerRecord.offset() + 1; // increase offset
    currentCount++; // increase count

    return new KafkaMessage(payload, key, kafkaRequest.getTopic(), kafkaRequest.getPartition(),
            consumerRecord.offset(), consumerRecord.checksum());
  }

  public boolean fetch() throws IOException {
    if (currentOffset >= lastOffset) {
      return false;
    }
    long tempTime = System.currentTimeMillis();
    this.kafkaConsumer.seek(this.topicPartition, currentOffset);
    ConsumerRecords<byte[], byte[]> result = this.kafkaConsumer.poll(CamusJob.getKafkaFetchRequestMaxWait(context));

    lastFetchTime = (System.currentTimeMillis() - tempTime);
    log.debug("Time taken to fetch : " + (lastFetchTime / 1000) + " seconds");
    totalFetchTime += lastFetchTime;

    this.messageIter = result.iterator();

    return this.messageIter.hasNext();
  }

  public void close() throws IOException {
    if(this.kafkaConsumer != null) {
      this.kafkaConsumer.close();
    }
  }

  public long getTotalBytes()  {
    return (lastOffset > beginOffset) ? lastOffset - beginOffset : 0;
  }

  public long getReadBytes()  {
    return currentOffset - beginOffset;
  }

  public long getCount() {
    return this.currentCount;
  }

  public long getFetchTime()  {
    return lastFetchTime;
  }

  public long getTotalFetchTime() {
    return totalFetchTime;
  }
}
