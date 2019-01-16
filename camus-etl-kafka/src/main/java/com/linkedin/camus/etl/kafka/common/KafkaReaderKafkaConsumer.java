package com.linkedin.camus.etl.kafka.common;

import com.linkedin.camus.etl.kafka.CamusJob;
import com.linkedin.camus.etl.kafka.mapred.EtlInputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

public class KafkaReaderKafkaConsumer extends KafkaReader {
    private static Logger log = Logger.getLogger(KafkaReaderKafkaConsumer.class);
    private KafkaConsumer<byte[], byte[]> kafkaConsumer = null;
    private EtlRequest kafkaRequest = null;
    /**
     * Construct using the json representation of the kafka request
     *
     * @param inputFormat
     * @param context
     * @param request
     * @param clientTimeout
     * @param fetchBufferSize
     */
    public KafkaReaderKafkaConsumer(EtlInputFormat inputFormat, TaskAttemptContext context, EtlRequest request, int clientTimeout, int fetchBufferSize) throws Exception {
        super(inputFormat, context, request, clientTimeout, fetchBufferSize);

        Properties props = new Properties();
        props.put("bootstrap.servers", CamusJob.getKafkaBrokers(context));
        props.put("group.id", CamusJob.getKafkaClientName(context));
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        this.kafkaRequest = request;

        String topic = "foo";
        TopicPartition partition0 = new TopicPartition(this.kafkaRequest.getTopic(), this.kafkaRequest.getPartition());
        //consumer.assign(Arrays.asList(partition0, partition1));

        this.kafkaConsumer = new KafkaConsumer<byte[], byte[]>(props);
        this.kafkaConsumer.assign(Arrays.asList(partition0));
        this.kafkaConsumer.poll
    }

    @Override
    public boolean hasNext() throws IOException {
        return super.hasNext();
    }

    @Override
    public KafkaMessage getNext(EtlKey etlKey) throws IOException {
        return super.getNext(etlKey);
    }

    @Override
    public boolean fetch() throws IOException {
        //return super.fetch();
    }

    @Override
    public void close() throws IOException {
        super.close();
    }

    @Override
    public long getTotalBytes() {
        return super.getTotalBytes();
    }

    @Override
    public long getReadBytes() {
        return super.getReadBytes();
    }

    @Override
    public long getCount() {
        return super.getCount();
    }

    @Override
    public long getFetchTime() {
        return super.getFetchTime();
    }

    @Override
    public long getTotalFetchTime() {
        return super.getTotalFetchTime();
    }
}
