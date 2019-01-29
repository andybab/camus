package com.linkedin.camus.etl.kafka.mapred;

import java.io.IOException;
import java.util.List;

import kafka.javaapi.consumer.SimpleConsumer;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.kafka.common.EtlKey;
import com.linkedin.camus.workallocater.CamusRequest;


public class EtlInputFormatForUnitTest extends EtlInputFormat {
  public static enum ConsumerType {
    REGULAR,
    MOCK
  }

  public static enum RecordReaderClass {
    REGULAR,
    TEST
  }

  public static enum CamusRequestType {
    REGULAR,
    MOCK_OFFSET_TOO_EARLY,
    MOCK_OFFSET_TOO_LATE
  }

  public static SimpleConsumer consumer;
  public static ConsumerType consumerType = ConsumerType.REGULAR;
  public static RecordReaderClass recordReaderClass = RecordReaderClass.REGULAR;
  public static CamusRequestType camusRequestType = CamusRequestType.REGULAR;

  public EtlInputFormatForUnitTest() {
    super();
  }

  @Override
  public RecordReader<EtlKey, CamusWrapper> createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    switch (recordReaderClass) {
      case REGULAR:
        return new EtlRecordReader(this, split, context);
      case TEST:
        return new EtlRecordReaderForUnitTest(this, split, context);
      default:
        throw new RuntimeException("record reader class not found");
    }
  }

  protected void writeRequests(List<CamusRequest> requests, JobContext context) throws IOException {
    //do nothing
  }

  public static void modifyRequestOffsetTooEarly(CamusRequest etlRequest) {
    etlRequest.setEarliestOffset(-1L);
    etlRequest.setOffset(-2L);
    etlRequest.setLatestOffset(1L);
  }

  public static void modifyRequestOffsetTooLate(CamusRequest etlRequest) {
    etlRequest.setEarliestOffset(-1L);
    etlRequest.setOffset(2L);
    etlRequest.setLatestOffset(1L);
  }

  public static void reset() {
    consumerType = ConsumerType.REGULAR;
    recordReaderClass = RecordReaderClass.REGULAR;
    camusRequestType = CamusRequestType.REGULAR;
  }
}
