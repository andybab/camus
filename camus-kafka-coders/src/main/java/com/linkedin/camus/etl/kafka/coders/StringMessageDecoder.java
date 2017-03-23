package com.linkedin.camus.etl.kafka.coders;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.Message;
import com.linkedin.camus.coders.MessageDecoder;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.util.Properties;


/**
 * MessageDecoder class that will set the timestamp field to System.currentTimeMillis()
 * that is set during class initialization
 *
 * This MessageDecoder returns a CamusWrapper that works with Strings payloads,
 * since JSON data is always a String.
 */
public class StringMessageDecoder extends MessageDecoder<Message, String> {
  private static final Logger log = Logger.getLogger(StringMessageDecoder.class);
  private static final long timestamp = System.currentTimeMillis();

  @Override
  public void init(Properties props, String topicName) {
    this.props = props;
    this.topicName = topicName;
  }

  @Override
  public CamusWrapper<String> decode(Message message) {

    String payloadString;

    try {
      payloadString = new String(message.getPayload(), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      log.error("Unable to load UTF-8 encoding, falling back to system default", e);
      payloadString = new String(message.getPayload());
    }

    return new CamusWrapper<String>(payloadString, timestamp);
  }
}
