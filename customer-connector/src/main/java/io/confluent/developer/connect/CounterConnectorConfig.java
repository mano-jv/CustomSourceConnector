package io.confluent.developer.connect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;

public class CounterConnectorConfig extends AbstractConfig {

  public static final String KAFKA_TOPIC_CONF = "kafka.topic";
  public static final String PAYLOAD_STRING = "input.payload";
  private static final String KAFKA_TOPIC_DOC = "Topic to write to";
  private static final String PAYLOAD_DOC = "Convert payload to";
  public static final String INTERVAL_CONF = "interval.ms";
  private static final String INTERVAL_DOC = "Interval between messages (ms)";

  public CounterConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
    super(config, parsedConfig);
  }

  public CounterConnectorConfig(Map<String, String> parsedConfig) {
    this(conf(), parsedConfig);
  }

  public static ConfigDef conf() {
    return new ConfigDef()
        .define(KAFKA_TOPIC_CONF, Type.STRING, Importance.HIGH, KAFKA_TOPIC_DOC)
        .define(PAYLOAD_STRING, Type.STRING, Importance.HIGH, PAYLOAD_DOC)
        .define(INTERVAL_CONF, Type.LONG, 5_000L, Importance.HIGH, INTERVAL_DOC);
  }

  public String getKafkaTopic() {
    return this.getString(KAFKA_TOPIC_CONF);
  }

  public Long getInterval() {
    return this.getLong(INTERVAL_CONF);
  }

  public String getPayloadString() {
    return this.getString(PAYLOAD_STRING);
  }
}
