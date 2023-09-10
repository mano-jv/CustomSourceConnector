package io.confluent.developer.connect;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

@Slf4j
public class CounterSourceTask extends SourceTask {

  public static final String TASK_ID = "task.id";
  RestTemplate client = new RestTemplate();
  public static final String CURRENT_ITERATION = "current.iteration";

  private CounterConnectorConfig config;
  ObjectMapper objectMapper = new ObjectMapper();
  private String topic;
  private long interval;
  private String payload;
  private long count = 0L;
  private int taskId;
  private Map sourcePartition;

  @Override
  public String version() {
    return CounterSourceTask.class.getPackage().getImplementationVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    config = new CounterConnectorConfig(props);
    topic = config.getKafkaTopic();
    interval = config.getInterval();
    payload = config.getPayloadString();
    taskId = Integer.parseInt(props.get(TASK_ID));
    sourcePartition = Collections.singletonMap(TASK_ID, taskId);

    Map<String, Object> offset = context.offsetStorageReader().offset(sourcePartition);
    if (offset != null) {
      // the offset contains our next state, so restore it as is
      count = ((Long) offset.get(CURRENT_ITERATION));
    }
  }

  @Override
  public List<SourceRecord> poll() {


    if (interval > 0) {
      try {
        Thread.sleep(interval);
      } catch (InterruptedException e) {
        Thread.interrupted();
        return null;
      }
    }

    RandomObject requestPayload = new RandomObject();
    requestPayload.setMappingId("*********");
    requestPayload.setPayload(payload);

    String url = "https://cloud.dev.api.trimblecloud.com/transportation/transformation/v1/transform";

    String jsonInputString = null;
    try {
      jsonInputString = objectMapper.writeValueAsString(requestPayload);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    headers.set("Authorization",
        "Bearer ************************");

    HttpEntity<String> requestEntity = new HttpEntity<>(jsonInputString, headers);

    RestTemplate restTemplate = new RestTemplate();
    ResponseEntity<String> responseEntity = restTemplate.exchange(url, HttpMethod.POST, requestEntity, String.class);

    String responseBody = responseEntity.getBody();
    Map sourceOffset = Collections.singletonMap(CURRENT_ITERATION, count + 1);

    final List<SourceRecord> records = Collections.singletonList(new SourceRecord(
        sourcePartition,
        sourceOffset,
        topic,
        null,
        Schema.STRING_SCHEMA,
        responseBody
    ));
    count++;
    return records;
  }

  @Override
  public void stop() {
  }
}
