package org.yamunasoftware.ikgw;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

public class Gateway {
  private static final String topic = "IMADDS";
  private static final int pollingPeriod = 10000;
  private static final Logger logger = LoggerFactory.getLogger(Gateway.class);

  public static void main(String[] args) {
    HashMap<String, String> systemInfo = Input.getSystemInfo();
    String deviceID = systemInfo.get("SYSTEM_ID");
    String kafkaURL = systemInfo.get("KAFKA_URL");
    KafkaProducer<String, String> producer = setupProducer(deviceID, kafkaURL);

    while (true) {
      try {
        sendMessage(producer, deviceID);
        Thread.sleep(pollingPeriod);
      }

      catch (InterruptedException e) {
        producer.flush();
        producer.close();
        Thread.currentThread().interrupt();
        break;
      }

      catch (Exception e) {
        logger.error("Error: Main Execution\n{}\n", e.getMessage());
      }
    }
  }

  private static void sendMessage(KafkaProducer<String, String> producer, String id) throws Exception {
    String message = buildMessage(id);
    ProducerRecord<String, String> record = new ProducerRecord<>(topic, id, message);
    RecordMetadata metadata = producer.send(record).get();
    logger.info("Sent message from device {}\nPartition: {}\nOffset: {}\nTimestamp: {}\n",
        id, metadata.partition(), metadata.offset(), metadata.timestamp());
  }

  private static KafkaProducer<String, String> setupProducer(String id, String url) {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, url);
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    properties.put(ProducerConfig.CLIENT_ID_CONFIG, id);
    properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

    properties.put(ProducerConfig.RETRIES_CONFIG, 3);
    properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 60000);
    properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 15000);
    properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
    return new KafkaProducer<>(properties);
  }

  private static String buildMessage(String id) {
    StringBuilder message = new StringBuilder("{\"deviceID\":\"").append(id).append("\",");
    ArrayList<HashMap<Integer, HashMap<String, Float>>> data = Input.dataReadout();
    message.append("\"deviceData\":[");
    int count = 0;

    for (HashMap<Integer, HashMap<String, Float>> deviceData : data) {
      for (Integer channel : deviceData.keySet()) {
        if (count != 0) message.append(",");
        HashMap<String, Float> values = deviceData.get(channel);
        double temperature = values.get("Temperature");
        double pressure = values.get("Pressure");
        double humidity = values.get("Humidity");

        message.append("{\"channel\":").append(channel).append(",");
        message.append("\"temperature\":").append(temperature).append(",");
        message.append("\"pressure\":").append(pressure).append(",");
        message.append("\"humidity\":").append(humidity).append("}");
      }
      count++;
    }

    message.append("]}");
    return message.toString();
  }
}
