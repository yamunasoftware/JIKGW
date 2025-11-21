package org.yamunasoftware.ikgw;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

public class Gateway {
  private static final int pollingPeriod = 10000;
  private static final Logger logger = LoggerFactory.getLogger(Gateway.class);

  public static void main(String[] args) {
    while (true) {
      try {
        sendMessage();
      } catch (Exception e) {
        logger.error("Error: Main Execution\n{}\n", e.getMessage());
      }

      try {
        Thread.sleep(pollingPeriod);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
  }

  private static void sendMessage() throws Exception {
    HashMap<String, String> systemInfo = Input.getSystemInfo();
    String deviceID = systemInfo.get("SYSTEM_ID");
    String key = deviceID + "||" + Instant.now().getEpochSecond();

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, systemInfo.get("KAFKA_URL"));
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.CLIENT_ID_CONFIG, deviceID);

    try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
      String message = buildMessage(deviceID);
      ProducerRecord<String, String> record = new ProducerRecord<>(systemInfo.get("KAFKA_TOPIC"), key, message);
      RecordMetadata metadata = producer.send(record).get();
      logger.info("\nSent:\nKey:{}", record.key());
    }
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
