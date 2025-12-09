package org.yamunasoftware.ikgw;

import com.pi4j.Pi4J;
import com.pi4j.context.Context;
import com.pi4j.io.i2c.I2C;
import com.pi4j.io.i2c.I2CConfig;
import com.pi4j.io.i2c.I2CProvider;

import java.io.InputStream;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import java.util.ArrayList;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import one.microproject.rpi.hardware.gpio.sensors.BME280;
import one.microproject.rpi.hardware.gpio.sensors.BME280Builder;
import one.microproject.rpi.hardware.gpio.sensors.impl.BME280Impl;

public class Input {
  private static final String systemInfoFile = ".sysinfo";
  private static final Logger logger = LoggerFactory.getLogger(Input.class);

  private static final int address = 0x70;
  private static final int channels = 8;

  private static final Context context = Pi4J.newAutoContext();
  private static final I2CConfig config = I2C.newConfigBuilder(context).id("TCA9548A").bus(1).device(address).build();
  private static final I2CProvider provider = context.provider("linuxfs-i2c");
  private static final I2C multiplexer = provider.create(config);

  public static ArrayList<HashMap<Integer, HashMap<String, Float>>> dataReadout() {
    ArrayList<HashMap<Integer, HashMap<String, Float>>> readout = new ArrayList<>();
    for (int channel = 0; channel < channels; channel++) {
      HashMap<Integer, HashMap<String, Float>> sensorData = readChannel(channel);
      readout.add(sensorData);
    }
    return readout;
  }

  private static HashMap<Integer, HashMap<String, Float>> readChannel(int channel) {
    HashMap<Integer, HashMap<String, Float>> sensorData = new HashMap<>();
    int channelByte = 1 << channel;
    multiplexer.write((byte)(channelByte));

    try (BME280 bme280 = BME280Builder.get().context(context).build()) {
      HashMap<String, Float> values = new HashMap<>();
      BME280Impl.Data data = bme280.getSensorValues();

      values.put("Temperature", data.getTemperature());
      values.put("Pressure", (data.getPressure() / 1000));
      values.put("Humidity", (data.getRelativeHumidity()));
      sensorData.put(channel, values);
    }

    catch (Exception e) {
      logger.error("Error: Unable to Read Sensor on Channel {}\n{}\n", channel, e.getMessage());
    }
    return sensorData;
  }

  public static HashMap<String, String> getSystemInfo() {
    HashMap<String, String> systemInfo = new HashMap<>();
    try {
      InputStream stream = Input.class.getClassLoader().getResourceAsStream(systemInfoFile);

      if (stream == null) {
        throw new FileNotFoundException("Unable to Open System File");
      }

      BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
      String line = "";
      while ((line = reader.readLine()) != null) {
        if (line.contains("SYSTEM_ID")) {
          String id = line.replace("SYSTEM_INFO=", "");
          systemInfo.put("SYSTEM_ID", id);
        }

        if (line.contains("KAFKA_URL")) {
          String url = line.replace("KAFKA_URL=", "");
          systemInfo.put("KAFKA_URL", url);
        }

        if (line.contains("KAFKA_TOPIC")) {
          String topic = line.replace("KAFKA_TOPIC=", "");
          systemInfo.put("KAFKA_TOPIC", topic);
        }
      }
    }

    catch (FileNotFoundException e) {
      logger.error("Error: Unable to Open System Info\n{}\n", e.getMessage());
    }

    catch (IOException e) {
      logger.error("Error: Unable to Read System Info\n{}\n", e.getMessage());
    }

    catch (Exception e) {
      logger.error("Error: Error While Reading System Info\n{}\n", e.getMessage());
    }
    return systemInfo;
  }
}