package org.traccar.mqtt;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.Context;
import org.traccar.iotm.IotmStaticSignal;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Subscribes to a control topic (default: traccar/command/+/+),
 * accepts "static1|static2" as the last topic token with payload "0" or "1",
 * and publishes an IoTM Output Control payload to "<IMEI>/OUTC" (QoS 1).
 *
 * Examples:
 *   traccar/command/862123456789012/static1   payload: "1"  -> STATIC SIGNAL1 = 1
 *   traccar/command/862123456789012/static2   payload: "0"  -> STATIC SIGNAL2 = 0
 */
public final class MqttCommandService implements AutoCloseable, MqttCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(MqttCommandService.class);

    private final MqttClient client;
    private final String topicPattern;
    private final int qos;
    private final AtomicInteger uniq = new AtomicInteger(0);

    public MqttCommandService() throws Exception {
        var cfg = Context.getConfig();
        if (!cfg.getBoolean("mqtt.cmd.enable", false)) {
            client = null;
            topicPattern = null;
            qos = 0;
            LOGGER.info("MQTT command service disabled (mqtt.cmd.enable=false)");
            return;
        }

        String url = cfg.getString("mqtt.cmd.url", "tcp://localhost:1883");
        String clientId = cfg.getString("mqtt.cmd.clientId", "traccar-cmd-" + System.nanoTime());
        this.topicPattern = cfg.getString("mqtt.cmd.topic", "traccar/command/+/+");
        this.qos = cfg.getInteger("mqtt.cmd.qos", 1);

        MqttConnectOptions opts = new MqttConnectOptions();
        if (cfg.hasKey("mqtt.cmd.username")) opts.setUserName(cfg.getString("mqtt.cmd.username"));
        if (cfg.hasKey("mqtt.cmd.password")) opts.setPassword(cfg.getString("mqtt.cmd.password").toCharArray());
        opts.setAutomaticReconnect(true);
        opts.setCleanSession(true);

        this.client = new MqttClient(url, clientId);
        this.client.setCallback(this);
        this.client.connect(opts);
        this.client.subscribe(this.topicPattern, this.qos);

        LOGGER.info("MQTT command service connected to {} and subscribed to {}", url, topicPattern);
    }

    @Override
    public void close() throws Exception {
        try {
            if (client != null && client.isConnected()) client.disconnect();
        } catch (MqttException e) {
            LOGGER.warn("MQTT disconnect error: {}", e.getMessage());
        }
    }

    @Override
    public void connectionLost(Throwable cause) {
        LOGGER.warn("MQTT connection lost: {}", cause.getMessage());
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) {
        try {
            // topic: traccar/command/<IMEI>/<action>
            String[] parts = topic.split("/");
            if (parts.length < 4) return;
            String imei = parts[2];
            String action = parts[3].toLowerCase();

            int signal;
            if ("static1".equals(action)) signal = 1;
            else if ("static2".equals(action)) signal = 2;
            else return; // ignore other actions

            String payloadStr = new String(message.getPayload(), StandardCharsets.UTF_8).trim();
            boolean on = parseBoolean(payloadStr);

            int uniqueByte = uniq.incrementAndGet() & 0xFF;
            byte[] payload = IotmStaticSignal.buildWithDefaultExpiry(signal, on, uniqueByte);

            String outTopic = imei + "/OUTC";
            MqttMessage out = new MqttMessage(payload);
            out.setQos(Math.max(1, this.qos)); // ensure QoS >=1 for device control
            client.publish(outTopic, out);

            LOGGER.info("Published IoTM OUTC cmd: imei={} signal={} value={} bytes={}",
                    imei, signal, on ? 1 : 0, payload.length);

        } catch (Exception e) {
            LOGGER.warn("MQTT cmd handling error: {}", e.getMessage());
        }
    }

    private static boolean parseBoolean(String s) {
        String v = s.toLowerCase();
        return Arrays.asList("1", "on", "true", "yes").contains(v);
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        // no-op
    }
}