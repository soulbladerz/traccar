package org.traccar.mqtt;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.traccar.config.Config;
import org.traccar.LifecycleObject;
import org.traccar.iotm.IotmStaticSignal;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

@Singleton
public final class MqttCommandService implements LifecycleObject, MqttCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(MqttCommandService.class);

    private final Config config;

    private MqttClient client;
    private String topicPattern;
    private int qos;
    private boolean enabled;

    private final AtomicInteger uniq = new AtomicInteger(0);

    @Inject
    public MqttCommandService(Config config) {
        this.config = config;
    }

    @Override
    public void start() throws Exception {
        // Config: read string keys and parse (ConfigKey-typed getters don't work for custom keys)
        enabled = Boolean.parseBoolean(config.getString("mqtt.cmd.enable", "false"));
        if (!enabled) {
            LOGGER.info("MQTT command service disabled (mqtt.cmd.enable=false)");
            return;
        }

        String url = config.getString("mqtt.cmd.url", "tcp://localhost:1883");
        String clientId = config.getString("mqtt.cmd.clientId", "traccar-cmd-" + System.nanoTime());
        topicPattern = config.getString("mqtt.cmd.topic", "traccar/command/+/+");
        qos = Integer.parseInt(config.getString("mqtt.cmd.qos", "1"));

        String user = config.getString("mqtt.cmd.username", null);
        String pass = config.getString("mqtt.cmd.password", null);

        MqttConnectOptions opts = new MqttConnectOptions();
        if (user != null && !user.isEmpty()) {
            opts.setUserName(user);
        }
        if (pass != null) {
            opts.setPassword(pass.toCharArray());
        }
        opts.setAutomaticReconnect(true);
        opts.setCleanSession(true);

        client = new MqttClient(url, clientId);
        client.setCallback(this);
        client.connect(opts);
        client.subscribe(topicPattern, qos);

        LOGGER.info("MQTT command service connected to {} and subscribed to {}", url, topicPattern);
    }

    @Override
    public void stop() throws Exception {
        if (!enabled) return;
        try {
            if (client != null && client.isConnected()) {
                client.disconnect();
            }
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

            final int signal;
            if ("static1".equals(action)) {
                signal = 1;
            } else if ("static2".equals(action)) {
                signal = 2;
            } else {
                return; // ignore other actions
            }

            String payloadStr = new String(message.getPayload(), StandardCharsets.UTF_8).trim();
            boolean on = Arrays.asList("1", "on", "true", "yes").contains(payloadStr.toLowerCase());

            int uniqueByte = uniq.incrementAndGet() & 0xFF;
            byte[] payload = IotmStaticSignal.buildWithDefaultExpiry(signal, on, uniqueByte);

            String outTopic = imei + "/OUTC";
            MqttMessage out = new MqttMessage(payload);
            out.setQos(Math.max(1, qos)); // ensure QoS >= 1
            client.publish(outTopic, out);

            LOGGER.info("Published IoTM OUTC cmd: imei={} signal={} value={} bytes={}",
                    imei, signal, on ? 1 : 0, payload.length);

        } catch (Exception e) {
            LOGGER.warn("MQTT cmd handling error: {}", e.getMessage());
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        // no-op
    }

    /** Called from CommandsManager to route UI commands into MQTT OUTC */
    public void publishStatic(String uniqueId, int signal, boolean on) throws Exception {
        if (!enabled) throw new IllegalStateException("MQTT command service not enabled");
        if (client == null || !client.isConnected()) throw new IllegalStateException("MQTT client not connected");

        int uniqueByte = uniq.incrementAndGet() & 0xFF;
        byte[] payload = IotmStaticSignal.buildWithDefaultExpiry(signal, on, uniqueByte);

        MqttMessage out = new MqttMessage(payload);
        out.setQos(Math.max(1, qos)); // enforce QoS >= 1
        client.publish(uniqueId + "/OUTC", out);

        LOGGER.info("OUTC static publish: imei={} signal={} value={}", uniqueId, signal, on ? 1 : 0);
    }
}
