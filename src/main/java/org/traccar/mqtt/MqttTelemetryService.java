package org.traccar.mqtt;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
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
import org.traccar.LifecycleObject;
import org.traccar.config.Config;
import org.traccar.database.DeviceLookupService;
import org.traccar.handler.DatabaseHandler;
import org.traccar.handler.PostProcessHandler;
import org.traccar.model.Device;
import org.traccar.model.Position;
import org.traccar.protocol.IotmProtocol;
import org.traccar.protocol.IotmProtocolDecoder;
import org.traccar.session.DeviceSession;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Date;

@Singleton
public class MqttTelemetryService implements LifecycleObject, MqttCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(MqttTelemetryService.class);

    private final Config config;
    private final ObjectMapper objectMapper;
    private final DatabaseHandler databaseHandler;
    private final PostProcessHandler postProcessHandler;
    private final DeviceLookupService deviceLookupService;
    private IotmProtocol protocol;
    private IotmProtocolDecoder decoder;

    private MqttClient client;
    private String topicPattern;
    private int qos;
    private boolean enabled;
    private boolean jsonEnabled;

    @Inject
    public MqttTelemetryService(
            Config config, ObjectMapper objectMapper,
            DatabaseHandler databaseHandler, PostProcessHandler postProcessHandler,
            DeviceLookupService deviceLookupService) {
        this.config = config;
        this.objectMapper = objectMapper;
        this.databaseHandler = databaseHandler;
        this.postProcessHandler = postProcessHandler;
        this.deviceLookupService = deviceLookupService;
        jsonEnabled = Boolean.parseBoolean(config.getString("mqtt.tlm.json", "false"));
    }

    @Override
    public void start() throws Exception {
        enabled = Boolean.parseBoolean(config.getString("mqtt.tlm.enable", "false"));
        if (!enabled) {
            LOGGER.info("MQTT telemetry service disabled (mqtt.tlm.enable=false)");
            return;
        }

        String url = config.getString("mqtt.tlm.url", "tcp://localhost:1883");
        String clientId = config.getString("mqtt.tlm.clientId", "traccar-tlm-" + System.nanoTime());
        topicPattern = config.getString("mqtt.tlm.topic", "+/+");
        qos = Integer.parseInt(config.getString("mqtt.tlm.qos", "1"));

        String user = config.getString("mqtt.tlm.username", null);
        String pass = config.getString("mqtt.tlm.password", null);

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

        LOGGER.info("MQTT telemetry service connected to {} and subscribed to {}", url, topicPattern);
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
        } finally {
            if (client != null) {
                try {
                    client.close();
                } catch (MqttException e) {
                    LOGGER.warn("MQTT close error: {}", e.getMessage());
                }
            }
        }
    }

    @Override
    public void connectionLost(Throwable cause) {
        LOGGER.warn("MQTT connection lost: {}", cause.getMessage());
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) {
        try {
            String payloadStr = new String(message.getPayload(), StandardCharsets.UTF_8).trim();
            if (jsonEnabled && payloadStr.startsWith("{")) {
                JsonNode node = objectMapper.readTree(payloadStr);
                Position position = new Position("mqtt");
                if (node.has("deviceId")) {
                    position.setDeviceId(node.get("deviceId").asLong());
                }
                if (node.has("time")) {
                    position.setTime(Date.from(Instant.parse(node.get("time").asText())));
                }
                if (node.has("latitude")) {
                    position.setLatitude(node.get("latitude").asDouble());
                }
                if (node.has("longitude")) {
                    position.setLongitude(node.get("longitude").asDouble());
                }
                position.setValid(!node.has("valid") || node.get("valid").asBoolean());
                handlePosition(position);
            } else {
                String[] parts = topic.split("/");
                if (parts.length > 0) {
                    if (protocol == null || decoder == null) {
                        protocol = new IotmProtocol(config);
                        decoder = new IotmProtocolDecoder(protocol);
                    }
                    String uniqueId = parts[0];
                    Device device = deviceLookupService.lookup(new String[]{uniqueId});
                    if (device != null) {
                        DeviceSession deviceSession = new DeviceSession(
                                device.getId(), device.getUniqueId(), device.getModel(),
                                protocol, null, null);
                        MqttPublishMessage mqttMessage = MqttMessageBuilders.publish()
                                .topicName(topic)
                                .qos(MqttQoS.valueOf(message.getQos()))
                                .payload(Unpooled.wrappedBuffer(message.getPayload()))
                                .build();
                        Object result;
                        try {
                            var method = IotmProtocolDecoder.class.getDeclaredMethod(
                                    "decode", DeviceSession.class, MqttPublishMessage.class);
                            method.setAccessible(true);
                            result = method.invoke(decoder, deviceSession, mqttMessage);
                        } catch (ReflectiveOperationException e) {
                            throw new RuntimeException(e);
                        }
                        if (result instanceof Position position) {
                            handlePosition(position);
                        } else if (result instanceof Iterable<?> list) {
                            for (Object obj : list) {
                                if (obj instanceof Position position) {
                                    handlePosition(position);
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.warn("MQTT telemetry handling error: {}", e.getMessage());
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        // no-op
    }

    private void handlePosition(Position position) {
        databaseHandler.handlePosition(position, r ->
                postProcessHandler.handlePosition(position, i -> {}));
    }
}

