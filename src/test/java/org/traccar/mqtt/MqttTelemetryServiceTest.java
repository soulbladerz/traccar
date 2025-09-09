package org.traccar.mqtt;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.traccar.config.Config;
import org.traccar.database.DeviceLookupService;
import org.traccar.handler.DatabaseHandler;
import org.traccar.handler.PostProcessHandler;
import org.traccar.handler.BasePositionHandler;
import org.traccar.model.Position;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class MqttTelemetryServiceTest {

    @Test
    public void testJsonMessage() throws Exception {
        var config = new Config();
        var field = Config.class.getDeclaredField("properties");
        field.setAccessible(true);
        ((java.util.Properties) field.get(config)).setProperty("mqtt.tlm.json", "true");

        var databaseHandler = mock(DatabaseHandler.class);
        doAnswer(invocation -> { ((BasePositionHandler.Callback) invocation.getArgument(1)).processed(false); return null; })
                .when(databaseHandler).handlePosition(any(Position.class), any());
        var postProcessHandler = mock(PostProcessHandler.class);
        doAnswer(invocation -> { ((BasePositionHandler.Callback) invocation.getArgument(1)).processed(false); return null; })
                .when(postProcessHandler).handlePosition(any(Position.class), any());
        var deviceLookupService = mock(DeviceLookupService.class);

        var service = new MqttTelemetryService(config, new ObjectMapper(),
                databaseHandler, postProcessHandler, deviceLookupService);

        String json = "{\"deviceId\":1,\"time\":\"2020-01-01T00:00:00Z\",\"latitude\":10,\"longitude\":20}";
        service.messageArrived("test", new MqttMessage(json.getBytes(StandardCharsets.UTF_8)));

        ArgumentCaptor<Position> captor = ArgumentCaptor.forClass(Position.class);
        verify(databaseHandler).handlePosition(captor.capture(), any());
        Position position = captor.getValue();
        assertEquals(1L, position.getDeviceId());
        assertEquals(10.0, position.getLatitude());
        assertEquals(20.0, position.getLongitude());
    }
}
