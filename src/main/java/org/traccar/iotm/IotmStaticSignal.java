package org.traccar.iotm;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;

/**
 * Builds Xirgo "IoTM" Structure v2 Output Control payloads
 * for STATIC SIGNAL 1/2 control (latched ON/OFF).
 *
 * Output IDs (IoTM spec):
 *   0x08 = STATIC SIGNAL1 CONTROL
 *   0x09 = STATIC SIGNAL2 CONTROL
 *
 * Command length = 1 byte
 *   data = 0x01 -> set static signal = 1 (ON)
 *          any other value (0x00) -> set = 0 (OFF)
 *
 * The overall packet is:
 *   [0x02 structureVersion=2]
 *   [recordType=0x04][recordLenLE=2B]
 *   [expirationUTC_LE=4B]
 *   [outputId=1B]
 *   [uniqueId=1B]
 *   [cmdLen=1B=1]
 *   [data=1B]
 *   [checksum = sum of all bytes starting from structureVersion]
 */
public final class IotmStaticSignal {

    private IotmStaticSignal() {}

    /**
     * Build a Structure v2 + Output Control (type 0x04) payload to set STATIC SIGNAL 1/2.
     *
     * @param signal  1 or 2 -> STATIC SIGNAL1 or STATIC SIGNAL2 (maps to Output ID 0x08 or 0x09)
     * @param on      true -> data=0x01 (set 1); false -> data=0x00 (set 0)
     * @param uniqueIdByte  0..255, used to prevent duplicate command rejection on device side
     * @param expiryEpochUtc  Unix time (seconds) when the command expires
     * @return payload bytes to publish to "<IMEI>/OUTC" with QoS 1
     */
    public static byte[] buildStaticSignalPayload(int signal, boolean on, int uniqueIdByte, long expiryEpochUtc) {
        if (signal < 1 || signal > 2) {
            throw new IllegalArgumentException("signal must be 1 or 2");
        }
        int outputId = 0x07 + signal;        // 1->0x08, 2->0x09
        byte data = (byte) (on ? 0x01 : 0x00);
        if (uniqueIdByte < 0 || uniqueIdByte > 255) uniqueIdByte &= 0xFF;

        // ---- Build Output control record (type=0x04) ----
        ByteArrayOutputStream rec = new ByteArrayOutputStream();
        rec.write(0x04);                     // Record type
        rec.write(0x00);                     // Record length placeholder (LE)
        rec.write(0x00);

        // Expiration (UTC, little-endian 4 bytes)
        ByteBuffer bb = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        bb.putInt((int) expiryEpochUtc);
        byte[] exp = bb.array();
        rec.writeBytes(exp);

        rec.write(outputId);                 // Output ID (0x08 or 0x09)
        rec.write(uniqueIdByte & 0xFF);      // Unique ID
        rec.write(0x01);                     // Command length = 1
        rec.write(data);                     // Data: 0x01 = set 1, else 0

        // Patch record length (remaining bytes after the 2-byte length field)
        byte[] recBytes = rec.toByteArray();
        int remaining = recBytes.length - 3; // exclude type(1)+length(2)
        recBytes[1] = (byte) (remaining & 0xFF);
        recBytes[2] = (byte) ((remaining >>> 8) & 0xFF);

        // ---- Wrap with Structure v2 + checksum ----
        ByteArrayOutputStream packet = new ByteArrayOutputStream();
        packet.write(0x02);                  // Structure version = 2
        packet.writeBytes(recBytes);         // Output control record only

        byte checksum = 0;
        byte[] tmp = packet.toByteArray();
        for (byte b : tmp) checksum += b;    // sum of bytes starting at version
        packet.write(checksum);              // final checksum byte

        return packet.toByteArray();
    }

    /** Convenience with default expiry of now+60s. */
    public static byte[] buildWithDefaultExpiry(int signal, boolean on, int uniqueIdByte) {
        long expiry = Instant.now().getEpochSecond() + 60; // valid for 60s
        return buildStaticSignalPayload(signal, on, uniqueIdByte, expiry);
    }
}