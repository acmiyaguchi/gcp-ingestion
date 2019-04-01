/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

/**
 * Decodes an incoming PubsubMessage that contains an avro encoded payload that
 * is registered in the schema store.
 */
public class GenericRecordBinaryEncoder {

  public byte[] encodeRecord(GenericRecord record, Schema schema) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
      Encoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
      writer.write(record, encoder);
      return baos.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
