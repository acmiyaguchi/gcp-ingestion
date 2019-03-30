/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.avro;

import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.AvroIO.RecordFormatter;
import org.apache.beam.sdk.values.KV;

/**
 * Encodes an incoming message into a generic avro record.
*/
public class IdentityRecordFormatter
    implements RecordFormatter<KV<Map<String, String>, GenericRecord>> {

  @Override
  public GenericRecord formatRecord(KV<Map<String, String>, GenericRecord> element, Schema schema) {
    return element.getValue();
  }
}
