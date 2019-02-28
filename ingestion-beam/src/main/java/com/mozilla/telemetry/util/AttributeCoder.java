/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.TreeMap;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.TypeDescriptor;

public class AttributeCoder extends CustomCoder<TreeMap<String, String>> {

  private static final Coder<Map<String, String>> coder = MapCoder.of(StringUtf8Coder.of(),
      StringUtf8Coder.of());

  public static Coder<TreeMap<String, String>> of(TypeDescriptor<TreeMap<String, String>> ignored) {
    return of();
  }

  public static AttributeCoder of() {
    return new AttributeCoder();
  }

  @Override
  public void encode(TreeMap<String, String> value, OutputStream outStream) throws IOException {
    encode(value, outStream, Context.NESTED);
  }

  public void encode(TreeMap<String, String> value, OutputStream outStream, Context context)
      throws IOException {
    coder.encode(value, outStream, context);
  }

  @Override
  public TreeMap<String, String> decode(InputStream inStream) throws IOException {
    return decode(inStream, Context.NESTED);
  }

  @Override
  public TreeMap<String, String> decode(InputStream inStream, Context context) throws IOException {
    Map<String, String> attributes = coder.decode(inStream, context);
    return new TreeMap<>(attributes);
  }

  @Override
  public void verifyDeterministic() {
  }
}
