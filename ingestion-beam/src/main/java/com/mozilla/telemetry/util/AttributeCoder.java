package com.mozilla.telemetry.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.TypeDescriptor;

public class AttributeCoder extends CustomCoder<SortedMap<String, String>> {
    private static final Coder<Map<String, String>> coder =
            MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

    public static Coder<SortedMap<String, String>> of(TypeDescriptor<SortedMap<String, String>> ignored) {
        return of();
    }

    public static AttributeCoder of() {
        return new AttributeCoder();
    }

    @Override
    public void encode(SortedMap<String, String> value, OutputStream outStream) throws IOException {
        encode(value, outStream, Context.NESTED);
    }

    public void encode(SortedMap<String, String> value, OutputStream outStream, Context context)
            throws IOException {
        coder.encode(value, outStream, context);
    }

    @Override
    public SortedMap<String, String> decode(InputStream inStream) throws IOException {
        return decode(inStream, Context.NESTED);
    }

    @Override
    public SortedMap<String, String> decode(InputStream inStream, Context context) throws IOException {
        Map<String, String> attributes = coder.decode(inStream, context);
        return new TreeMap<>(attributes);
    }
}
