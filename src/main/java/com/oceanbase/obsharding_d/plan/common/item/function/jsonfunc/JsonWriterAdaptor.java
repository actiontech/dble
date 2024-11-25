/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.common.item.function.jsonfunc;

import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.io.Writer;

/**
 * @author dcy
 * Create Date: 2022-09-28
 */
final class JsonWriterAdaptor extends JsonWriter {
    CustomJsonWriter jsonWriter;

    JsonWriterAdaptor(Writer out, CustomJsonWriter jsonWriter) {
        super(out);
        this.jsonWriter = jsonWriter;
    }

    @Override
    public boolean isLenient() {
        return jsonWriter.isLenient();
    }

    @Override
    public JsonWriter beginArray() throws IOException {
        jsonWriter.beginArray();
        return this;
    }

    @Override
    public JsonWriter endArray() throws IOException {
        jsonWriter.endArray();
        return this;
    }

    @Override
    public JsonWriter beginObject() throws IOException {
        jsonWriter.beginObject();
        return this;
    }

    @Override
    public JsonWriter endObject() throws IOException {
        jsonWriter.endObject();
        return this;
    }

    @Override
    public JsonWriter name(String name) throws IOException {
        jsonWriter.name(name);
        return this;
    }


    @Override
    public JsonWriter jsonValue(String value) throws IOException {
        jsonWriter.jsonValue(value);
        return this;
    }

    @Override
    public JsonWriter nullValue() throws IOException {
        jsonWriter.nullValue();
        return this;
    }

    @Override
    public JsonWriter value(String value) throws IOException {
        jsonWriter.value(value);
        return this;
    }

    @Override
    public JsonWriter value(boolean value) throws IOException {
        jsonWriter.value(value);
        return this;
    }

    @Override
    public JsonWriter value(Boolean value) throws IOException {
        jsonWriter.value(value);
        return this;
    }

    @Override
    public JsonWriter value(double value) throws IOException {
        jsonWriter.value(value);
        return this;
    }

    @Override
    public JsonWriter value(long value) throws IOException {
        jsonWriter.value(value);
        return this;
    }

    @Override
    public JsonWriter value(Number value) throws IOException {
        jsonWriter.value(value);
        return this;
    }

    @Override
    public void flush() throws IOException {
        jsonWriter.flush();
    }

    @Override
    public void close() throws IOException {
        jsonWriter.close();
    }
}
