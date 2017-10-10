package org.rabix.bindings.model;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public class ContentSerializer extends JsonSerializer<String> {
  public ContentSerializer() {
    // TODO Auto-generated constructor stub
  }
  @Override
  public void serialize(String value, JsonGenerator gen, SerializerProvider serializers) throws IOException, JsonProcessingException {
    gen.writeBinary(value.getBytes());
  }
}