package org.rabix.bindings.model;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

public class ContentDeserializer extends JsonDeserializer<String> {
public ContentDeserializer() {
  // TODO Auto-generated constructor stub
}
    @Override
    public String deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
      return new String(p.getBinaryValue());
    }
  }