package com.knoldus;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class CustomDeserializer implements Deserializer {
    @Override public void close() {
    }
    @Override public void configure(Map arg0, boolean arg1) {
    }
    @Override
    public User deserialize(String arg0, byte[] arg1) {
        ObjectMapper mapper = new ObjectMapper();
        User userObject = null;
        try {
            userObject = mapper.readValue(arg1, User.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return userObject;
    }
}