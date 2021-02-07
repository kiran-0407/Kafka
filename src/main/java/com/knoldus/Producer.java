package com.knoldus;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.knoldus.User;

import java.util.Properties;
import java.util.Random;

public class Producer {
    public static void main(String[] args){

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "com.knoldus.CustomSerializer");
        
        KafkaProducer<String,User> kafkaProducer = new KafkaProducer<>(properties);
        try{
            Random randomNumber = new Random();
            for(int i = 1; i <= 10; i++){
                User ob=new User(i,"Kiran",randomNumber.nextInt(30),"MCA");
                kafkaProducer.send(new ProducerRecord<String,User>("user", Integer.toString(i),ob ));
            }

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            kafkaProducer.close();
        }
    }
}