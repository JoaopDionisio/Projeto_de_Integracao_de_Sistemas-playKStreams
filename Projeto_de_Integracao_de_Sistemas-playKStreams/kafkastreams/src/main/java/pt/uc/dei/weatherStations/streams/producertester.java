package pt.uc.dei.weatherStations.streams;


import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONException;
import org.json.JSONObject;

public class producertester {
    static String [] locations = {"Celas","Solum","S.Jose","Se Velha","Se Nova","Portela","S.Martinho","Baixa"}; 
    static String [] type = {"red","green"};
    static Random rand = new Random();

    
    public static ProducerRecord<String, String> generateWeatherEvent(String topicName, String station) throws JSONException {
        JSONObject jsonRecord = new JSONObject();

        jsonRecord.put("location", locations[rand.nextInt(8)]);
        jsonRecord.put("Weather Station", station);
        if(topicName == "StandardWeather1")
            jsonRecord.put("temperature",  rand.nextInt(-5,40));
        else
            jsonRecord.put("type", type[rand.nextInt(2)]);
        return new ProducerRecord<>(topicName, station, jsonRecord.toString());
    }

   

    public static void main(String[] args) throws Exception{ //Assign topicName to string variable

        String [] topicNames = {"results-1"};

        // create instance for properties to access producer configs
        Properties props = new Properties(); //Assign localhost id
        props.put("bootstrap.servers", "broker1:9092"); // adicionar mais um broker
        //Set acknowledgements for producer requests. props.put("acks", "all");
        //If the request fails, the producer can automatically retry,
        props.put("retries", 0);
        //Specify buffer size in config
        props.put("batch.size", 16384);
        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);
        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        String template1 = "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"WS1\"}]},\"payload\":{\"WS1\":\"lalala\"}}";
        JSONObject jsn = new JSONObject(template1);
        String str = jsn.toString();

        for(String topicName : topicNames){
            Producer<String, String> producer = new KafkaProducer<>(props);
                producer.send(new ProducerRecord<>("results-1",str));
                System.out.println("Sending message to topic " + topicName);
            
            producer.close();

        }
    }
}