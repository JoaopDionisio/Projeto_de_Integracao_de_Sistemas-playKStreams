package pt.uc.dei.weatherStations;

/* Systems Integration - Project 3 
 * Authors:
 *  João Dionísio, 2019217030, joaodionisio@student.dei.uc.pt 
 *  Sofia Alves, 2019227240, sbalves@student.dei.uc.pt
*/

import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONException;
import org.json.JSONObject;

public class SimpleProducer {
    static String [] locations = {"Celas","Solum","S.Jose","Se Velha","Se Nova","Portela","S.Martinho","Baixa"}; 
    static String [] type = {"red","green"};
    static Random rand = new Random();

    /* Function to generate records that store the information about weather events. */
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

        String [] topicNames = {"StandardWeather1","WeatherAlert1"};

        // create instance for properties to access producer configs
        Properties props = new Properties(); //Assign localhost id
        props.put("bootstrap.servers", "broker1:9092,broker2:9093"); // adicionar mais um broker
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

        for(String topicName : topicNames){
            Producer<String, String> producer = new KafkaProducer<>(props);
            for(int i = 0; i < 30; i++) {
                producer.send(generateWeatherEvent(topicName, "WS1"));
                producer.send(generateWeatherEvent(topicName, "WS2"));
                producer.send(generateWeatherEvent(topicName, "WS3"));
                if (i % 10 == 0)
                    System.out.println("Sending message " + (i + 1) + " to topic " + topicName);
            }
            producer.close();

        }
    }
}