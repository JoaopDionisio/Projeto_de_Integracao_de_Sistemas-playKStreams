package pt.uc.dei.weatherStations;

/* Systems Integration - Project 3 
 * Authors:
 *  João Dionísio, 2019217030, joaodionisio@student.dei.uc.pt 
 *  Sofia Alves, 2019227240, sbalves@student.dei.uc.pt
*/

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONException;
import org.json.JSONObject;

public class SimpleConsumer {

    public static String getParameter(String str, String par) throws JSONException{
        JSONObject strJson = new JSONObject(str);
        return strJson.getString(par);
    }

    public static void ex1(ConsumerRecords<String, String> records) throws JSONException{
        System.out.println("1. Count    temperature    readings    of    standard    weather    events    per    weather    station\n");

        for (ConsumerRecord<String, String> record : records) {
            JSONObject strJson = new JSONObject(record.value());
            JSONObject payload = strJson.getJSONObject("payload");
            System.out.println("Weather Station: " + payload.getString("key") + " No. temperature readings: " + payload.getString("value"));

        }
    }

    public static void ex2(ConsumerRecords<String, String> records) throws JSONException{
        System.out.println("2. Count	temperature	readings	of	standard	weather	events	per	weather	location\n");
        
        for (ConsumerRecord<String, String> record : records) {
            JSONObject strJson = new JSONObject(record.value());
            JSONObject payload = strJson.getJSONObject("payload");
            System.out.println("Location: " + payload.getString("key") + " No. temperature readings: " + payload.getString("value"));

        }

    }
 
    public static void ex3(ConsumerRecords<String, String> records) throws JSONException{
        System.out.println("3. Get	minimum	and	maximum	temperature	per	weather	station.\n");

        for (ConsumerRecord<String, String> record : records) {
            JSONObject strJson = new JSONObject(record.value());
            JSONObject payload = strJson.getJSONObject("payload");
            
            if(payload.getString("key").contains("MAX"))
                System.out.println("Weather station: " + payload.getString("key") + "\t\t\tMax temperature: " +  payload.getString("value"));
            else if(payload.getString("key").contains("MIN"))
                System.out.println("Weather station: " + payload.getString("key") + "\t\t\tMin temperature: " +  payload.getString("value"));

        }
    }
   
    public static void ex4(ConsumerRecords<String, String> records) throws JSONException{
        System.out.println("4. Get	minimum	and	maximum	temperature	per	location.\n");

        for (ConsumerRecord<String, String> record : records) {
            JSONObject strJson = new JSONObject(record.value());
            JSONObject payload = strJson.getJSONObject("payload");
            
            if(payload.getString("key").contains("MAX"))
                System.out.println("Location: " + payload.getString("key") + "\t\t\tMax temperature: " +  payload.getString("value"));
            else if(payload.getString("key").contains("MIN"))
                System.out.println("Location: " + payload.getString("key") + "\t\t\tMin temperature: " +  payload.getString("value"));

        }
    }
   
    public static void ex5(ConsumerRecords<String, String> records) throws JSONException{
        System.out.println("5. Count	temperature	readings	of weather alerts	events	per	weather	station\n");

        for (ConsumerRecord<String, String> record : records) {
            JSONObject strJson = new JSONObject(record.value());
            JSONObject payload = strJson.getJSONObject("payload");
            System.out.println("Weather Station: " + payload.getString("key") + " No. alerts:  " + payload.getString("value"));

        }
    }

    public static void ex6(ConsumerRecords<String, String> records)throws JSONException{
        System.out.println("6. Count	the	total	alerts	per	type \n");
        
        for (ConsumerRecord<String, String> record : records) {
            JSONObject strJson = new JSONObject(record.value());
            JSONObject payload = strJson.getJSONObject("payload");
            System.out.println("Type: " + payload.getString("key") + " No. alerts:  " + payload.getString("value"));
        }
    }

    public static void ex7(ConsumerRecords<String, String> records)throws JSONException{
        System.out.println("7. Get	minimum	temperature of	weather	stations	with	red	alert	events \n");

        for (ConsumerRecord<String, String> record : records) {
            JSONObject strJson = new JSONObject(record.value());
            JSONObject payload = strJson.getJSONObject("payload");
            System.out.println("K: " + payload.getString("key") + " Min temp: " + payload.getString("value"));
        }
    }

    public static void ex8(ConsumerRecords<String, String> records)throws JSONException{
        System.out.println("8. Get maximum temperature of each location of alert events for the last hour \n");
        
        for (ConsumerRecord<String, String> record : records) {
            JSONObject strJson = new JSONObject(record.value());
            JSONObject payload = strJson.getJSONObject("payload");
            System.out.println("Location: " + payload.getString("key") + " Max temp: " + payload.getString("value"));
        }
    }

    public static void ex9(ConsumerRecords<String, String> records)throws JSONException{
        System.out.println("9. Get minimum temperature per weather station in red alert zones \n");

        for (ConsumerRecord<String, String> record : records) {
            JSONObject strJson = new JSONObject(record.value());
            JSONObject payload = strJson.getJSONObject("payload");
            System.out.println("Weather Station: " + payload.getString("key") + " Min temp: " + payload.getString("value"));
        }
    }

    public static void ex10(ConsumerRecords<String, String> records)throws JSONException{
        System.out.println("10. Get the average temperature per weather station.");

        for (ConsumerRecord<String, String> record : records) {
            JSONObject strJson = new JSONObject(record.value());
            JSONObject payload = strJson.getJSONObject("payload");
            System.out.println("Weather Station: " + payload.getString("key") + " Average temperature: " + payload.getString("value"));
        }
    }

    public static void ex11(ConsumerRecords<String, String> records)throws JSONException{
        System.out.println("11. Get the average temperature of weather stations with red alert events for the last hour \n");

        for (ConsumerRecord<String, String> record : records) {
            JSONObject strJson = new JSONObject(record.value());
            JSONObject payload = strJson.getJSONObject("payload");
            System.out.println("Average temperature: " + payload.getString("value"));
        }
    }



    

    public static void printTables(ConsumerRecords<String, String> records, int i) throws JSONException{

        switch(i) {
            case 1:
                ex1(records);
                break;
            case 2:
                ex2(records);
                break;
            case 3:
                ex3(records);
                break;
            case 4:
                ex4(records);
                break;
            case 5:
                ex5(records);
                break;
            case 6:
                ex6(records);
                break;
            case 7:
                ex7(records);
                break;
            case 8:
                ex8(records);
                break;
            case 9:
                ex9(records);
                break;
            case 10:
                ex10(records);
                break;
            case 11:
                ex11(records);
                break;
        }
        
    }


    public static void main(String[] args) throws Exception{
        //Assign topicName to string variable
        String topicName = "DBinfoex";
        // create instance for properties to access producer configs
        Properties props = new Properties();
        //Assign localhost id
        props.put("bootstrap.servers", "broker1:9092"); //Set acknowledgements for producer requests. props.put("acks", "all");
        //If the request fails, the producer can automatically retry,
        props.put("retries", 0);
        //Specify buffer size in config
        props.put("batch.size", 16384);
        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);
        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        
        int i = 11;
        
        Consumer<String, String> consumer = new KafkaConsumer<>(props); consumer.subscribe(Collections.singletonList(topicName + i));
        try {
            Duration d = Duration.ofSeconds(1000000);
            ConsumerRecords<String, String> records = consumer.poll(d);
            printTables(records, i);  
            }
        finally {
            consumer.close();
        
        }

        
    } 
}