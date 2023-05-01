package pt.uc.dei.weatherStations.streams;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.json.JSONException;
import org.json.JSONObject;

public class TheoreticalClass {

    /* Function that returns a given property specified on "par" of a json string "str". */
    public static String getParameter(String str, String par) throws JSONException{
        JSONObject strJson = new JSONObject(str);
        return strJson.getString(par);
    }

  

    /* 1. Count    temperature    readings    of    standard    weather    events    per    weather    station */
    public static void getTotalTemperaturesStdWS(KStream<String, String> lines, String resultTopic, int topic, String key_json, String value_json){
        
        KTable<String, Long> outlines = lines.groupByKey().count();
        outlines.mapValues((k,v) -> {
                                return ("" + v);
                                })
                .toStream()
                .peek((key, value) -> {
                    if(topic == 1)
                        System.out.println("#1| Weather Station: " + key + " No. temperature readings: " + value);
                    else
                        System.out.println("#5| Weather Station: " + key + " No. alerts: " + value);})
                .map((k, v) -> new KeyValue<>(key_json + "\"" + k + "\"" + "}}",value_json + "\"" + v + "\""+ "}}"))
                .to(resultTopic + "-"+topic, Produced.with(Serdes.String(), Serdes.String()));
    }

    /* 2. Count	temperature	readings	of	standard	weather	events	per	location */
    public static void getTotalTemperaturesStdLocation(KStream<String, String> lines, String resultTopic, String key_json, String value_json){
                KTable outlines = lines.map((k,v) -> {
                                                try {
                                                    return new KeyValue<>(getParameter(v, "location"), v);
                                                } catch (JSONException e) {
                                                    // TODO Auto-generated catch block
                                                    e.printStackTrace();
                                                }
                                                return null;
                                           })
                                        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))    
                                        .count();


                outlines.mapValues((k,v) -> {
                        return ("" + v);})
                        .toStream()
                        .peek((key, value) -> System.out.println("#2| Location: " + key + " No. temperature readings: " + value))
                        .map((k, v) -> new KeyValue<>(key_json + "\"" + k + "\"" + "}}",value_json + "\"" + v + "\""+ "}}"))
                        .to(resultTopic + "-2", Produced.with(Serdes.String(), Serdes.String()));
    }

    /* Returns the lowest value of temperature*/
    public static int getMinTemperature(int v1, int v2){
        if(v1 < v2)
            return v1;
        return v2;
    }
 
    /* Returns the highest value of temperature*/
    public static int getMaxTemperature(int v1, int v2){
        if(v1 > v2)
            return v1;
        return v2;
    }

    /* 3. Get	minimum	and	maximum	temperature	per	weather	station. */
    public static void getMinMaxPerWS(KStream<String, String> lines, String resultTopic, String key_json, String value_json){
        lines.map((k,v) -> {
            try {
                return new KeyValue<>(k, Integer.valueOf(getParameter(v, "temperature")));
            } catch (NumberFormatException e) {
                e.printStackTrace();
            } catch (JSONException e) {
                e.printStackTrace();
            }
            return null;
            })
            .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))  
            .reduce((v1, v2) ->  getMinTemperature(v1,v2))
            .toStream()
            .peek((k,v) -> System.out.println("#3| Weather station: " + k + "\t\t\tMin temperature: " + v))
            .map((k, v) -> new KeyValue<>(key_json + "\"" + k + "-MIN" + "\"" + "}}",value_json + "\"" + v + "\""+ "}}"))
            .to(resultTopic + "-3", Produced.with(Serdes.String(), Serdes.String()));

        lines.map((k,v) -> {
                try {
                    return new KeyValue<>(k, Integer.valueOf(getParameter(v, "temperature")));
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                } catch (JSONException e) {
                    e.printStackTrace();
                }
                return null;
            })
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))  
                .reduce((v1, v2) -> {
                    v2 = getMaxTemperature(v1,v2);
                    return v2;})
                .toStream()
                .peek((k,v) -> System.out.println("#3| Weather station: " + k + "\t\t\tMax temperature: " + v))
                .map((k, v) -> new KeyValue<>(key_json + "\"" + k  + "-MAX" + "\"" + "}}",value_json + "\"" + v + "\""+ "}}"))
                .to(resultTopic + "-3", Produced.with(Serdes.String(), Serdes.String()));

    }

    
    /* 4. Get	minimum	and	maximum	temperature	per	weather	location.  */
    public static void getMinMaxPerLocation(KStream<String, String> lines, String resultTopic, String key_json, String value_json){
        lines
            .map((k,v) -> {
                        try {
                            return new KeyValue<>(getParameter(v, "location"), Integer.valueOf(getParameter(v, "temperature")));
                        } catch (JSONException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                        return null;
                    })
            .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
            .reduce((v1, v2) -> getMinTemperature(v1,v2))
            .toStream()
            .peek((k,v) -> System.out.println("#4| Location: " + k + "\t\t\tMin temperature: " + v))
            .map((k, v) -> new KeyValue<>(key_json + "\"" + k + "-MIN" + "\"" + "}}",value_json + "\"" + v + "\""+ "}}"))
            .to(resultTopic + "-4", Produced.with(Serdes.String(), Serdes.String()));

            lines
            .map((k,v) -> {
                        try {
                            return new KeyValue<>(getParameter(v, "location"), Integer.valueOf(getParameter(v, "temperature")));
                        } catch (JSONException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                        return null;
                    })
            .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
            .reduce((v1, v2) -> getMaxTemperature(v1,v2))
            .toStream()
            .peek((k,v) -> System.out.println("#4| Location: " + k + "\t\t\tMax temperature: " + v))
            .map((k, v) -> new KeyValue<>(key_json + "\"" + k + "-MAX" + "\"" + "}}",value_json + "\"" + v + "\""+ "}}"))
            .to(resultTopic + "-4", Produced.with(Serdes.String(), Serdes.String()));
            
    }

    /* 6. Count	the	total	alerts	per	type */
    public static void getTotalAlertsType(KStream<String, String> lines, String resultTopic, String key_json, String value_json){
        lines.map((k,v) -> {
                            try {
                                System.out.println(v);
                                return new KeyValue<>(getParameter(v, "type"), v);
                            } catch (JSONException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }
                            return null;
                        })
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))    
            .count()
            .toStream()
            .peek((key, value) -> System.out.println("#6| Type: " + key + " No. alerts: " + value))
            .map((k, v) -> new KeyValue<>(key_json + "\"" + k + "\"" + "}}",value_json + "\"" + v + "\""+ "}}"))
            .to(resultTopic + "-6");
    }



    /* 7. Get	minimum	temperature of	weather	stations	with	red	alert	events  */
    public static void getMinTempRedAlert(KStream<String, String> linesAlerts, KStream<String, String> linesStd, String resultTopic, String key_json, String value_json){        
        KStream<String, String> redAlerts  = linesAlerts
        .filter((k,v) -> {
            try {
                return "red".equals(getParameter(v,"type"));
            } catch (JSONException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return false;
        });
        

            KStream<String, String> temperatures = linesStd 
                .map((k,v) -> {
                try {
                    return new KeyValue<>(k, getParameter(v, "temperature"));
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                } catch (JSONException e) {
                    e.printStackTrace();
                }
                return null;
                });

        ValueJoiner<String,String,String> valueJoiner = (leftValue, rightValue) -> {
                                                                                    JSONObject joinedValue;
                                                                                    try {
                                                                                        joinedValue = new JSONObject(leftValue);
                                                                                        joinedValue.put("temperature", Integer.valueOf(rightValue));
                                                                                        return joinedValue.toString();
                                                                                    } catch (JSONException e) {
                                                                                        // TODO Auto-generated catch block
                                                                                        e.printStackTrace();
                                                                                    }
                                                                                    return null;
                                                                                    };

        redAlerts.join(temperatures, valueJoiner, JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1)))
                    .map((k,v) -> {
                        try {
                            return new KeyValue<>("1", Integer.valueOf(getParameter(v, "temperature")));
                        } catch (NumberFormatException e) {
                            e.printStackTrace();
                        } catch (JSONException e) {
                            e.printStackTrace();
                        }
                        return null;
                    })      
                    .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))                                                               
                    .reduce((v1, v2) -> getMinTemperature(v1,v2))
                    .toStream()
                    .peek((k,v) -> System.out.println("#7| K: " + k + " Min temp: " + v))
                    .map((k, v) -> new KeyValue<>(key_json + "\"" + k + "\"" + "}}",value_json + "\"" + v + "\""+ "}}"))
                    .to(resultTopic + "-7", Produced.with(Serdes.String(), Serdes.String()));
    }
    
    /* 8. Get maximum temperature of each location of alert events for the last hour */
    public static void getMaxTempAlertHour(KStream<String, String> linesAlerts, KStream<String, String> linesStd, String resultTopic, String key_json, String value_json){
        KStream<String, String> AlertLocations  = linesAlerts 
        .map((k,v) -> {
                try {
                    System.out.println("ValueTemp: " + v);
                    return new KeyValue<>(getParameter(v, "location"),v);
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                } catch (JSONException e) {
                    e.printStackTrace();
                }
                return null;
                });

        KStream<String, String> StdLocations  = linesStd /*  => left  {"ws1":{"type":"red"}}; {"ws2":{"type":"red"}};
                                                                      {"ws1":{"type":"red"}}; {"ws3":{"type":"red"}}; */
        .map((k,v) -> {
                try {
                    System.out.println("ValueTemp: " + v);
                    return new KeyValue<>(getParameter(v, "location"), getParameter(v, "temperature"));
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                } catch (JSONException e) {
                    e.printStackTrace();
                }
                return null;
                });
        
        ValueJoiner<String,String,String> valueJoiner = (leftValue, rightValue) -> {
                                                                                    JSONObject joinedValue;
                                                                                    try {
                                                                                        joinedValue = new JSONObject(leftValue);
                                                                                        joinedValue.put("temperature", rightValue);
                                                                                        return joinedValue.toString();
                                                                                    } catch (JSONException e) {
                                                                                        // TODO Auto-generated catch block
                                                                                        e.printStackTrace();
                                                                                    }
                                                                                    return null;
                                                                                    };


        AlertLocations.join(StdLocations, valueJoiner, JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(60)))
                    .map((k,v) -> {
                        try {
                            System.out.println("K: " + k + " v: " + v);
                            return new KeyValue<>(getParameter(v, "location"), Integer.valueOf(getParameter(v, "temperature")));
                        } catch (NumberFormatException e) {
                            e.printStackTrace();
                        } catch (JSONException e) {
                            e.printStackTrace();
                        }
                        return null;
                    })      
                    .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))                                                               
                    .reduce((v1, v2) -> getMaxTemperature(v1,v2))
                    .toStream()
                    .peek((k,v) -> System.out.println("#8| K: " + k + " Max temp: " + v))
                    .map((k, v) -> new KeyValue<>(key_json + "\"" + k + "\"" + "}}",value_json + "\"" + v + "\""+ "}}"))
                    .to(resultTopic + "-8", Produced.with(Serdes.String(), Serdes.String()));
        

    }
    
    /* 9. Get minimum temperature per weather station in red alert zones */
    public static void getMinTempRedZones(KStream<String, String> linesAlerts, KStream<String, String> linesStd, String resultTopic, String key_json, String value_json){        
        KStream<String, String> RedAlert  = linesAlerts 
         .filter((k,v) -> {
            try {
                return "red".equals(getParameter(v,"type"));
            } catch (JSONException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return false;
        });
        


        KStream<String, String> StdTemp = linesStd  
            .map((k,v) -> {
            try {
                System.out.println("ValueTemp: " + v);
                return new KeyValue<>(k, getParameter(v, "temperature"));
            } catch (NumberFormatException e) {
                e.printStackTrace();
            } catch (JSONException e) {
                e.printStackTrace();
            }
            return null;
            });
        
        
        ValueJoiner<String,String,String> valueJoiner = (leftValue, rightValue) -> {
                                                                                    JSONObject joinedValue;
                                                                                    try {
                                                                                        joinedValue = new JSONObject(leftValue);
                                                                                        joinedValue.put("temperature", rightValue);
                                                                                        return joinedValue.toString();
                                                                                    } catch (JSONException e) {
                                                                                        // TODO Auto-generated catch block
                                                                                        e.printStackTrace();
                                                                                    }
                                                                                    return null;
                                                                                    };

        RedAlert.join(StdTemp, valueJoiner, JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1)))
                    .map((k,v) -> {
                        try {
                            System.out.println("K: " + k + " v: " + v);
                            return new KeyValue<>(k, Integer.valueOf(getParameter(v, "temperature")));
                        } catch (NumberFormatException e) {
                            e.printStackTrace();
                        } catch (JSONException e) {
                            e.printStackTrace();
                        }
                        return null;
                    })      
                    .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))                                                               
                    .reduce((v1, v2) -> getMinTemperature(v1,v2))
                    .toStream()
                    .peek((k,v) -> System.out.println("#9| K: " + k + " Min temp: " + v))
                    .map((k, v) -> new KeyValue<>(key_json + "\"" + k + "\"" + "}}",value_json + "\"" + v + "\""+ "}}"))
                    .to(resultTopic + "-9", Produced.with(Serdes.String(), Serdes.String()));
    }
    
    /* 10. Get the average temperature per weather station */
    public static void getAvgTempPerWS( KStream<String, String> lines, String resultTopic, String key_json, String value_json){

        lines.map((k,v) -> {
            try {
                return new KeyValue<>(k, Integer.valueOf(getParameter(v, "temperature")));
            } catch (NumberFormatException e) {
                e.printStackTrace();
            } catch (JSONException e) {
                e.printStackTrace();
            }
            return null;
            })
            .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
            .aggregate(() -> new int[]{0, 0}, (aggKey, newValue, aggValue) -> {
                aggValue[0] += 1;
                aggValue[1] += newValue;

                return aggValue;
            }, Materialized.with(Serdes.String(), new IntArraySerde()))
            .mapValues(v -> v[0] != 0 ? "" + (1.0 * v[1]) / v[0] : "div by 0")
            .toStream()
            .peek((k, v) -> System.out.println("#10| Weather Station: " + k + " Average temperature: " + v))
            .map((k, v) -> new KeyValue<>(key_json + "\"" + k + "\"" + "}}",value_json + "\"" + v + "\""+ "}}"))
            .to(resultTopic + "-10", Produced.with(Serdes.String(), Serdes.String()));
    }
    
    /* 11. Get the average temperature of weather stations with red alert events for the last hour */
    public static void getAvgTempRedZonesLastHour(KStream<String, String> linesAlerts, KStream<String, String> linesStd, String resultTopic, String key_json, String value_json){        
        KStream<String, String> redAlerts  = linesAlerts
        .filter((k,v) -> {
            try {
                return "red".equals(getParameter(v,"type"));
            } catch (JSONException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return false;
        });
        

        KStream<String, String> temperatures = linesStd
            .map((k,v) -> {
            try {
                return new KeyValue<>(k, getParameter(v, "temperature"));
            } catch (NumberFormatException e) {
                e.printStackTrace();
            } catch (JSONException e) {
                e.printStackTrace();
            }
            return null;
            });



        ValueJoiner<String,String,String> valueJoiner = (leftValue, rightValue) -> {
                                                                                    JSONObject joinedValue;
                                                                                    try {
                                                                                        joinedValue = new JSONObject(leftValue);
                                                                                        joinedValue.put("temperature", Integer.valueOf(rightValue));
                                                                                        return joinedValue.toString();
                                                                                    } catch (JSONException e) {
                                                                                        // TODO Auto-generated catch block
                                                                                        e.printStackTrace();
                                                                                    }
                                                                                    return null;
                                                                                    };

        redAlerts.join(temperatures, valueJoiner, JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(60)))
        .map((k,v) -> {
            try {
                return new KeyValue<>("1", Integer.valueOf(getParameter(v, "temperature")));
            } catch (NumberFormatException e) {
                e.printStackTrace();
            } catch (JSONException e) {
                e.printStackTrace();
            }
            return null;
            })
            .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
            .aggregate(() -> new int[]{0, 0}, (aggKey, newValue, aggValue) -> {
                aggValue[0] += 1;
                aggValue[1] += newValue;

                return aggValue;
            }, Materialized.with(Serdes.String(), new IntArraySerde()))
            .mapValues(v -> v[0] != 0 ? "" + (1.0 * v[1]) / v[0] : "div by 0")
            .toStream()
            .peek((k, v) -> System.out.println("#11| Weather Station: " + k + " Average temperature: " + v))
            .map((k, v) -> new KeyValue<>(key_json + "\"" + k + "\"" + "}}",value_json + "\"" + v + "\""+ "}}"))
            .to(resultTopic + "-11", Produced.with(Serdes.String(), Serdes.String()));                                                               
    }


    public static void main(String[] args) throws InterruptedException, IOException, JSONException {         

        String topicStandard = "StandardWeather1";
        String topicAlert = "WeatherAlert1";
        String outtopicname = "results";

        java.util.Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "theoretical-class13");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9093");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        
        StreamsBuilder builder = new StreamsBuilder(); 
        KStream<String, String> linesStd = builder.stream(topicStandard);
        KStream<String, String> linesAlert = builder.stream(topicAlert);


        String key_json = "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"key\"}],\"optional\":false},\"payload\":{\"key\":";
        String value_json = "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"value\"}],\"optional\":false},\"payload\":{\"value\":";
        

       // lines.peek((k,v)-> System.out.println(v));
       getTotalTemperaturesStdWS(linesStd, outtopicname, 1, key_json, value_json);
       getTotalTemperaturesStdLocation(linesStd, outtopicname, key_json, value_json);
       getMinMaxPerWS(linesStd, outtopicname, key_json, value_json);
       getMinMaxPerLocation(linesStd, outtopicname, key_json, value_json);
       getTotalTemperaturesStdWS(linesAlert, outtopicname,5, key_json, value_json);
       getTotalAlertsType(linesAlert, outtopicname, key_json, value_json);
       getMinTempRedAlert(linesAlert, linesStd, outtopicname, key_json, value_json);
       getMaxTempAlertHour(linesAlert, linesStd, outtopicname, key_json, value_json);
       getMinTempRedZones(linesAlert, linesStd, outtopicname, key_json, value_json);
       getAvgTempRedZonesLastHour(linesAlert, linesStd, outtopicname, key_json, value_json);
       getAvgTempPerWS(linesStd, outtopicname, key_json, value_json);

        
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
         streams.start();
    } 
}