package com.basu.assignment;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

public class App {
    public static Map<String, Integer> affectedNodes;
    public static Map<String, Integer> alarmEventTimes;
    public static Map<String, Integer> alarmTypes;


    public static void main(final String[] args) throws Exception {
        affectedNodes = new HashMap<String, Integer>();
        alarmTypes = new HashMap<String, Integer>();
        alarmEventTimes = new HashMap<String, Integer>();
        //given
        String inputTopic = "test";
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streamer");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //when
        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> textLines = builder.stream(inputTopic);
        Pattern pattern = Pattern.compile("metadata\":", Pattern.UNICODE_CHARACTER_CLASS);

        KTable<String, Long> wordCounts = textLines
                .flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
                .groupBy((key, word) -> word)
                .count();
        App app = new App();
        wordCounts.foreach((word, count) -> app.populateHistogramData(word));
        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);

        streams.start();
        //then
        Thread.sleep(2000);
        streams.close();
          //output after stream closed
        System.out.println("Histogram about the most frequent alarms");
        app.printHistogram(alarmTypes);
        System.out.println("Histogram about the most frequent nodes");
        app.printHistogram(affectedNodes);
        System.out.println("Timeline about the ERA015 alarms per hour");
        app.printHistogram(alarmEventTimes);

    }

    public void populateHistogramData(String dataLine) {
        try {
            String newData = StringEscapeUtils.unescapeJava(dataLine.substring(2, dataLine.length() - 1));
            ObjectMapper objectMapper = new ObjectMapper();
            Alarmdata alarmdata = objectMapper.readValue(newData, Alarmdata.class);
            if ("era015".equals(alarmdata.getVnocalarmid())){
                populateAlarmEventTimeFrequencyData(alarmdata.getAlarmeventtime());
            }
            populateAlarmTypesFrequencyData(alarmdata.getVnocalarmid().toUpperCase());
            populateAffectedNodeFrequencyData(alarmdata.getAffectednode().toUpperCase());
        } catch (Exception e) {

        }

    }

    private void populateAffectedNodeFrequencyData(String affectedNode) {
        if (affectedNodes.containsKey(affectedNode)) {
            Integer frequencyNode = affectedNodes.get(affectedNode) + 1;
            affectedNodes.put(affectedNode, frequencyNode);
        } else {
            if (affectedNode != null) {
                affectedNodes.put(affectedNode, 1);
            }
        }
    }

    private void populateAlarmTypesFrequencyData(String  alarmVnocalarmid) {
        if (alarmTypes.containsKey(alarmVnocalarmid)) {
            Integer frequencyType = alarmTypes.get(alarmVnocalarmid) + 1;
            alarmTypes.put(alarmVnocalarmid, frequencyType);
        } else {
            if (alarmVnocalarmid != null) {
                alarmTypes.put(alarmVnocalarmid, 1);
            }
        }
    }

    private void populateAlarmEventTimeFrequencyData(String alarmTime) throws ParseException {
        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");
        Date date = fmt.parse(alarmTime.toUpperCase());
        SimpleDateFormat fmt2 = new SimpleDateFormat("yyyy-MM-dd HH");
        String HourData = fmt2.format(date);
        if (alarmEventTimes.containsKey(HourData)){
            Integer frequencyTime = alarmEventTimes.get(HourData) + 1;
            alarmEventTimes.put(HourData, frequencyTime);
        }else {
            alarmEventTimes.put(HourData, 1);
        }
    }

    public void printHistogram(Map<String, Integer> data) {
        SortedSet<String> keys = new TreeSet<String>(data.keySet());
        String s = "";
        for (String key : keys) {
            s += key + " : ";
            s += data.get(key);
            s += "\n";
        }
        System.out.println(s);
    }
}
