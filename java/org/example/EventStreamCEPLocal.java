package org.example;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;


public class EventStreamCEPLocal {


    public static ArrayList<String> producerList= new ArrayList<String>(){
        {
            add("p1");
            add("p2");
            add("p3");
            add("p4");
            add("p5");
            add("p6");
        }
    };

    public static ArrayList<String> DOList= new ArrayList<String>(){
        {
            add("do1");
            add("do2");
            add("do3");
        }
    };



    public static void main(String[] args) throws Exception {


        //initializeEnvironment();


        // Set up the Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        String inputDirectory = "/home/majidlotfian/flink/flink-quickstart/PLprivacy_Poster/input_folder/";

        //creating event list for each producer
        List<DataEvent> eventListP2 = InitializeProducerLists(inputDirectory,"p2");
        List<DataEvent> eventListP3 = InitializeProducerLists(inputDirectory,"p3");
        List<DataEvent> eventListP4 = InitializeProducerLists(inputDirectory,"p4");
        List<DataEvent> eventListP5 = InitializeProducerLists(inputDirectory,"p5");
        List<DataEvent> eventListP6 = InitializeProducerLists(inputDirectory,"p6");



        //creating merged list of events for each public pattern
        List<DataEvent> inputListPublicPattern1= new ArrayList<>();
        inputListPublicPattern1.addAll(eventListP2);
        inputListPublicPattern1.addAll(eventListP3);
        inputListPublicPattern1.addAll(eventListP4);
        Collections.sort(inputListPublicPattern1);

        List<DataEvent> inputListPublicPattern2= new ArrayList<>();
        inputListPublicPattern2.addAll(eventListP3);
        inputListPublicPattern2.addAll(eventListP4);
        inputListPublicPattern2.addAll(eventListP5);
        inputListPublicPattern2.addAll(eventListP6);
        Collections.sort(inputListPublicPattern2);



        //creating event stream for each producer
        DataStream<DataEvent> eventStreamP2 = InitializeProducerStreams(eventListP2,env);
        DataStream<DataEvent> eventStreamP3 = InitializeProducerStreams(eventListP3,env);
        DataStream<DataEvent> eventStreamP4 = InitializeProducerStreams(eventListP4,env);
        DataStream<DataEvent> eventStreamP5 = InitializeProducerStreams(eventListP5,env);
        DataStream<DataEvent> eventStreamP6 = InitializeProducerStreams(eventListP6,env);

        //creating event streams for each public pattern

        DataStream<DataEvent> eventStreamPub1PrivatePattern1 = InitializeProducerStreams(inputListPublicPattern1,env);
        DataStream<DataEvent> eventStreamPub1PrivatePattern2 = InitializeProducerStreams(inputListPublicPattern1,env);
        DataStream<DataEvent> eventStreamPub1PrivatePattern3 = InitializeProducerStreams(inputListPublicPattern1,env);

        DataStream<DataEvent> eventStreamPub1PrivatePattern1Drop1 = InitializeProducerStreams(inputListPublicPattern1,env);
        DataStream<DataEvent> eventStreamPub1PrivatePattern2Drop1 = InitializeProducerStreams(inputListPublicPattern1,env);
        DataStream<DataEvent> eventStreamPub1PrivatePattern3Drop1 = InitializeProducerStreams(inputListPublicPattern1,env);


        DataStream<DataEvent> eventStreamPub1PrivatePattern1Reorder12 = InitializeProducerStreams(inputListPublicPattern1,env);
        DataStream<DataEvent> eventStreamPub1PrivatePattern2Reorder12 = InitializeProducerStreams(inputListPublicPattern1,env);
        DataStream<DataEvent> eventStreamPub1PrivatePattern3Reorder12 = InitializeProducerStreams(inputListPublicPattern1,env);


        DataStream<DataEvent> eventStreamPublicPattern1 = InitializeProducerStreams(inputListPublicPattern1,env);





        DataStream<DataEvent> eventStreamPub2PrivatePattern2 = InitializeProducerStreams(inputListPublicPattern2,env);
        DataStream<DataEvent> eventStreamPub2PrivatePattern3 = InitializeProducerStreams(inputListPublicPattern2,env);
        DataStream<DataEvent> eventStreamPub2PrivatePattern4 = InitializeProducerStreams(inputListPublicPattern2,env);
        DataStream<DataEvent> eventStreamPub2PrivatePattern5 = InitializeProducerStreams(inputListPublicPattern2,env);

        DataStream<DataEvent> eventStreamPub2PrivatePattern2Drop1 = InitializeProducerStreams(inputListPublicPattern2,env);
        DataStream<DataEvent> eventStreamPub2PrivatePattern3Drop1 = InitializeProducerStreams(inputListPublicPattern2,env);
        DataStream<DataEvent> eventStreamPub2PrivatePattern4Drop1 = InitializeProducerStreams(inputListPublicPattern2,env);
        DataStream<DataEvent> eventStreamPub2PrivatePattern5Drop1 = InitializeProducerStreams(inputListPublicPattern2,env);

        DataStream<DataEvent> eventStreamPub2PrivatePattern2Reorder12 = InitializeProducerStreams(inputListPublicPattern2,env);
        DataStream<DataEvent> eventStreamPub2PrivatePattern3Reorder12 = InitializeProducerStreams(inputListPublicPattern2,env);
        DataStream<DataEvent> eventStreamPub2PrivatePattern4Reorder12 = InitializeProducerStreams(inputListPublicPattern2,env);
        DataStream<DataEvent> eventStreamPub2PrivatePattern5Reorder12 = InitializeProducerStreams(inputListPublicPattern2,env);
        DataStream<DataEvent> eventStreamPublicPattern2 = InitializeProducerStreams(inputListPublicPattern2,env);



        //creating a list of event streams
        List<DataStream<DataEvent>> producersEventStreams = new ArrayList<>();
        producersEventStreams.add(eventStreamP2);
        producersEventStreams.add(eventStreamP3);
        producersEventStreams.add(eventStreamP4);
        producersEventStreams.add(eventStreamP5);
        producersEventStreams.add(eventStreamP6);






        // Define the first private pattern
        Pattern<DataEvent, ?> privatePattern1 = Pattern.<DataEvent>begin("start")
                .where(new SimpleCondition<DataEvent>() {
                    @Override
                    public boolean filter(DataEvent dataEvent) throws Exception {
                        //System.out.println("type of this event is : "+dataEvent.getType()+" - we want A");
                        return dataEvent.getType().equals("A") && dataEvent.getDONumber().equals("do1");
                    }
                }).followedBy("middle")
                .where(new SimpleCondition<DataEvent>() {
                    @Override
                    public boolean filter(DataEvent dataEvent) throws Exception {
                        return dataEvent.getType().equals("B")&& dataEvent.getDONumber().equals("do1");
                    }

                }).followedBy("end")
                .where(new SimpleCondition<DataEvent>() {
                    @Override
                    public boolean filter(DataEvent dataEvent) throws Exception {
                        return dataEvent.getType().equals("A")&& dataEvent.getDONumber().equals("do1");
                    }

                })
                .within(Time.seconds(10));

        // Define the second private pattern
        Pattern<DataEvent, ?> privatePattern2 = Pattern.<DataEvent>begin("start")
                .where(new SimpleCondition<DataEvent>() {
                    @Override
                    public boolean filter(DataEvent dataEvent) throws Exception {
                        //System.out.println("type of this event is : "+dataEvent.getType()+" - we want A");
                        return dataEvent.getType().equals("A")&& dataEvent.getDONumber().equals("do2");
                    }
                }).followedBy("middle")
                .where(new SimpleCondition<DataEvent>() {
                    @Override
                    public boolean filter(DataEvent dataEvent) throws Exception {
                        return dataEvent.getType().equals("B")&& dataEvent.getDONumber().equals("do2");
                    }

                }).followedBy("end")
                .where(new SimpleCondition<DataEvent>() {
                    @Override
                    public boolean filter(DataEvent dataEvent) throws Exception {
                        return dataEvent.getType().equals("A")&& dataEvent.getDONumber().equals("do2");
                    }

                })
                .within(Time.seconds(10));

        // Define the third private pattern
        Pattern<DataEvent, ?> privatePattern3 = Pattern.<DataEvent>begin("start")
                .where(new SimpleCondition<DataEvent>() {
                    @Override
                    public boolean filter(DataEvent dataEvent) throws Exception {
                        //System.out.println("type of this event is : "+dataEvent.getType()+" - we want A");
                        return dataEvent.getType().equals("B")&& dataEvent.getDONumber().equals("do2");
                    }
                }).followedBy("middle")
                .where(new SimpleCondition<DataEvent>() {
                    @Override
                    public boolean filter(DataEvent dataEvent) throws Exception {
                        return dataEvent.getType().equals("A")&& dataEvent.getDONumber().equals("do2");
                    }

                }).followedBy("end")
                .where(new SimpleCondition<DataEvent>() {
                    @Override
                    public boolean filter(DataEvent dataEvent) throws Exception {
                        return dataEvent.getType().equals("C")&& dataEvent.getDONumber().equals("do2");
                    }

                })
                .within(Time.seconds(10));
        // Define the FORTH private pattern
        Pattern<DataEvent, ?> privatePattern4 = Pattern.<DataEvent>begin("start")
                .where(new SimpleCondition<DataEvent>() {
                    @Override
                    public boolean filter(DataEvent dataEvent) throws Exception {
                        //System.out.println("type of this event is : "+dataEvent.getType()+" - we want A");
                        return dataEvent.getType().equals("C")&& dataEvent.getDONumber().equals("do3");
                    }
                }).followedBy("middle")
                .where(new SimpleCondition<DataEvent>() {
                    @Override
                    public boolean filter(DataEvent dataEvent) throws Exception {
                        return dataEvent.getType().equals("A")&& dataEvent.getDONumber().equals("do3");
                    }

                }).followedBy("end")
                .where(new SimpleCondition<DataEvent>() {
                    @Override
                    public boolean filter(DataEvent dataEvent) throws Exception {
                        return dataEvent.getType().equals("A")&& dataEvent.getDONumber().equals("do3");
                    }

                })
                .within(Time.seconds(10));
        // Define the fifth private pattern
        Pattern<DataEvent, ?> privatePattern5 = Pattern.<DataEvent>begin("start")
                .where(new SimpleCondition<DataEvent>() {
                    @Override
                    public boolean filter(DataEvent dataEvent) throws Exception {
                        //System.out.println("type of this event is : "+dataEvent.getType()+" - we want A");
                        return dataEvent.getType().equals("B")&& dataEvent.getDONumber().equals("do3");
                    }
                }).followedBy("middle")
                .where(new SimpleCondition<DataEvent>() {
                    @Override
                    public boolean filter(DataEvent dataEvent) throws Exception {
                        return dataEvent.getType().equals("C")&& dataEvent.getDONumber().equals("do3");
                    }

                }).followedBy("end")
                .where(new SimpleCondition<DataEvent>() {
                    @Override
                    public boolean filter(DataEvent dataEvent) throws Exception {
                        return dataEvent.getType().equals("A")&& dataEvent.getDONumber().equals("do3");
                    }

                })
                .within(Time.seconds(10));

        //creating a list of private patterns
        List<Pattern<DataEvent, ?>> privatePatternSet = new ArrayList<>();
        privatePatternSet.add(privatePattern1);
        privatePatternSet.add(privatePattern2);
        privatePatternSet.add(privatePattern3);
        privatePatternSet.add(privatePattern4);
        privatePatternSet.add(privatePattern5);



        //Define the first public pattern
        Pattern<DataEvent, ?> publicPattern1 = Pattern.<DataEvent>begin("start")
                .where(new SimpleCondition<DataEvent>() {
                    @Override
                    public boolean filter(DataEvent dataEvent) throws Exception {
                        //System.out.println("type of this event is : "+dataEvent.getType()+" - we want A");
                        return dataEvent.getType().equals("C")&& dataEvent.getDONumber().equals("do1");
                    }
                }).followedByAny("end")
                .where(new SimpleCondition<DataEvent>() {
                    @Override
                    public boolean filter(DataEvent dataEvent) throws Exception {
                        return dataEvent.getType().equals("A")&& dataEvent.getDONumber().equals("do2");
                    }

                }).within(Time.seconds(10));


        Pattern<DataEvent, ?> publicPattern2 = Pattern.<DataEvent>begin("start")
                .where(new SimpleCondition<DataEvent>() {
                    @Override
                    public boolean filter(DataEvent dataEvent) throws Exception {
                        //System.out.println("type of this event is : "+dataEvent.getType()+" - we want A");
                        return dataEvent.getType().equals("B")&& dataEvent.getDONumber().equals("do2");
                    }
                }).followedByAny("end")
                .where(new SimpleCondition<DataEvent>() {
                    @Override
                    public boolean filter(DataEvent dataEvent) throws Exception {
                        return dataEvent.getType().equals("D")&& dataEvent.getDONumber().equals("do3");
                    }

                }).within(Time.seconds(10));
        //pattern.oneOrMore();


        //creating a list of public patterns
        List<Pattern<DataEvent, ?>> publicPatternSet = new ArrayList<>();
        publicPatternSet.add(publicPattern1);
        publicPatternSet.add(publicPattern2);

        //PROCESSING the merged stream for all public patterns
        String outputDirectory1 = "/home/majidlotfian/flink/flink-quickstart/PLprivacy_Poster/output_folder/5Private-2Public/Local/Run1/publicPattern1/";

        privatePatternProcess(privatePattern1, eventStreamPub1PrivatePattern1,outputDirectory1,"Pr1Complex.txt");
        privatePatternProcess(privatePattern2, eventStreamPub1PrivatePattern2,outputDirectory1,"Pr2Complex.txt");
        privatePatternProcess(privatePattern3, eventStreamPub1PrivatePattern3,outputDirectory1,"Pr3Complex.txt");
        publicPatternProcess(publicPattern1, eventStreamPublicPattern1 ,outputDirectory1,"Pub1Complex.txt");

        DroppingProcess(privatePattern1, eventStreamPub1PrivatePattern1Drop1,outputDirectory1,"Pr1Drop1.txt");
        DroppingProcess(privatePattern2, eventStreamPub1PrivatePattern2Drop1,outputDirectory1,"Pr2Drop1.txt");
        DroppingProcess(privatePattern3, eventStreamPub1PrivatePattern3Drop1,outputDirectory1,"Pr3Drop1.txt");

        ReorderingProcess(privatePattern1, eventStreamPub1PrivatePattern1Reorder12,outputDirectory1,"Pr1Reorder12.txt");
        ReorderingProcess(privatePattern2, eventStreamPub1PrivatePattern2Reorder12,outputDirectory1,"Pr2Reorder12.txt");
        ReorderingProcess(privatePattern3, eventStreamPub1PrivatePattern3Reorder12,outputDirectory1,"Pr3Reorder12.txt");



        String outputDirectory2 = "/home/majidlotfian/flink/flink-quickstart/PLprivacy_Poster/output_folder/5Private-2Public/Local/Run1/publicPattern2/";

        privatePatternProcess(privatePattern2, eventStreamPub2PrivatePattern2,outputDirectory2,"Pr2Complex.txt");
        privatePatternProcess(privatePattern3, eventStreamPub2PrivatePattern3,outputDirectory2,"Pr3Complex.txt");
        privatePatternProcess(privatePattern4, eventStreamPub2PrivatePattern4,outputDirectory2,"Pr4Complex.txt");
        privatePatternProcess(privatePattern5, eventStreamPub2PrivatePattern5,outputDirectory2,"Pr5Complex.txt");
        publicPatternProcess(publicPattern2, eventStreamPublicPattern2 ,outputDirectory2,"Pub2Complex.txt");
        DroppingProcess(privatePattern2, eventStreamPub2PrivatePattern2Drop1,outputDirectory2,"Pr2Drop1.txt");
        DroppingProcess(privatePattern3, eventStreamPub2PrivatePattern3Drop1,outputDirectory2,"Pr3Drop1.txt");
        DroppingProcess(privatePattern2, eventStreamPub2PrivatePattern2Drop1,outputDirectory2,"Pr4Drop1.txt");
        DroppingProcess(privatePattern3, eventStreamPub2PrivatePattern3Drop1,outputDirectory2,"Pr5Drop1.txt");

        ReorderingProcess(privatePattern2, eventStreamPub2PrivatePattern2Reorder12,outputDirectory2,"Pr2Reorder12.txt");
        ReorderingProcess(privatePattern3, eventStreamPub2PrivatePattern3Reorder12,outputDirectory2,"Pr3Reorder12.txt");
        ReorderingProcess(privatePattern4, eventStreamPub2PrivatePattern4Reorder12,outputDirectory2,"Pr4Reorder12.txt");
        ReorderingProcess(privatePattern5, eventStreamPub2PrivatePattern5Reorder12,outputDirectory2,"Pr5Reorder12.txt");


        env.execute("EventStreamCEP");


    }
    public static List<DataEvent> InitializeProducerLists(String directory, String producerID) throws IOException {

        String filePath = directory + producerID+".txt";

        // read the contents of the file into a list of strings
        List<String> lines = Files.readAllLines(Paths.get(filePath));

        // create a list to store the contents of the file
        List<DataEvent> eventList = new ArrayList<>();

        // add the lines of the file to the list
        for (String line : lines) {
            eventList.add(new DataEvent(line));
        }
        return eventList;
    }
    public static DataStream<DataEvent> InitializeProducerStreams(List<DataEvent> eventList , StreamExecutionEnvironment env){

        DataStream<DataEvent> results = env.fromCollection(eventList)
                // Assign timestamps and watermarks
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<DataEvent>() {
                    private long currentMaxTimestamp;
                    private final long maxOutOfOrderness = 10000; // 10 seconds

                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
                    }

                    @Override
                    public long extractTimestamp(DataEvent element, long previousElementTimestamp) {
                        long timestamp = Long.parseLong(element.getTimestamp());
                        currentMaxTimestamp = Math.max(currentMaxTimestamp, timestamp);
                        return timestamp;
                    }
                });
        return results;


    }

    public static void privatePatternProcess(Pattern<DataEvent, ?> privatePattern, DataStream<DataEvent> inputStream,String directory, String filename){

        PatternStream<DataEvent> patternStream = CEP.pattern(inputStream, privatePattern);


            //detecting and outputting the results for private pattern without privacy protection
        DataStream<DataEvent> privatePatternComplex = patternStream.process(new PatternProcessFunction<DataEvent, DataEvent>() {
            @Override
            public void processMatch(Map<String, List<DataEvent>> map, Context context, Collector<DataEvent> collector) throws Exception {

                //giving out the matches as a complex event
                DataEvent startEvent = map.get("start").get(0);
                DataEvent middleEvent = map.get("middle").get(0);
                DataEvent endEvent = map.get("end").get(0);
                String eventString = "Timestamp " + endEvent.getTimestamp() + ",Type " + startEvent.getType() + "-" + middleEvent.getType() + "-" + endEvent.getType() +
                        "(" + startEvent.getTimestamp() + "-" + middleEvent.getTimestamp() + "-" + endEvent.getTimestamp() + ")" + ",Value " + "0" + ",DONumber " +
                        startEvent.getDONumber() + ",ProducerID " + startEvent.getProducerID();
                collector.collect(new DataEvent(eventString));
            }
        });

        writeDataStreamToFile(privatePatternComplex, directory+filename);

    }

    public static void publicPatternProcess(Pattern<DataEvent, ?> publicPattern, DataStream<DataEvent> inputStream, String directory, String filename){


        PatternStream<DataEvent> patternStream= CEP.pattern(inputStream, publicPattern);

        DataStream<DataEvent> publicPatternComplex = patternStream.process(new PatternProcessFunction<DataEvent, DataEvent>() {
            @Override
            public void processMatch(Map<String, List<DataEvent>> map, Context context, Collector<DataEvent> collector) throws Exception {

                //giving out the matches of public pattern 1 as a complex event
                DataEvent startEvent = map.get("start").get(0);
                DataEvent endEvent = map.get("end").get(0);
                String eventString = "Timestamp " + endEvent.getTimestamp() + ",Type " + startEvent.getType() + "-" + endEvent.getType() +
                        "(" + startEvent.getTimestamp() + "-" + endEvent.getTimestamp() + ")" + ",Value " + "0" + ",DONumber " +
                        "(" + startEvent.getDONumber() + "-"+endEvent.getDONumber() + ")" + ",ProducerID " + startEvent.getProducerID();
                collector.collect(new DataEvent(eventString));
            }
        });

        writeDataStreamToFile(publicPatternComplex, directory+filename);

    }


    public static void DroppingProcess(Pattern<DataEvent, ?> privatePattern, DataStream<DataEvent> inputStream, String directory, String filename){
        PatternStream<DataEvent> patternStream = CEP.pattern(inputStream, privatePattern);
        DataStream<DataEvent> drop1Stream = patternStream.process(new PatternProcessFunction<DataEvent, DataEvent>() {

            @Override
            public void processMatch(Map<String, List<DataEvent>> map, Context context, Collector<DataEvent> collector) throws Exception {

                //giving out the first event of matches
                DataEvent startEvent = map.get("start").get(0);
                collector.collect(startEvent);
            }
        });

        writeDataStreamToFile(drop1Stream, directory+filename);

    }

    public static void ReorderingProcess(Pattern<DataEvent, ?> privatePattern, DataStream<DataEvent> inputStream, String directory, String filename){

        PatternStream<DataEvent> patternStream = CEP.pattern(inputStream, privatePattern);
        DataStream<List<DataEvent>> Reorder12ListStream = patternStream.process(new PatternProcessFunction<DataEvent, List<DataEvent>>() {

            @Override
            public void processMatch(Map<String, List<DataEvent>> map, Context context, Collector<List<DataEvent>> collector) throws Exception {

                //giving out the last event of matches
                DataEvent event1 = map.get("start").get(0);
                DataEvent event2 = map.get("middle").get(0);
                //String reorder12 = "Timestamp "+ event1.getTimestamp()+"-"+event2.getTimestamp()+
                List<DataEvent> list12 = new ArrayList<>();
                list12.add(event1);
                list12.add(event2);
                collector.collect(list12);
            }
        });

        writeDataStreamListToFile(Reorder12ListStream, directory+filename);



    }

    public static void writeDataStreamToFile(DataStream<DataEvent> dataStream, String filePath){
        dataStream.map(new MapFunction<DataEvent, String>() {
                    @Override
                    public String map(DataEvent event) throws Exception {
                        //System.out.println("event in output: "+event);
                        String eventString = "";
                        for (int i=0;i<event.getData().size();i++){
                            eventString+= event.getData().get(i).getAttributeName() + " " + event.getData().get(i).getAttributeValue();
                            if (i<event.getData().size()-1){
                                eventString+=",";
                            }
                        }
                        return eventString;
                    }
                })
                .writeAsText(filePath, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);  // ensure that events are written in order
    }

    public static void writeDataStreamListToFile(DataStream<List<DataEvent>> dataStream, String filePath){
        dataStream.map(new MapFunction<List<DataEvent>, String>() {
                    @Override
                    public String map(List<DataEvent> eventList) throws Exception {
                        //System.out.println("event in output: "+event);
                        String eventString = "";
                        for (int i=0;i<eventList.get(0).getData().size();i++){
                            eventString+= eventList.get(0).getData().get(i).getAttributeName() + " " + eventList.get(0).getData().get(i).getAttributeValue();
                            if (i<eventList.get(0).getData().size()-1){
                                eventString+=",";
                            }
                        }
                        eventString+= "*";
                        for (int i=0;i<eventList.get(1).getData().size();i++){
                            eventString+= eventList.get(1).getData().get(i).getAttributeName() + " " + eventList.get(1).getData().get(i).getAttributeValue();
                            if (i<eventList.get(1).getData().size()-1){
                                eventString+=",";
                            }
                        }

                        return eventString;
                    }
                })
                .writeAsText(filePath, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);  // ensure that events are written in order
    }



}
