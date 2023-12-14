package org.example;


import org.apache.flink.api.common.functions.MapFunction;

import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

//import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.RollingPolicy;
//import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.RollingPolicyFactory;
//import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.SizeRollingPolicy;

import org.apache.flink.streaming.api.windowing.time.Time;

import org.apache.flink.cep.CEP;

import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

//import org.apache.poi.ss.usermodel.Cell;
//import org.apache.poi.ss.usermodel.FormulaEvaluator;
//import org.apache.poi.ss.usermodel.Row;
//import org.apache.poi.ss.usermodel.Sheet;
//import org.apache.poi.ss.usermodel.Workbook;

//import org.apache.poi.xssf.usermodel.XSSFSheet;
//import org.apache.poi.xssf.usermodel.XSSFWorkbook;


import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class EventStreamCEP {


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

        //env.getConfig().disableSysoutLogging();


        //read the input file and put it in a list
        String filePathP1 = "/home/majidlotfian/flink/flink-quickstart/PLprivacy_Poster/input_folder/p1.txt";

        // read the contents of the file into a list of strings
        List<String> linesP1 = Files.readAllLines(Paths.get(filePathP1));

        // create a list to store the contents of the file
        List<DataEvent> eventListP1 = new ArrayList<>();

        // add the lines of the file to the list
        for (String line : linesP1) {
            eventListP1.add(new DataEvent(line));
        }

        DataStream<DataEvent> eventStreamP1 = env.fromCollection(eventListP1)
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

       //DataStream<DataEvent> eventStreamP1KeyBy = eventStreamP1
        //.keyBy(dataEvent -> dataEvent.getTimestamp());

        // read the input data directly from a file
        /*
        DataStream<DataEvent> eventStream = env.readFile(inputFormatP1, "/home/majidlotfian/flink/flink-quickstart/PLprivacy_Poster/input_folder/p1.txt")
                .map(new MapFunction<String, DataEvent>() {
                    @Override
                    public DataEvent map(String value) throws Exception {
                        // Parse the line into an event object
                        //String[] fields = value.split(",");
                        //int timestamp = Integer.parseInt(fields[0]);
                        //String type = fields[1];
                        //System.out.println("string value :"+value);
                        DataEvent event = new DataEvent(value);
                        //System.out.println("created event: "+event);
                        //event.setTimestamp(timestamp);
                        //TimeUnit.SECONDS.sleep(1);
                        //System.out.println("time: " +System.currentTimeMillis()/1000);
                        //Thread.currentThread().sleep(1000);
                        return event;
                    }
                })// Assign timestamps and watermarks
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
        */



        // Define the first private pattern
        Pattern<DataEvent, ?> privatePattern1 = Pattern.<DataEvent>begin("start")
                .where(new SimpleCondition<DataEvent>() {
                    @Override
                    public boolean filter(DataEvent dataEvent) throws Exception {
                        //System.out.println("type of this event is : "+dataEvent.getType()+" - we want A");
                        return dataEvent.getType().equals("A");
                    }
                }).next("middle")
                .where(new SimpleCondition<DataEvent>() {
                    @Override
                    public boolean filter(DataEvent dataEvent) throws Exception {
                        return dataEvent.getType().equals("B");
                    }

                }).followedBy("end")
                .where(new SimpleCondition<DataEvent>() {
                    @Override
                    public boolean filter(DataEvent dataEvent) throws Exception {
                        return dataEvent.getType().equals("A");
                    }

                })
                .within(Time.seconds(10));

        // Define the second private pattern
        Pattern<DataEvent, ?> privatePattern2 = Pattern.<DataEvent>begin("start")
                .where(new SimpleCondition<DataEvent>() {
                    @Override
                    public boolean filter(DataEvent dataEvent) throws Exception {
                        //System.out.println("type of this event is : "+dataEvent.getType()+" - we want A");
                        return dataEvent.getType().equals("A");
                    }
                }).followedBy("middle")
                .where(new SimpleCondition<DataEvent>() {
                    @Override
                    public boolean filter(DataEvent dataEvent) throws Exception {
                        return dataEvent.getType().equals("B");
                    }

                }).followedBy("end")
                .where(new SimpleCondition<DataEvent>() {
                    @Override
                    public boolean filter(DataEvent dataEvent) throws Exception {
                        return dataEvent.getType().equals("A");
                    }

                })
                .within(Time.seconds(10));

        // Define the third private pattern
        Pattern<DataEvent, ?> privatePattern3 = Pattern.<DataEvent>begin("start")
                .where(new SimpleCondition<DataEvent>() {
                    @Override
                    public boolean filter(DataEvent dataEvent) throws Exception {
                        //System.out.println("type of this event is : "+dataEvent.getType()+" - we want A");
                        return dataEvent.getType().equals("B");
                    }
                }).followedBy("middle")
                .where(new SimpleCondition<DataEvent>() {
                    @Override
                    public boolean filter(DataEvent dataEvent) throws Exception {
                        return dataEvent.getType().equals("A");
                    }

                }).followedBy("end")
                .where(new SimpleCondition<DataEvent>() {
                    @Override
                    public boolean filter(DataEvent dataEvent) throws Exception {
                        return dataEvent.getType().equals("C");
                    }

                })
                .within(Time.seconds(10));
        // Define the FORTH private pattern
        Pattern<DataEvent, ?> privatePattern4 = Pattern.<DataEvent>begin("start")
                .where(new SimpleCondition<DataEvent>() {
                    @Override
                    public boolean filter(DataEvent dataEvent) throws Exception {
                        //System.out.println("type of this event is : "+dataEvent.getType()+" - we want A");
                        return dataEvent.getType().equals("C");
                    }
                }).followedBy("middle")
                .where(new SimpleCondition<DataEvent>() {
                    @Override
                    public boolean filter(DataEvent dataEvent) throws Exception {
                        return dataEvent.getType().equals("A");
                    }

                }).followedBy("end")
                .where(new SimpleCondition<DataEvent>() {
                    @Override
                    public boolean filter(DataEvent dataEvent) throws Exception {
                        return dataEvent.getType().equals("A");
                    }

                })
                .within(Time.seconds(10));
        // Define the fifth private pattern
        Pattern<DataEvent, ?> privatePattern5 = Pattern.<DataEvent>begin("start")
                .where(new SimpleCondition<DataEvent>() {
                    @Override
                    public boolean filter(DataEvent dataEvent) throws Exception {
                        //System.out.println("type of this event is : "+dataEvent.getType()+" - we want A");
                        return dataEvent.getType().equals("B");
                    }
                }).followedBy("middle")
                .where(new SimpleCondition<DataEvent>() {
                    @Override
                    public boolean filter(DataEvent dataEvent) throws Exception {
                        return dataEvent.getType().equals("C");
                    }

                }).followedBy("end")
                .where(new SimpleCondition<DataEvent>() {
                    @Override
                    public boolean filter(DataEvent dataEvent) throws Exception {
                        return dataEvent.getType().equals("A");
                    }

                })
                .within(Time.seconds(10));


        //Define the first public pattern
        Pattern<DataEvent, ?> publicPattern1 = Pattern.<DataEvent>begin("start")
                .where(new SimpleCondition<DataEvent>() {
                    @Override
                    public boolean filter(DataEvent dataEvent) throws Exception {
                        //System.out.println("type of this event is : "+dataEvent.getType()+" - we want A");
                        return dataEvent.getType().equals("C");
                    }
                }).followedByAny("end")
                .where(new SimpleCondition<DataEvent>() {
                    @Override
                    public boolean filter(DataEvent dataEvent) throws Exception {
                        return dataEvent.getType().equals("A");
                    }

                }).within(Time.seconds(10));
        //pattern.oneOrMore();

        String PrName="Pr1";
        Pattern<DataEvent, ?> privatePattern=privatePattern5;
        Pattern<DataEvent, ?> publicPattern=publicPattern1;


        // Create pattern streams using the defined patterns
        PatternStream<DataEvent> privatePatternStream = CEP.pattern(eventStreamP1, privatePattern);
        PatternStream<DataEvent> publicPatternStream = CEP.pattern(eventStreamP1, publicPattern);



        
        //detecting the private pattern  over the input stream
        DataStream<DataEvent> PrivatePattern1Complex = privatePatternStream.process(new PatternProcessFunction<DataEvent, DataEvent>() {
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

        //Collecting the first event of each pattern match for Pr1Drop1
        DataStream<DataEvent> Pr1Drop1Stream = privatePatternStream.process(new PatternProcessFunction<DataEvent, DataEvent>() {

            @Override
            public void processMatch(Map<String, List<DataEvent>> map, Context context, Collector<DataEvent> collector) throws Exception {

                //giving out the first event of matches
                DataEvent startEvent = map.get("start").get(0);
                collector.collect(startEvent);
            }
        });

        //ataStream<DataEvent> Pr1Drop1StreamKeyBy = Pr1Drop1Stream
          //      .keyBy(dataEvent -> dataEvent.getTimestamp());

        //Collecting the middle event of each pattern match for Pr1Drop2
        DataStream<DataEvent> Pr1Drop2Stream = privatePatternStream.process(new PatternProcessFunction<DataEvent, DataEvent>() {
            @Override
            public void processMatch(Map<String, List<DataEvent>> map, Context context, Collector<DataEvent> collector) throws Exception {

                //giving out the middle event of matches
                DataEvent middleEvent = map.get("middle").get(0);
                collector.collect(middleEvent);
            }
        });

        //DataStream<DataEvent> Pr1Drop2StreamKeyBy = Pr1Drop2Stream
          //      .keyBy(dataEvent -> dataEvent.getTimestamp());


        //Collecting the last event of each pattern match for Pr1Drop3
        DataStream<DataEvent> Pr1Drop3Stream = privatePatternStream.process(new PatternProcessFunction<DataEvent, DataEvent>() {

            @Override
            public void processMatch(Map<String, List<DataEvent>> map, Context context, Collector<DataEvent> collector) throws Exception {

                //giving out the last event of matches
                DataEvent endEvent = map.get("end").get(0);
                collector.collect(endEvent);
            }
        });

        DataStream<List<DataEvent>> Pr1Reorder12ListStream = privatePatternStream.process(new PatternProcessFunction<DataEvent, List<DataEvent>>() {

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

        DataStream<List<DataEvent>> Pr1Reorder13ListStream = privatePatternStream.process(new PatternProcessFunction<DataEvent, List<DataEvent>>() {

            @Override
            public void processMatch(Map<String, List<DataEvent>> map, Context context, Collector<List<DataEvent>> collector) throws Exception {

                //giving out the last event of matches
                DataEvent event1 = map.get("start").get(0);
                DataEvent event3 = map.get("end").get(0);
                //String reorder12 = "Timestamp "+ event1.getTimestamp()+"-"+event2.getTimestamp()+
                List<DataEvent> list13 = new ArrayList<>();
                list13.add(event1);
                list13.add(event3);
                collector.collect(list13);
            }
        });


        DataStream<List<DataEvent>> Pr1Reorder23ListStream = privatePatternStream.process(new PatternProcessFunction<DataEvent, List<DataEvent>>() {

            @Override
            public void processMatch(Map<String, List<DataEvent>> map, Context context, Collector<List<DataEvent>> collector) throws Exception {

                //giving out the last event of matches
                DataEvent event2 = map.get("middle").get(0);
                DataEvent event3 = map.get("end").get(0);

                //String reorder12 = "Timestamp "+ event1.getTimestamp()+"-"+event2.getTimestamp()+
                List<DataEvent> list23 = new ArrayList<>();
                list23.add(event2);
                list23.add(event3);
                collector.collect(list23);
            }
        });

        //DataStream<DataEvent> Pr1Drop3StreamKeyBy = Pr1Drop3Stream
          //      .keyBy(dataEvent -> dataEvent.getTimestamp());

        // Detection process for public pattern
        DataStream<DataEvent> PublicPattern1Complex = publicPatternStream.process(new PatternProcessFunction<DataEvent, DataEvent>() {
            @Override
            public void processMatch(Map<String, List<DataEvent>> map, Context context, Collector<DataEvent> collector) throws Exception {

                //giving out the matches of public pattern 1 as a complex event
                DataEvent startEvent = map.get("start").get(0);
                DataEvent endEvent = map.get("end").get(0);
                String eventString = "Timestamp " + endEvent.getTimestamp() + ",Type " + startEvent.getType() + "-" + endEvent.getType() +
                        "(" + startEvent.getTimestamp() + "-" + endEvent.getTimestamp() + ")" + ",Value " + "0" + ",DONumber " +
                        startEvent.getDONumber() + ",ProducerID " + startEvent.getProducerID();
                collector.collect(new DataEvent(eventString));
            }
        });


        //code for connecting streams
        /* connect function needs streams which already applied keyby - for the full version of the paper use "KeyBy"
        //connecting streams to filter events from detected patterns
        DataStream<DataEvent> filteredStreamPrivate = droppedResultPrivateKeyBy.connect(eventStreamfromListKeyBy).flatMap(
                new RichCoFlatMapFunction<DataEvent, DataEvent, DataEvent>() {
                    private ValueState<Boolean> seen =null;

                    @Override
                    public void open(Configuration config) {
                        ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>(
                                // state name
                                "have-seen-key",
                                // type information of state
                                TypeInformation.of(new TypeHint<Boolean>() {
                                }));
                            seen = getRuntimeContext().getState(descriptor);

                    }

                    @Override
                    public void flatMap1(
                            DataEvent droppedEvent,
                            Collector<DataEvent> collector) throws Exception {
                            seen.update(Boolean.TRUE);


                    }

                    @Override
                    public void flatMap2(
                            DataEvent passedEvent,
                            Collector<DataEvent> collector) throws Exception {
                        if (seen.value() == Boolean.FALSE) {
                            collector.collect(passedEvent);
                        }

                    }
                });
        */


        // write the matched patterns to a text file for private pattern 1
        String outputPathPrivatePattern1Complex = "/home/majidlotfian/flink/flink-quickstart/PLprivacy_Poster/output_folder/1Private-1Public/Run1/Pr1Complex.txt";

        // write the detected events for drop to a text file for private pattern 1
        String outputPathPrivatePattern1Drop1 = "/home/majidlotfian/flink/flink-quickstart/PLprivacy_Poster/output_folder/1Private-1Public/Run1/Pr1Drop1.txt";
        String outputPathPrivatePattern1Drop2 = "/home/majidlotfian/flink/flink-quickstart/PLprivacy_Poster/output_folder/1Private-1Public/Run1/Pr1Drop2.txt";
        String outputPathPrivatePattern1Drop3 = "/home/majidlotfian/flink/flink-quickstart/PLprivacy_Poster/output_folder/1Private-1Public/Run1/Pr1Drop3.txt";

        // write the detected events for reorder to a text file for private pattern 1
        String outputPathPrivatePattern1Reorder12 = "/home/majidlotfian/flink/flink-quickstart/PLprivacy_Poster/output_folder/1Private-1Public/Run1/Pr1Reorder12.txt";
        String outputPathPrivatePattern1Reorder13= "/home/majidlotfian/flink/flink-quickstart/PLprivacy_Poster/output_folder/1Private-1Public/Run1/Pr1Reorder13.txt";
        String outputPathPrivatePattern1Reorder23 = "/home/majidlotfian/flink/flink-quickstart/PLprivacy_Poster/output_folder/1Private-1Public/Run1/Pr1Reorder23.txt";

        // write the matched patterns to a text file for public pattern 1
        String outputPathPublicPattern1Complex = "/home/majidlotfian/flink/flink-quickstart/PLprivacy_Poster/output_folder/1Private-1Public/Run1/Pub1Complex.txt";



        PrivatePattern1Complex.map(new MapFunction<DataEvent, String>() {
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
                .writeAsText(outputPathPrivatePattern1Complex, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);  // ensure that events are written in order

        Pr1Drop1Stream.map(new MapFunction<DataEvent, String>() {
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
                .writeAsText(outputPathPrivatePattern1Drop1, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);  // ensure that events are written in order

        Pr1Drop2Stream.map(new MapFunction<DataEvent, String>() {
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
                .writeAsText(outputPathPrivatePattern1Drop2, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);  // ensure that events are written in order

        Pr1Drop3Stream.map(new MapFunction<DataEvent, String>() {
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
                .writeAsText(outputPathPrivatePattern1Drop3, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);  // ensure that events are written in order

        Pr1Reorder12ListStream.map(new MapFunction<List<DataEvent>, String>() {
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
                .writeAsText(outputPathPrivatePattern1Reorder12, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);  // ensure that events are written in order

        Pr1Reorder13ListStream.map(new MapFunction<List<DataEvent>, String>() {
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
                .writeAsText(outputPathPrivatePattern1Reorder13, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);  // ensure that events are written in order

        Pr1Reorder23ListStream.map(new MapFunction<List<DataEvent>, String>() {
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
                .writeAsText(outputPathPrivatePattern1Reorder23, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);  // ensure that events are written in order

        //writing the filtered stream with connecting stream funciton in an output file
        /*
        filteredStreamPrivate.map(new MapFunction<DataEvent, String>() {
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
                .writeAsText(outputPathFilteredStreamPrivate, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);  // ensure that events are written in order
        */


        PublicPattern1Complex.map(new MapFunction<DataEvent, String>() {
                    @Override
                    public String map(DataEvent event) throws Exception {
                        //System.out.println("event in output: "+event);
                        String eventString1 = "";
                        for (int i=0;i<event.getData().size();i++){
                            eventString1+= event.getData().get(i).getAttributeName() + " " + event.getData().get(i).getAttributeValue();
                            if (i<event.getData().size()-1){
                                eventString1+=",";
                            }
                        }
                        return eventString1;
                    }
                })
                .writeAsText(outputPathPublicPattern1Complex, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);  // ensure that events are written in order


        env.execute("EventStreamCEP");


    }
    /*
    private static void drop(List<Long> timestamps, DataStream<DataEvent> eventStream, Pattern<DataEvent, ?> pattern, DataStream<DataEvent> droppedEventStream) throws IOException {


        //eventStream.print();
        System.out.println("size :" + timestamps.size());
        for (long t:timestamps) {
            try {
                droppedEventStream = eventStream.filter(event -> event.getTimestamp() != t);

            }catch(NullPointerException e) {
                System.out.println("NullPointerException thrown!");
            }
        }
        //System.out.println("dropped stream : ");
        //droppedEventStream.print();




        // Create a pattern stream using the defined pattern
        PatternStream<DataEvent> patternStreamAfterDrop = CEP.pattern(droppedEventStream, pattern);

        DataStream<DataEvent> resultsAfterDrop = patternStreamAfterDrop.process(new PatternProcessFunction<DataEvent, DataEvent>() {

            @Override
            public void processMatch(Map<String, List<DataEvent>> map, Context context, Collector<DataEvent> collector) throws Exception {




                DataEvent startEvent = map.get("start").get(0);
                DataEvent middleEvent = map.get("middle").get(0);
                DataEvent endEvent = map.get("end").get(0);
                collector.collect(new DataEvent( endEvent.getTimestamp(),
                        "After Drop : "+startEvent.getType()+"-"+ middleEvent.getType()+"-"+ endEvent.getType() + "("+startEvent.getTimestamp()+"-" +middleEvent.getTimestamp()+"-" +endEvent.getTimestamp()+")"));
            }
        });



        String outputPath2 = "/home/majidlotfian/flink/flink-quickstart/PLprivacy/output_folder/output2.txt";
        resultsAfterDrop.map(new MapFunction<DataEvent, String>() {
                    @Override
                    public String map(DataEvent value) throws Exception {
                        return value.getTimestamp()+":"+value.getType();
                    }
                })
                .writeAsText(outputPath2, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);  // ensure that events are written in order


    }

    */
    /*
    private static void initializeEnvironment() throws IOException {

        // obtaining input bytes from a file
        FileInputStream fis=new FileInputStream(new File("/home/majidlotfian/flink/flink-quickstart/PLprivacy/input_folder/input.xlsx"));

        // creating workbook instance that refers to .xlsx file
        XSSFWorkbook wb=new XSSFWorkbook(fis);

        // creating a Sheet object to retrieve the object
        XSSFSheet sheet=wb.getSheetAt(0);

        // evaluating cell type
        FormulaEvaluator formulaEvaluator=wb.getCreationHelper().createFormulaEvaluator();

        String type="";
        int timestamp=0;

        for(Row row: sheet)     //iteration over row using for each loop
        {
            if (row.getRowNum()>0) {
                boolean isEmptyRow = true;

                // Iterate through each cell in the row
                for (Cell cell : row) {
                    if (cell.getCellType() != Cell.CELL_TYPE_BLANK) {
                        isEmptyRow = false;
                        break;
                    }
                }

                if (isEmptyRow) {
                    continue;
                }

                for (Cell cell : row)    //iteration over cell using for each loop
                {
                    if (cell.getColumnIndex() == 0) {
                        //System.out.println(cell.getNumericCellValue());
                        timestamp = (int) cell.getNumericCellValue();
                    }
                    if (cell.getColumnIndex() == 1) {
                        //System.out.println(cell.getNumericCellValue());
                        type = cell.getStringCellValue();
                    }


                }


                originalStream.add(new DataEvent(timestamp, type));
                System.out.println("original stream "+ originalStream.get(row.getRowNum()-1).getTimestamp()
                        + " : " + originalStream.get(row.getRowNum()-1).getType());
            }
        }
    }

 */


}
