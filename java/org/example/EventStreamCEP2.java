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

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;


public class EventStreamCEP2 {

    public static List<DataEvent> originalStream = new ArrayList<>();
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
    public static List<DataEvent> complexEvents = new ArrayList<>();


    public static void main(String[] args) throws Exception {


        // Set up the Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);



        //creating list of events from original stream

        //read the input file and put it in a list
        String filePathP1 = "/home/majidlotfian/flink/flink-quickstart/PLprivacy_Poster/input_folder/p1.txt";

        // read the contents of the file into a list of strings
        List<String> linesP1D1 = Files.readAllLines(Paths.get(filePathP1));

        // create a list to store the contents of the file
        List<DataEvent> eventListP1D1 = new ArrayList<>();

        // add the lines of the file to the list
        for (String line : linesP1D1) {
            eventListP1D1.add(new DataEvent(line));
        }

        // read the contents of the file into a list of strings
        List<String> linesP1D2 = Files.readAllLines(Paths.get(filePathP1));

        // create a list to store the contents of the file
        List<DataEvent> eventListP1D2 = new ArrayList<>();

        // add the lines of the file to the list
        for (String line : linesP1D2) {
            eventListP1D2.add(new DataEvent(line));
        }

        // read the contents of the file into a list of strings
        List<String> linesP1D3 = Files.readAllLines(Paths.get(filePathP1));

        // create a list to store the contents of the file
        List<DataEvent> eventListP1D3 = new ArrayList<>();

        // add the lines of the file to the list
        for (String line : linesP1D3) {
            eventListP1D3.add(new DataEvent(line));
        }
//**************************************************REORDER
        // read the contents of the file into a list of strings
        List<String> linesP1R12 = Files.readAllLines(Paths.get(filePathP1));

        // create a list to store the contents of the file
        List<DataEvent> eventListP1R12 = new ArrayList<>();

        // add the lines of the file to the list
        for (String line : linesP1R12) {
            eventListP1R12.add(new DataEvent(line));
        }

///////////////////////////////////////////
        // read the contents of the file into a list of strings
        List<String> linesP1R13 = Files.readAllLines(Paths.get(filePathP1));

        // create a list to store the contents of the file
        List<DataEvent> eventListP1R13 = new ArrayList<>();

        // add the lines of the file to the list
        for (String line : linesP1R13) {
            eventListP1R13.add(new DataEvent(line));
        }
///////////////////////////////////////////
        // read the contents of the file into a list of strings
        List<String> linesP1R23 = Files.readAllLines(Paths.get(filePathP1));

        // create a list to store the contents of the file
        List<DataEvent> eventListP1R23 = new ArrayList<>();

        // add the lines of the file to the list
        for (String line : linesP1R23) {
            eventListP1R23.add(new DataEvent(line));
        }
///////////////////////////////////////////


        //creating list of events from Dropped stream

        //read the input file and put it in a list
        String filePathPr1Drop1 = "/home/majidlotfian/flink/flink-quickstart/PLprivacy_Poster/output_folder/1Private-1Public/Run1/Pr1Drop1.txt";
        String filePathPr1Drop2 = "/home/majidlotfian/flink/flink-quickstart/PLprivacy_Poster/output_folder/1Private-1Public/Run1/Pr1Drop2.txt";
        String filePathPr1Drop3 = "/home/majidlotfian/flink/flink-quickstart/PLprivacy_Poster/output_folder/1Private-1Public/Run1/Pr1Drop3.txt";

        //read the input file OF REORDER and put it in a list
        String filePathPr1Reorder12 = "/home/majidlotfian/flink/flink-quickstart/PLprivacy_Poster/output_folder/1Private-1Public/Run1/Pr1Reorder12.txt";
        String filePathPr1Reorder13 = "/home/majidlotfian/flink/flink-quickstart/PLprivacy_Poster/output_folder/1Private-1Public/Run1/Pr1Reorder13.txt";
        String filePathPr1Reorder23 = "/home/majidlotfian/flink/flink-quickstart/PLprivacy_Poster/output_folder/1Private-1Public/Run1/Pr1Reorder23.txt";


        // read the contents of the file into a list of strings
        List<String> linesDrop1 = Files.readAllLines(Paths.get(filePathPr1Drop1));
        List<String> linesDrop2 = Files.readAllLines(Paths.get(filePathPr1Drop2));
        List<String> linesDrop3 = Files.readAllLines(Paths.get(filePathPr1Drop3));

        // read the contents of the reordering file into a list of strings
        List<String> linesReorder12 = Files.readAllLines(Paths.get(filePathPr1Reorder12));
        List<String> linesReorder13 = Files.readAllLines(Paths.get(filePathPr1Reorder13));
        List<String> linesReorder23 = Files.readAllLines(Paths.get(filePathPr1Reorder23));



        // create a list to store the contents of the file
        List<DataEvent> eventListDrop1 = new ArrayList<>();
        List<DataEvent> eventListDrop2 = new ArrayList<>();
        List<DataEvent> eventListDrop3 = new ArrayList<>();

        // create a list to store the contents of the file
        List<List<DataEvent>> eventListReorder12 = new ArrayList<>();
        List<List<DataEvent>> eventListReorder13 = new ArrayList<>();
        List<List<DataEvent>> eventListReorder23 = new ArrayList<>();


        //**************** drop event 1
        // add the lines of the Pr1Drop1 file to the list
        for (String line : linesDrop1) {
            eventListDrop1.add(new DataEvent(line));
        }

        Collections.sort(eventListDrop1);

        //removing dropped events from original stream
        List<String> droppedTimestamps1 = new ArrayList<>();
        for (DataEvent dataEventDropped: eventListDrop1) {
            droppedTimestamps1.add(dataEventDropped.getTimestamp());
        }

        for (String t: droppedTimestamps1) {
            eventListP1D1.remove(DataEvent.extractByTimestamp(t, eventListP1D1));
        }


        //**************** drop event 2
        // add the lines of the Pr1Drop2 file to the list
        for (String line : linesDrop2) {
            eventListDrop2.add(new DataEvent(line));
        }

        Collections.sort(eventListDrop2);

        //removing dropped events from original stream
        List<String> droppedTimestamps2 = new ArrayList<>();
        for (DataEvent dataEventDropped: eventListDrop2) {
            droppedTimestamps2.add(dataEventDropped.getTimestamp());
        }

        for (String t:droppedTimestamps2) {
            eventListP1D2.remove(DataEvent.extractByTimestamp(t, eventListP1D2));
        }
        //System.out.println("size drop 2 : "+eventListDrop2.size() + " p1d2: "+eventListP1D2.size());


        //**************** drop event 3
        // add the lines of the Pr1Drop3 file to the list
        for (String line : linesDrop3) {
            eventListDrop3.add(new DataEvent(line));
        }

        Collections.sort(eventListDrop3);

        //removing dropped events from original stream
        List<String> droppedTimestamps3 = new ArrayList<>();
        for (DataEvent dataEventDropped: eventListDrop3) {
            droppedTimestamps3.add(dataEventDropped.getTimestamp());
        }

        for (String t: droppedTimestamps3) {
            eventListP1D3.remove(DataEvent.extractByTimestamp(t, eventListP1D3));
        }
        //********************reorder
        //**************** reorder events 1 and 2
        // add the lines of the Pr1R12 file to the list
        for (String line : linesReorder12) {
            String[] s = line.split("\\*");
            List<DataEvent> list12 = new ArrayList<>();
            list12.add(new DataEvent(s[0]));
            list12.add(new DataEvent(s[1]));
            Collections.sort(list12);

            eventListReorder12.add(list12);
        }

        //reordering events in original stream
        List<List<String>> reorderTimestamps12 = new ArrayList<>();

        for (List<DataEvent> reorderEventPair: eventListReorder12) {
            List<String> list12=new ArrayList<>();
            for (DataEvent de:reorderEventPair) {
                list12.add(de.getTimestamp());
            }

            reorderTimestamps12.add(list12);

        }

        for (List<String> l: reorderTimestamps12) {
            DataEvent r1 = DataEvent.extractByTimestamp(l.get(0), eventListP1R12);
            DataEvent r2 = DataEvent.extractByTimestamp(l.get(1), eventListP1R12);
            String r1Timestamp=r1.getTimestamp();
            String r2Timestamp=r2.getTimestamp();


            DataEvent.setTimestamp(r1,r2Timestamp);
            DataEvent.setTimestamp(r2,r1Timestamp);

            eventListP1R12.set(Integer.parseInt(l.get(0))-1, r2);
            eventListP1R12.set(Integer.parseInt(l.get(1))-1, r1);

        }
///////////////////////
//**************** reorder events 1 and 3
        // add the lines of the Pr1R13 file to the list
        for (String line : linesReorder13) {
            String[] s = line.split("\\*");
            List<DataEvent> list13 = new ArrayList<>();
            list13.add(new DataEvent(s[0]));
            list13.add(new DataEvent(s[1]));
            Collections.sort(list13);

            eventListReorder13.add(list13);
        }

        //reordering events in original stream
        List<List<String>> reorderTimestamps13 = new ArrayList<>();

        for (List<DataEvent> reorderEventPair: eventListReorder13) {
            List<String> list13 =new ArrayList<>();
            for (DataEvent de:reorderEventPair) {
                list13.add(de.getTimestamp());
            }
            reorderTimestamps13.add(list13);
        }

        for (List<String> l: reorderTimestamps13) {
            DataEvent r1 = DataEvent.extractByTimestamp(l.get(0), eventListP1R13);
            DataEvent r2 = DataEvent.extractByTimestamp(l.get(1), eventListP1R13);
            String r1Timestamp=r1.getTimestamp();
            String r2Timestamp=r2.getTimestamp();


            DataEvent.setTimestamp(r1,r2Timestamp);
            DataEvent.setTimestamp(r2,r1Timestamp);

            eventListP1R13.set(Integer.parseInt(l.get(0))-1, r2);
            eventListP1R13.set(Integer.parseInt(l.get(1))-1, r1);

        }
        ///////////////////////
        //**************** reorder events 2 and 3
        // add the lines of the Pr1R23 file to the list
        for (String line : linesReorder23) {
            String[] s = line.split("\\*");
            List<DataEvent> list23 = new ArrayList<>();
            list23.add(new DataEvent(s[0]));
            list23.add(new DataEvent(s[1]));
            Collections.sort(list23);

            eventListReorder23.add(list23);
        }

        //reordering events in original stream
        List<List<String>> reorderTimestamps23 = new ArrayList<>();

        for (List<DataEvent> reorderEventPair: eventListReorder23) {
            List<String> list23 =new ArrayList<>();
            for (DataEvent de:reorderEventPair) {
                list23.add(de.getTimestamp());
            }
            reorderTimestamps23.add(list23);
        }

        for (List<String> l: reorderTimestamps23) {
            DataEvent r1 = DataEvent.extractByTimestamp(l.get(0), eventListP1R23);
            DataEvent r2 = DataEvent.extractByTimestamp(l.get(1), eventListP1R23);
            String r1Timestamp=r1.getTimestamp();
            String r2Timestamp=r2.getTimestamp();


            DataEvent.setTimestamp(r1,r2Timestamp);
            DataEvent.setTimestamp(r2,r1Timestamp);

            eventListP1R23.set(Integer.parseInt(l.get(0))-1, r2);
            eventListP1R23.set(Integer.parseInt(l.get(1))-1, r1);

        }

        ///////////////////////

        //generating event stream from filtered event list for drop1
        DataStream<DataEvent> filteredDrop1EventStream = env.fromCollection(eventListP1D1)
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

        //generating event stream from filtered event list for drop2
        DataStream<DataEvent> filteredDrop2EventStream = env.fromCollection(eventListP1D2)
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

        //generating event stream from filtered event list for drop3
        DataStream<DataEvent> filteredDrop3EventStream = env.fromCollection(eventListP1D3)
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

        //generating event stream from filtered event list for reorder 1,2
        DataStream<DataEvent> filteredReorder12EventStream = env.fromCollection(eventListP1R12)
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

        //generating event stream from filtered event list for REORDER 1,3
        DataStream<DataEvent> filteredReorder13EventStream = env.fromCollection(eventListP1R13)
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

        //generating event stream from filtered event list for Reorder 2,3
        DataStream<DataEvent> filteredReorder23EventStream = env.fromCollection(eventListP1R23)
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


        Pattern<DataEvent, ?> publicPattern = Pattern.<DataEvent>begin("start")
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

        //creating pattern streams for drop
        PatternStream<DataEvent> Pub1Pr1Drop1PatternStream = CEP.pattern(filteredDrop1EventStream,
                publicPattern);
        PatternStream<DataEvent> Pub1Pr1Drop2PatternStream = CEP.pattern(filteredDrop2EventStream,
                publicPattern);
        PatternStream<DataEvent> Pub1Pr1Drop3PatternStream = CEP.pattern(filteredDrop3EventStream,
                publicPattern);

        //creating pattern streams for reorder
        PatternStream<DataEvent> Pub1Pr1Reorder12PatternStream = CEP.pattern(filteredReorder12EventStream,
                publicPattern);
        PatternStream<DataEvent> Pub1Pr1Reorder13PatternStream = CEP.pattern(filteredReorder13EventStream,
                publicPattern);
        PatternStream<DataEvent> Pub1Pr1Reorder23PatternStream = CEP.pattern(filteredReorder23EventStream,
                publicPattern);



        // Detection process for public pattern with drop 1
        DataStream<DataEvent> Pub1Pr1Drop1Stream = Pub1Pr1Drop1PatternStream.process(new PatternProcessFunction<DataEvent, DataEvent>() {

            @Override
            public void processMatch(Map<String, List<DataEvent>> map, Context context, Collector<DataEvent> collector) throws Exception {

                //giving out the matches as a complex event
                DataEvent startEvent = map.get("start").get(0);
                DataEvent endEvent = map.get("end").get(0);
                String eventString = "Timestamp " + endEvent.getTimestamp() + ",Type " + startEvent.getType() + "-" + endEvent.getType() +
                        "(" + startEvent.getTimestamp() + "-" + endEvent.getTimestamp() + ")" + ",Value " + "0" + ",DONumber " +
                        startEvent.getDONumber() + ",ProducerID " + startEvent.getProducerID();
                //System.out.println("complex event : "+eventString);
                collector.collect(new DataEvent(eventString));
            }
        });

        // Detection process for public pattern with drop 2
        DataStream<DataEvent> Pub1Pr1Drop2Stream = Pub1Pr1Drop2PatternStream.process(new PatternProcessFunction<DataEvent, DataEvent>() {

            @Override
            public void processMatch(Map<String, List<DataEvent>> map, Context context, Collector<DataEvent> collector) throws Exception {

                //giving out the matches as a complex event
                DataEvent startEvent = map.get("start").get(0);
                DataEvent endEvent = map.get("end").get(0);
                String eventString = "Timestamp " + endEvent.getTimestamp() + ",Type " + startEvent.getType() + "-" + endEvent.getType() +
                        "(" + startEvent.getTimestamp() + "-" + endEvent.getTimestamp() + ")" + ",Value " + "0" + ",DONumber " +
                        startEvent.getDONumber() + ",ProducerID " + startEvent.getProducerID();
                //System.out.println("complex event : "+eventString);
                collector.collect(new DataEvent(eventString));
            }
        });

        // Detection process for public pattern with drop 3
        DataStream<DataEvent> Pub1Pr1Drop3Stream = Pub1Pr1Drop3PatternStream.process(new PatternProcessFunction<DataEvent, DataEvent>() {

            @Override
            public void processMatch(Map<String, List<DataEvent>> map, Context context, Collector<DataEvent> collector) throws Exception {

                //giving out the matches as a complex event
                DataEvent startEvent = map.get("start").get(0);
                DataEvent endEvent = map.get("end").get(0);
                String eventString = "Timestamp " + endEvent.getTimestamp() + ",Type " + startEvent.getType() + "-" + endEvent.getType() +
                        "(" + startEvent.getTimestamp() + "-" + endEvent.getTimestamp() + ")" + ",Value " + "0" + ",DONumber " +
                        startEvent.getDONumber() + ",ProducerID " + startEvent.getProducerID();
                //System.out.println("complex event : "+eventString);
                collector.collect(new DataEvent(eventString));
            }
        });

        // Detection process for public pattern with reorder 1,2
        DataStream<DataEvent> Pub1Pr1Reorder12Stream = Pub1Pr1Reorder12PatternStream.process(new PatternProcessFunction<DataEvent, DataEvent>() {

            @Override
            public void processMatch(Map<String, List<DataEvent>> map, Context context, Collector<DataEvent> collector) throws Exception {

                //giving out the matches as a complex event
                DataEvent startEvent = map.get("start").get(0);
                DataEvent endEvent = map.get("end").get(0);
                String eventString = "Timestamp " + endEvent.getTimestamp() + ",Type " + startEvent.getType() + "-" + endEvent.getType() +
                        "(" + startEvent.getTimestamp() + "-" + endEvent.getTimestamp() + ")" + ",Value " + "0" + ",DONumber " +
                        startEvent.getDONumber() + ",ProducerID " + startEvent.getProducerID();
                //System.out.println("complex event : "+eventString);
                collector.collect(new DataEvent(eventString));
            }
        });

        // Detection process for public pattern with reorder 1,3
        DataStream<DataEvent> Pub1Pr1Reorder13Stream = Pub1Pr1Reorder13PatternStream.process(new PatternProcessFunction<DataEvent, DataEvent>() {

            @Override
            public void processMatch(Map<String, List<DataEvent>> map, Context context, Collector<DataEvent> collector) throws Exception {

                //giving out the matches as a complex event
                DataEvent startEvent = map.get("start").get(0);
                DataEvent endEvent = map.get("end").get(0);
                String eventString = "Timestamp " + endEvent.getTimestamp() + ",Type " + startEvent.getType() + "-" + endEvent.getType() +
                        "(" + startEvent.getTimestamp() + "-" + endEvent.getTimestamp() + ")" + ",Value " + "0" + ",DONumber " +
                        startEvent.getDONumber() + ",ProducerID " + startEvent.getProducerID();
                //System.out.println("complex event : "+eventString);
                collector.collect(new DataEvent(eventString));
            }
        });

        // Detection process for public pattern with reorder 2,3
        DataStream<DataEvent> Pub1Pr1Reorder23Stream = Pub1Pr1Reorder23PatternStream.process(new PatternProcessFunction<DataEvent, DataEvent>() {

            @Override
            public void processMatch(Map<String, List<DataEvent>> map, Context context, Collector<DataEvent> collector) throws Exception {

                //giving out the matches as a complex event
                DataEvent startEvent = map.get("start").get(0);
                DataEvent endEvent = map.get("end").get(0);
                String eventString = "Timestamp " + endEvent.getTimestamp() + ",Type " + startEvent.getType() + "-" + endEvent.getType() +
                        "(" + startEvent.getTimestamp() + "-" + endEvent.getTimestamp() + ")" + ",Value " + "0" + ",DONumber " +
                        startEvent.getDONumber() + ",ProducerID " + startEvent.getProducerID();
                //System.out.println("complex event : "+eventString);
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


        // write the matched patterns to a text file for drop
        String outputPathFilteredDrop1 = "/home/majidlotfian/flink/flink-quickstart/PLprivacy_Poster/output_folder/1Private-1Public/Run2/Pub1Pr1Drop1.txt";
        String outputPathFilteredDrop2 = "/home/majidlotfian/flink/flink-quickstart/PLprivacy_Poster/output_folder/1Private-1Public/Run2/Pub1Pr1Drop2.txt";
        String outputPathFilteredDrop3 = "/home/majidlotfian/flink/flink-quickstart/PLprivacy_Poster/output_folder/1Private-1Public/Run2/Pub1Pr1Drop3.txt";

        // write the matched patterns to a text file for reorder
        String outputPathFilteredReorder12 = "/home/majidlotfian/flink/flink-quickstart/PLprivacy_Poster/output_folder/1Private-1Public/Run2/Pub1Pr1Reorder12.txt";
        String outputPathFilteredReorder13 = "/home/majidlotfian/flink/flink-quickstart/PLprivacy_Poster/output_folder/1Private-1Public/Run2/Pub1Pr1Reorder13.txt";
        String outputPathFilteredReorder23 = "/home/majidlotfian/flink/flink-quickstart/PLprivacy_Poster/output_folder/1Private-1Public/Run2/Pub1Pr1Reorder23.txt";

        Pub1Pr1Drop1Stream.map(new MapFunction<DataEvent, String>() {
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
                .writeAsText(outputPathFilteredDrop1, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);  // ensure that events are written in order

        Pub1Pr1Drop2Stream.map(new MapFunction<DataEvent, String>() {
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
                .writeAsText(outputPathFilteredDrop2, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);  // ensure that events are written in order

        Pub1Pr1Drop3Stream.map(new MapFunction<DataEvent, String>() {
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
                .writeAsText(outputPathFilteredDrop3, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);  // ensure that events are written in order


        Pub1Pr1Reorder12Stream.map(new MapFunction<DataEvent, String>() {
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
                .writeAsText(outputPathFilteredReorder12, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);  // ensure that events are written in order

        Pub1Pr1Reorder13Stream.map(new MapFunction<DataEvent, String>() {
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
                .writeAsText(outputPathFilteredReorder13, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);  // ensure that events are written in order

        Pub1Pr1Reorder23Stream.map(new MapFunction<DataEvent, String>() {
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
                .writeAsText(outputPathFilteredReorder23, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);  // ensure that events are written in order







        env.execute("EventStreamCEP");


    }

}
