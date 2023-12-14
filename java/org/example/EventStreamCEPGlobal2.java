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


public class EventStreamCEPGlobal2 {


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


        String producersInputDirectory = "/home/majidlotfian/flink/flink-quickstart/PLprivacy_Poster/input_folder/";

        //creating event list for each producer
        List<DataEvent> eventListP2 = readFileToList(producersInputDirectory,"p2.txt");
        List<DataEvent> eventListP3 = readFileToList(producersInputDirectory,"p3.txt");
        List<DataEvent> eventListP4 = readFileToList(producersInputDirectory,"p4.txt");
        List<DataEvent> eventListP5 = readFileToList(producersInputDirectory,"p5.txt");
        List<DataEvent> eventListP6 = readFileToList(producersInputDirectory,"p6.txt");

        List<DataEvent> UnionAll = new ArrayList<>();
        UnionAll.addAll(eventListP2);
        UnionAll.addAll(eventListP3);
        UnionAll.addAll(eventListP4);
        UnionAll.addAll(eventListP5);
        UnionAll.addAll(eventListP6);
        Collections.sort(UnionAll);

        List<DataEvent> UnionAllReorder = new ArrayList<>();
        UnionAllReorder.addAll(eventListP2);
        UnionAllReorder.addAll(eventListP3);
        UnionAllReorder.addAll(eventListP4);
        UnionAllReorder.addAll(eventListP5);
        UnionAllReorder.addAll(eventListP6);
        Collections.sort(UnionAllReorder);





        String inputDirectoryPub1 = "/home/majidlotfian/flink/flink-quickstart/PLprivacy_Poster/output_folder/5Private-2Public/Global/Run1/publicPattern1/";
        String inputDirectoryPub2 = "/home/majidlotfian/flink/flink-quickstart/PLprivacy_Poster/output_folder/5Private-2Public/Global/Run1/publicPattern2/";

        //creating event list for each file in pub 1
        List<DataEvent> eventListPr1Drop1 = readFileToList(inputDirectoryPub1,"Pr1Drop1.txt");
        List<DataEvent> eventListPr2Drop1 = readFileToList(inputDirectoryPub1,"Pr2Drop1.txt");
        List<DataEvent> eventListPr3Drop1 = readFileToList(inputDirectoryPub1,"Pr3Drop1.txt");
        List<DataEvent> eventListPr4Drop1 = readFileToList(inputDirectoryPub2,"Pr4Drop1.txt");
        List<DataEvent> eventListPr5Drop1 = readFileToList(inputDirectoryPub2,"Pr5Drop1.txt");

        //creating event list for each file in pub 1 for reorder
        List<List<DataEvent>> eventListPr1Reorder = readListFileToList(inputDirectoryPub1,"Pr1Reorder12.txt");
        List<List<DataEvent>> eventListPr2Reorder = readListFileToList(inputDirectoryPub1,"Pr2Reorder12.txt");
        List<List<DataEvent>> eventListPr3Reorder = readListFileToList(inputDirectoryPub1,"Pr3Reorder12.txt");
        List<List<DataEvent>> eventListPr4Reorder = readListFileToList(inputDirectoryPub2,"Pr4Reorder12.txt");
        List<List<DataEvent>> eventListPr5Reorder = readListFileToList(inputDirectoryPub2,"Pr5Reorder12.txt");


        List<DataEvent> mergedDropsAll = new ArrayList<>();
        mergedDropsAll.addAll(eventListPr1Drop1);
        mergedDropsAll.addAll(eventListPr2Drop1);
        mergedDropsAll.addAll(eventListPr3Drop1);
        mergedDropsAll.addAll(eventListPr4Drop1);
        mergedDropsAll.addAll(eventListPr5Drop1);
        Collections.sort(mergedDropsAll);

        List<List<DataEvent>> mergedReorderAll = new ArrayList<>();
        mergedReorderAll.addAll(eventListPr1Reorder);
        mergedReorderAll.addAll(eventListPr2Reorder);
        mergedReorderAll.addAll(eventListPr3Reorder);
        mergedReorderAll.addAll(eventListPr4Reorder);
        mergedReorderAll.addAll(eventListPr5Reorder);



        //removing mergedDrop events from input stream for each public pattern

        List<DataEvent> modifiedListAll = ModifiedListAfterRemovingDroppedEvents(UnionAll,
                mergedDropsAll);
        Collections.sort(modifiedListAll);

        List<DataEvent> modifiedListAllReorder = ModifiedListAfterReorderingEvents(UnionAllReorder, mergedReorderAll);

        DataStream<DataEvent> modifiedStreamAll = createStreamFromList(modifiedListAll, env);
        DataStream<DataEvent> modifiedStreamAllReorder = createStreamFromList(modifiedListAllReorder, env);




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

                }).within(Time.seconds(100));


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

                }).within(Time.seconds(100));
        //pattern.oneOrMore();


        //PROCESSING the merged stream for all public patterns
        String outputDirectoryPub1 = "/home/majidlotfian/flink/flink-quickstart/PLprivacy_Poster/output_folder/5Private-2Public/Global/Run2/publicPattern1/";
        String outputDirectoryPub2 = "/home/majidlotfian/flink/flink-quickstart/PLprivacy_Poster/output_folder/5Private-2Public/Global/Run2/publicPattern2/";


        privatePatternProcess(privatePattern1,
                modifiedStreamAll, outputDirectoryPub1,"Pr1NewComplex.txt");
        privatePatternProcess(privatePattern2,
                modifiedStreamAll, outputDirectoryPub1,"Pr2NewComplex.txt");
        privatePatternProcess(privatePattern3,
                modifiedStreamAll, outputDirectoryPub1,"Pr3NewComplex.txt");
        publicPatternProcess(publicPattern1,
                modifiedStreamAll, outputDirectoryPub1,"Pub1NewComplex.txt");

        privatePatternProcess(privatePattern1,
                modifiedStreamAllReorder, outputDirectoryPub1,"Pr1NewComplexReorder.txt");
        privatePatternProcess(privatePattern2,
                modifiedStreamAllReorder, outputDirectoryPub1,"Pr2NewComplexReorder.txt");
        privatePatternProcess(privatePattern3,
                modifiedStreamAllReorder, outputDirectoryPub1,"Pr3NewComplexReorder.txt");
        publicPatternProcess(publicPattern1,
                modifiedStreamAllReorder, outputDirectoryPub1,"Pub1NewComplexReorder.txt");





        privatePatternProcess(privatePattern2, modifiedStreamAll, outputDirectoryPub2,"Pr2NewComplex.txt");
        privatePatternProcess(privatePattern3, modifiedStreamAll, outputDirectoryPub2,"Pr3NewComplex.txt");
        privatePatternProcess(privatePattern4, modifiedStreamAll, outputDirectoryPub2,"Pr4NewComplex.txt");
        privatePatternProcess(privatePattern5, modifiedStreamAll, outputDirectoryPub2,"Pr5NewComplex.txt");
        publicPatternProcess(publicPattern2, modifiedStreamAll, outputDirectoryPub2,"Pub2NewComplex.txt");

        privatePatternProcess(privatePattern2, modifiedStreamAllReorder, outputDirectoryPub2,"Pr2NewComplexReorder.txt");
        privatePatternProcess(privatePattern3, modifiedStreamAllReorder, outputDirectoryPub2,"Pr3NewComplexReorder.txt");
        privatePatternProcess(privatePattern4, modifiedStreamAllReorder, outputDirectoryPub2,"Pr4NewComplexReorder.txt");
        privatePatternProcess(privatePattern5, modifiedStreamAllReorder, outputDirectoryPub2,"Pr5NewComplexReorder.txt");
        publicPatternProcess(publicPattern2, modifiedStreamAllReorder, outputDirectoryPub2,"Pub2NewComplexReorder.txt");



        env.execute("EventStreamCEP");


    }
    public static List<DataEvent> readFileToList(String directory, String filename) throws IOException {

        String filePath = directory + filename;

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
        publicPatternComplex.print();

        writeDataStreamToFile(publicPatternComplex, directory+filename);

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
    public static List<DataEvent> ModifiedListAfterRemovingDroppedEvents(List<DataEvent> inputList, List<DataEvent> droppedList){

        List<String> droppedTimestamps1 = new ArrayList<>();
        for (DataEvent dataEventDropped: droppedList) {
            droppedTimestamps1.add(dataEventDropped.getTimestamp());
        }

        for (String t: droppedTimestamps1) {
            inputList.remove(DataEvent.extractByTimestamp(t, inputList));
        }

        return inputList;
    }

    public static List<DataEvent> ModifiedListAfterReorderingEvents(List<DataEvent> inputList, List<List<DataEvent>> reorderList){

        //reordering events in original stream


        for (List<DataEvent> l: reorderList) {
            DataEvent r1 = l.get(0);
            DataEvent r2 = l.get(1);
            String r1Timestamp=r1.getTimestamp();
            String r2Timestamp=r2.getTimestamp();

            int r1Index = findIndex(r1,inputList);
            int r2Index = findIndex(r2,inputList);



            DataEvent.setTimestamp(r1,r2Timestamp);
            DataEvent.setTimestamp(r2,r1Timestamp);

            inputList.set(r1Index, r2);
            inputList.set(r2Index, r1);

        }

        return inputList;
    }


    public static int findIndex(DataEvent dataEvent, List<DataEvent> list){
        int index =0;
        for (int i=0; i<list.size();i++){
            if (list.get(i).getTimestamp().equals(dataEvent.getTimestamp())){
                index = i;
                break;
            }
        }



        return index;
    }

    public static DataStream<DataEvent> createStreamFromList(List<DataEvent> list, StreamExecutionEnvironment env){
        DataStream<DataEvent> results = env.fromCollection(list)
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

    public static List<List<DataEvent>> readListFileToList(String directory, String filename) throws IOException {

        String filePath = directory + filename;

        List<List<DataEvent>> eventListReorder = new ArrayList<>();
        // read the contents of the file into a list of strings
        List<String> lines = Files.readAllLines(Paths.get(filePath));

        for (String line : lines) {
            String[] s = line.split("\\*");
            List<DataEvent> list12 = new ArrayList<>();
            list12.add(new DataEvent(s[0]));
            list12.add(new DataEvent(s[1]));
            Collections.sort(list12);

            eventListReorder.add(list12);
        }

        return eventListReorder;
    }


}
