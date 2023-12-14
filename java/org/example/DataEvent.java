package org.example;


import sun.awt.X11.XSystemTrayPeer;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class DataEvent implements Comparable<DataEvent>{
    private ArrayList<AttributeValue> data;
    // timestamp is embedded in kafka record;

    public DataEvent(ArrayList<AttributeValue> av){
        this.data = av;
    }
    public DataEvent(){

    }
    public DataEvent(String s){

        this.data = ExtractDataEvent(s).getData();

        //System.out.println("in constructor : "+ DataEventToString(event) );

    }

    public ArrayList<AttributeValue> getData(){return this.data;}


    public void setData(ArrayList<AttributeValue> data) {
        this.data = data;
    }

    public void addData(ArrayList<AttributeValue> data, AttributeValue av){
        data.add(av);
    }

    public String DataEventToString(DataEvent dataEvent){
        String attvalue = "";
        for (int i=0; i<dataEvent.data.size(); i++){
            attvalue += data.get(i).getAttributeName() + " " + data.get(i).getAttributeValue() + ",";
        }
        return attvalue;
    }

    public String getTimestamp(){

        for (AttributeValue av:this.getData()) {
            if (av.getAttributeName().equals("Timestamp")){
                return av.getAttributeValue();

            }
        }
        return "Not Found";
    }
    public String getDONumber(){

        for (AttributeValue av:this.getData()) {
            if (av.getAttributeName().equals("DONumber")){
                return av.getAttributeValue();

            }
        }
        return "Not Found";
    }

    public String getProducerID(){

        for (AttributeValue av:this.getData()) {
            if (av.getAttributeName().equals("ProducerID")){
                return av.getAttributeValue();

            }
        }
        return "Not Found";
    }

    public String getType(){

        for (AttributeValue av:this.getData()) {
            if (av.getAttributeName().equals("Type")){
                return av.getAttributeValue();

            }
        }
        return "Not Found";
    }

    public String getValue(){

        for (AttributeValue av:this.getData()) {
            if (av.getAttributeName().equals("Value")){
                return av.getAttributeValue();

            }
        }
        return "Not Found";
    }

    public static DataEvent ExtractDataEvent(String string) {
        String[] attValPairs = string.split(",");
        ArrayList<AttributeValue> al=new ArrayList<AttributeValue>();
        AttributeValue av=null;

        //System.out.println("first arg : "+args[0]);

        for (String pair:attValPairs
        ) {
            //System.out.println(s1 + " ");
            //args2[0] is att and args2[1] is value
            String[] attVal = pair.split("\\s+");


            int i =0;
            for (String str:attVal
            ) {

                if (i == 1) {
                    av.setAttributeValue(str);
                    al.add(av);
                    //System.out.println(s1 + "  if ");
                    i--;
                } else {
                    av = new AttributeValue();
                    av.setAttributeName(str);
                    //System.out.println(s1 + "  else");
                    i++;
                }
            }
        }
        //System.out.println(al.toString());
        DataEvent dataEvent=new DataEvent(al);
        //System.out.println(dataEvent.getData().get(2).getAttributeName() + dataEvent.getData().get(2).getAttributeValue());
        //System.out.println("in extract method "+DataEventToString(dataEvent));

        return dataEvent;
    }

    /*
    public String CumulativeUncertainty(ArrayList<DataEvent> deList){
        ArrayList<Double> uncertaintyValues = new ArrayList<>();
        for (DataEvent de:deList) {
            System.out.println("event in cumulative : "+de);

            uncertaintyValues.add(1-(Math.abs(Double.parseDouble(de.getValue())-Double.parseDouble(de.getGroundTruth()))/de.extractProducer(de.getProducerID()).getRange()));
        }

        double result = 0;
        for (double d:uncertaintyValues) {
            result += d;
        }
        return String.valueOf(result/uncertaintyValues.size());
    }

     */


    public String extractProducer(String pID){
        for (String p:EventStreamCEP.producerList) {
            if (p.equals(pID)){
                return p;
            }
        }
        return null;
    }

    @Override
    public String toString(){
        String attvalue = "";
        for (int i=0; i<this.data.size(); i++){
            attvalue += data.get(i).getAttributeName() + " " + data.get(i).getAttributeValue() + " ";
        }
        return attvalue;
    }

    public static DataEvent extractByTimestamp(String timestamp, List<DataEvent> list){
        DataEvent result = list.get(0);
        for (DataEvent d:list) {
            if (d.getTimestamp().equals(timestamp)){
                result = d;
            }

        }
        return result;
    }
    public static void setTimestamp(DataEvent dataEvent, String t){
        List<AttributeValue> data = dataEvent.getData();

        for (AttributeValue av: data){
            if (av.getAttributeName().equals("Timestamp")){
                av.setAttributeValue(t);
            }
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(data);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DataEvent) {
            DataEvent other = (DataEvent) obj;

            return data.containsAll(other.data);
        }
        return false;
    }

    @Override
    public int compareTo(DataEvent dataEvent) {
        return Integer.parseInt(this.getTimestamp()) - Integer.parseInt(dataEvent.getTimestamp());
    }
}

// previous definition of data event

/*
public class DataEvent {
    private long timestamp;

    private String type;

    private double value;

    private int doNumber;


    public DataEvent( long timestamp, String type, double value, int doNumber) {
        this.timestamp = timestamp;
        this.type = type;
        this.value = value;
        this.doNumber=doNumber;
    }

    public DataEvent(){
    }

    public DataEvent(String eventString){
        String[] attributeValuePairs = eventString.split(",");

        //splitting the attribute values pairs
        String[] timestampAttVal = attributeValuePairs[0].split("\\s+");


        this.timestamp=Long.parseLong(s[0]);
        this.type = s[1];
    }

    public String getType() {
        return type;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "DataEvent{" +
                "timestamp=" + timestamp +
                ", type='" + type + '\'' +
                '}';
    }
}

 */
