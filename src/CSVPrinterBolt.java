package storm.starter;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import storm.starter.tools.Rankable;
import storm.starter.tools.Rankings;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by fil on 09/12/2014.
 */


//Printer bolt which outputs a csv file
public class CSVPrinterBolt extends BaseRichBolt {

    PrintWriter writer;
    int count = 0;
    private OutputCollector _collector;
    private String filename;
    private DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
    private int windowLength;

    public CSVPrinterBolt(String filename, int windowLength) {
        this.filename = filename;
        this.windowLength = windowLength;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
        try {
            writer = new PrintWriter(filename, "UTF-8");
        } catch (FileNotFoundException e) {
            e.printStackTrace(); //To change body of catch statement use File | Settings | File Templates.
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace(); //To change body of catch statement use File | Settings | File Templates.
        }

        //CSV header
        writer.println("Timestamp,Object,Count,Window Length");
    }



    @Override
    public void execute(Tuple tuple) {

        Date date = new Date();
        Object temp = tuple.getValue(0);
        Rankings ranks = (Rankings) tuple.getValue(0);

        //prints timestamp, object, count, windowlength in csv type format
        if(ranks.size() != 0) {

            List<Rankable> list;
            list = ranks.getRankings();
            for (Rankable r : list) {
                writer.println(dateFormat.format(date) + "," +(String) r.getObject() + "," + r.getCount() + "," + windowLength);
            }
            writer.flush();
        }

        // Confirm that this tuple has been treated.
        _collector.ack(tuple);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @Override
    public void cleanup() {
        writer.close();
        super.cleanup();
    }
}