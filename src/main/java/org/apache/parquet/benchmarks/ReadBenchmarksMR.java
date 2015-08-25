package org.apache.parquet.benchmarks;

import static java.lang.Thread.sleep;
//import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import  org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.parquet.Log;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.example.ExampleInputFormat;
import org.apache.parquet.hadoop.example.GroupReadSupport;

public class ReadBenchmarksMR extends Configured implements Tool {
  private static final Log LOG = Log.getLog(ReadBenchmarksMR.class);

//  
//    private static class FieldDescription {
//	public String constraint;
//	public String type;
//	public String name;
//    }

//    private static class RecordSchema {
//	public RecordSchema(String message) {
//	    fields = new ArrayList<FieldDescription>();
//	    List<String> elements = Arrays.asList(message.split("\n"));
//	    Iterator<String> it = elements.iterator();
//	    while(it.hasNext()) {
//		String line = it.next().trim().replace(";", "");;
//		System.err.println("RecordSchema read line: " + line);
//		if(line.startsWith("optional") || line.startsWith("required")) {
//		    String[] parts = line.split(" ");
//		    FieldDescription field = new FieldDescription();
//		    field.constraint = parts[0];
//		    field.type = parts[1];
//		    field.name = parts[2];
//		    fields.add(field);
//		}
//	    }
//	}
//	private List<FieldDescription> fields;
//	public List<FieldDescription> getFields() {
//	    return fields;
//	}
//    }
    
//    public static class ReadRequestReduce extends Reducer<Text, Iterator<LongWritable>, Text, LongWritable> {
//    	 public void reduce(Text key, Iterator<LongWritable> values, Context context, Reporter report) throws IOException{ 
//             
//             //key就是map后的那个domain，values是map后的一个个url集合，output是reduce后的结果输出 
//             //把每一个url用\n连起来 
//    		 Long count = 0l;
//             while(values.hasNext()){ 
//                 count +=  values.next().get();
//             } 
//              
//             try {
//				context.write(key, new LongWritable(count));
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//         } 
//    	
//    }

    /*
     * Read a Parquet record
     */
    public static class ReadRequestMap extends Mapper<LongWritable, Group, Text, LongWritable> {
//	private static List<FieldDescription> expectedFields = null;
	public static int counter = 0;
	public static String lastID = null;
        @Override
	public void map(LongWritable key, Group value, Context context) throws IOException, InterruptedException {
	    NullWritable outKey = NullWritable.get();
//	    LOG.error("############### Value: " + value);
//	    LOG.error("&&&&&&&&&&&&& userid: " +value.getString("user_id",0));
//	    if(expectedFields == null) {
//		// Get the file schema which may be different from the fields in a particular record) from the input split
//		String fileSchema = ((ParquetInputSplit)context.getInputSplit()).getFileSchema();
//		// System.err.println("file schema from context: " + fileSchema);
//		RecordSchema schema = new RecordSchema(fileSchema);
//		expectedFields = schema.getFields();
//		//System.err.println("inferred schema: " + expectedFields.toString());
//	    }
//	    System.out.println(value);
//	    
//	    String cityCode = value.getString("user_id",0);
//	    if(cityCode == null || cityCode.equals("")) System.out.println(cityCode);
//	    else{
//	    	counter++;
//	    	lastID = cityCode;
//	    }
//	    lastID = value.toString();
//	    // No public accessor to the column values in a Group, so extract them from the string representation
//	    String line = value.toString();
//	    String[] fields = line.split("\n");
//
//            StringBuilder csv = new StringBuilder();
//	    boolean hasContent = false;
//	    int i = 0;
//	    // Look for each expected column
//	    Iterator<FieldDescription> it = expectedFields.iterator();
//	    while(it.hasNext()) {
//		if(hasContent ) {
//		    csv.append(',');
//		}
//		String name = it.next().name;
//		if(fields.length > i) {
//		    String[] parts = fields[i].split(": ");
//		    // We assume proper order, but there may be fields missing
//		    if(parts[0].equals(name)) {
//			boolean mustQuote = (parts[1].contains(",") || parts[1].contains("'"));
//			if(mustQuote) {
//			    csv.append('"');
//			}
//			csv.append(parts[1]);
//			if(mustQuote) {
//			    csv.append('"');
//			}
//			hasContent = true;
//			i++;
//		    }
//		}
//	    }
            System.out.println(value);
	    String temp = value.getString("user_id",0);
//	    context.write(new Text(value.getString("user_id",0)), new LongWritable(1));
        }
    }

    public int run(String[] args) throws Exception {
    	LOG.error("BBBBBBBBBBBBBBBBBBBBBegin : " + System.currentTimeMillis());
	getConf().set("mapred.textoutputformat.separator", ",");
        StringBuilder schemaString  = new StringBuilder("message sample { required binary ip; }");
//        MessageType schema  = MessageTypeParser.parseMessageType(schemaString.toString());
//        GroupReadSupport.ReadContext(schema, getConf());
        getConf().set(GroupReadSupport.PARQUET_READ_SCHEMA, schemaString.toString());
        Job job = new Job(getConf());
        job.setJarByClass(getClass());
        job.setJobName(getClass().getName());

	job.setMapOutputKeyClass(LongWritable.class);
	job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(ReadRequestMap.class);
	job.setNumReduceTasks(0);
        GroupReadSupport groupReadSupport = new GroupReadSupport();
//        groupReadSupport.
//        job.setReducerClass(ReadRequestReduce.class);
//        ParquetInputFormat<Group> parquetInputFormat = new ParquetInputFormat<Group>(GroupReadSupport.class);
	job.setInputFormatClass(ExampleInputFormat.class);
//        ExampleInputFormat.
//        job.setInputFormatClass(ParquetInputFormat<Group>.class);
//	TextOutputFormat.setOutputPath(job, outputDir);
	job.setOutputFormatClass(TextOutputFormat.class);

//	job.setOutputFormatClass(ExampleOutputFormat.class);
//	CompressionCodecName codec = CompressionCodecName.UNCOMPRESSED;
//	   codec = CompressionCodecName.SNAPPY;
//	
//	LOG.info("Output compression: " + codec);
//	ExampleOutputFormat.setCompression(job, codec);
	

	FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    	LOG.error("EEEEEEEEEEEEEEEEEEEEEEEnd : " + System.currentTimeMillis());

        return 0;
    }

    public static void main(String[] args) throws Exception {
        try {
        	System.out.println("Begin Time:  " + System.currentTimeMillis());
            int res = ToolRunner.run(new Configuration(), new ReadBenchmarksMR(), args);
        	System.out.println("End Time:  " + System.currentTimeMillis());

            System.out.println("******counter:     " + ReadRequestMap.counter);
            System.out.println("******lastID:     " + ReadRequestMap.lastID);
            System.exit(res);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(255);
        }
    }
}
