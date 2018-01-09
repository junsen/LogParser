import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogMapper extends Mapper<LongWritable,Text,Text,Text> {
    private Text word=new Text();
    //private final static IntWritable one=new IntWritable(1);

    public void map(LongWritable key,
                    Text value,
                    Context context) throws IOException,InterruptedException{
        //Get the value as a String
        String text=value.toString();
        //Retrieve the date and time out of the log message,first 15 characters
        //String SyslogDateTime=text.substring(0,15);

        System.err.println("We are inside the mapper method. ");
        //Output the syslog date and time as the key ans 1 as the value
        //context.write(new Text(SyslogDateTime),one);
        context.write(new Text(String.valueOf(text.length())),new Text(text));

    }
}


