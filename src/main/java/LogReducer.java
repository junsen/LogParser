import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class LogReducer extends Reducer<Text,Text,Text,Text>{

    private MultipleOutputs outputs;

    @Override
    protected void setup(Context context) throws IOException,InterruptedException{
        outputs=new MultipleOutputs(context);
    }

    @Override
    protected void reduce(Text key,Iterable<Text> values,
                       Context context) throws IOException,InterruptedException{
        int count=0;
        //Counts the occurrences of the date and time
        for(Text val:values){
            //count+=val.get();
            outputs.write(key,val,key.toString());
        }
//        if (key.toString().contains("a")){
//            outputs.write("aa",key,new IntWritable(count));
//        }
//        else if(key.toString().contains("b")){
//            outputs.write("b",key,new IntWritable(count));
//        }
        //System.err.println(String.format("We are inside the reduce method. count:%d",count));
        //Output the date and time with its count
        //context.write(key,new IntWritable(count));

    }

    @Override
    protected void cleanup(Context context) throws IOException,InterruptedException{
        outputs.close();
    }


}
