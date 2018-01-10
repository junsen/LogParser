import java.io.IOException;

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
        for(Text val:values){
            outputs.write(key,val,key.toString());
        }


    }

    @Override
    protected void cleanup(Context context) throws IOException,InterruptedException{
        outputs.close();
    }


}
