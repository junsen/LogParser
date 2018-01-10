import java.io.IOException;
import java.util.*;
import java.util.jar.Attributes;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.internal.InternalUtils;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

public class LogMapper extends Mapper<LongWritable,Text,Text,Text> {

    public void map(LongWritable key,
                    Text value,
                    Context context) throws IOException, InterruptedException {
        //Get the value as a String
        String line = value.toString();
        //Retrieve the date and time out of the log message,first 15 characters
        //String SyslogDateTime=text.substring(0,15);

        System.err.println("We are inside the mapper method. ");
        //Output the syslog date and time as the key ans 1 as the value
        //context.write(new Text(SyslogDateTime),one);
        //context.write(new Text(String.valueOf(text.length())), new Text(text));
        transform("trademark",parseDynamodbJson(line),context);

    }

    private Map<String, AttributeValue> parseDynamodbJson(String line) {
        Map<String, AttributeValue> mapAttributeValue = null;
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNodeRoot = objectMapper.readTree(line);
            Item item = Item.fromJSON(jsonNodeRoot.get("item").get("dynamodb").get("newImage").toString());

            mapAttributeValue = InternalUtils.toAttributeValues(item);

        } catch (Exception e) {
            System.err.println(e.getStackTrace());
        }
        return mapAttributeValue;
    }

    private void transform(String tableName,
                           Map<String, AttributeValue> rootAttributeValue,Context context)
                            throws IOException, InterruptedException {
        StringBuilder stringBuilder = new StringBuilder();
        Map<String, AttributeValue> listKeyValueMap = new HashMap();
        Map<String, AttributeValue> mapKeyValueMap = new HashMap();
        for (Map.Entry<String, AttributeValue> entry : rootAttributeValue.entrySet()) {
            if (entry.getValue().getM() != null) {
                Map<String, AttributeValue> attributeValueMap = entry.getValue().getM();
                for (Map.Entry<String, AttributeValue> subEntry : attributeValueMap.entrySet()) {
                    if (DynamodbJsonUtil.IsList(subEntry)) {
                        listKeyValueMap.put(tableName + "." + entry.getKey(), subEntry.getValue());
                    } else if (DynamodbJsonUtil.IsMap(subEntry)) {
                        mapKeyValueMap.put(tableName + "." + entry.getKey(), subEntry.getValue());
                    } else if (DynamodbJsonUtil.IsString(subEntry)) {
                        stringBuilder.append("\"" + entry.getKey() + "\"");
                        stringBuilder.append(":");
                        stringBuilder.append("\"" + subEntry.getValue().getS() + "\"");
                        stringBuilder.append(",");
                    } else if (DynamodbJsonUtil.IsNumber(subEntry)) {
                        stringBuilder.append("\"" + entry.getKey() + "\"");
                        stringBuilder.append(":");
                        stringBuilder.append(subEntry.getValue().getS());
                        stringBuilder.append(",");
                    } else {
                        stringBuilder.append("\"" + entry.getKey() + "\"");
                        stringBuilder.append(":");
                        stringBuilder.append("\"" + subEntry.getValue().getS() + "\"");
                        stringBuilder.append(",");
                    }
                }
            }
        }
        context.write(new Text(tableName),new Text(stringBuilder.toString()));

        for (Map.Entry<String, AttributeValue> listEntry : listKeyValueMap.entrySet()) {
            List<AttributeValue> arrayList=listEntry.getValue().getL();
            //get the Enumeration object
            Enumeration e = Collections.enumeration(arrayList);

            while(e.hasMoreElements()) {
                String line=TraversalMap(listEntry.getKey(),((AttributeValue) e.nextElement()).getM());
                context.write(new Text(listEntry.getKey()),new Text(line));
            }
        }

        for (Map.Entry<String, AttributeValue> mapEntry : mapKeyValueMap.entrySet()) {
            transform(mapEntry.getKey(), mapEntry.getValue().getM(),context);
        }



    }

    private String TraversalMap(String tablename,Map<String, AttributeValue> keyValueMap){
        StringBuilder stringBuilder=new StringBuilder();
        for (Map.Entry<String, AttributeValue> entry : keyValueMap.entrySet()) {
            Map<String, AttributeValue> attributeValueMap=entry.getValue().getM();
            for(Map.Entry<String,AttributeValue> subEntry :attributeValueMap.entrySet()) {
                Map<String, AttributeValue> subAttributeValueMap=subEntry.getValue().getM();
                for(Map.Entry<String,AttributeValue> thirdEntry :subAttributeValueMap.entrySet()) {
                    if(DynamodbJsonUtil.IsString(thirdEntry)){
                        stringBuilder.append("\"" + subEntry.getKey() + "\"");
                        stringBuilder.append(":");
                        stringBuilder.append("\"" + thirdEntry.getValue().getS() + "\"");
                        stringBuilder.append(",");
                    }
                    else if(DynamodbJsonUtil.IsNumber(thirdEntry)){
                        stringBuilder.append("\"" + subEntry.getKey() + "\"");
                        stringBuilder.append(":");
                        stringBuilder.append(thirdEntry.getValue().getS());
                        stringBuilder.append(",");
                    }
                }

            }
        }
        return stringBuilder.toString();
    }

}


