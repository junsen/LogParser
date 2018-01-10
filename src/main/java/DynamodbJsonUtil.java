import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import java.util.Map;

public class DynamodbJsonUtil {
    private static final String KEY_TYPE_LIST = "l";
    private static final String KEY_TYPE_MAP = "m";
    private static final String KEY_TYPE_STRING = "s";
    private static final String KEY_TYPE_NUMBER = "n";

    public static boolean IsNumber(Map.Entry<String,AttributeValue> entry){
        return entry.getKey().equalsIgnoreCase(KEY_TYPE_NUMBER)?true:false;
    }
    public static boolean IsString(Map.Entry<String, AttributeValue> entry) {
        return entry.getKey().equalsIgnoreCase(KEY_TYPE_STRING)?true:false;
    }

    public static boolean IsMap(Map.Entry<String, AttributeValue> entry) {
        return entry.getKey().equalsIgnoreCase(KEY_TYPE_MAP) ? true : false;
    }

    public static boolean IsList(Map.Entry<String, AttributeValue> entry) {
        return entry.getKey().equalsIgnoreCase(KEY_TYPE_LIST) ? true : false;
    }

    public static String AttributeValueMapToJson(Map<String, AttributeValue> attributeValueMap) {
        StringBuilder stringBuilder = new StringBuilder();
        for (Map.Entry<String, AttributeValue> entry : attributeValueMap.entrySet()) {
            stringBuilder.append("\"" + entry.getKey() + "\"");
            stringBuilder.append(":");
            if (entry.getKey().equalsIgnoreCase(KEY_TYPE_STRING)){
                stringBuilder.append("\"" + entry.getValue().getS() + "\"");
            }
            else if(entry.getKey().equalsIgnoreCase(KEY_TYPE_NUMBER)){
                stringBuilder.append(entry.getValue());
            }

            stringBuilder.append(",");
        }
        return stringBuilder.toString();
    }

}
