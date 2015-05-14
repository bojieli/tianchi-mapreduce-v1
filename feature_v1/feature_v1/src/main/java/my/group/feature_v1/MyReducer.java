package my.group.feature_v1;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;

/**
 * Reducer模板。请用真实逻辑替换模板内容
 */
public class MyReducer implements Reducer {
    private Record result;

    public void setup(TaskContext context) throws IOException {
        result = context.createOutputRecord();
    }

    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
        Map dict = new HashMap();
    	long item_id = 0;
        long behavior_type = 0;
        boolean buyflag = false;
        while (values.hasNext()) {
            Record val = values.next();
            item_id = val.getBigint("item_id");
            if(item_id == 0L)
            	continue;
            behavior_type = val.getBigint("behavior_type");
            if(behavior_type == 4L)
            {
            	buyflag = true;
            }
            if(!dict.containsKey(item_id)){
            	dict.put(item_id, 1);
            }
        }

        result.setBigint("label", 0L);
        result.setBigint("user_id",key.getBigint("user_id"));
        if(buyflag)
        {
        	for(Object kvp : dict.keySet())
        	{
        		item_id = Long.parseLong(kvp.toString());
        		result.setBigint("item_id", item_id);
        		result.setBigint("cart_nobuyc_1818", 0L);
        		context.write(result);
        	}
        }
        else{
        	for(Object kvp : dict.keySet())
        	{
        		item_id = Long.parseLong(kvp.toString());
        		result.setBigint("item_id", item_id);
        		result.setBigint("cart_nobuyc_1818", 1L);
        		context.write(result);
        	}
        }
    }

    public void cleanup(TaskContext arg0) throws IOException {

    }
}
