package my.group.feature_v1;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.HashMap;
import java.util.Map;

/**
 * Combiner模板。请用真实逻辑替换模板内容
 */
public class MyCombiner implements Reducer {
    private Record ib_value;

    public void setup(TaskContext context) throws IOException {
    	ib_value = context.createMapOutputValueRecord();
    }

    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
//        Map dict = new HashMap();
//        long item_id = 0;
//        long behavior_type = 0;
//    	while (values.hasNext()) {
//            Record val = values.next();
//            item_id = val.getBigint("item_id");
//            behavior_type = val.getBigint("behavior_type");
//            if(dict.containsKey(item_id))
//            {
//            	
//            	
//            }
//        }
//        count.set(0, c);
//        context.write(key, count);
    }

    public void cleanup(TaskContext arg0) throws IOException {

    }
}
