package my.group.feature_v1;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Mapper;

import java.io.IOException;

/**
 * Mapper模板。请用真实逻辑替换模板内容
 */
public class MyMapper implements Mapper {
    private Record ucpair;
    //<item, behavior_tpye> value
    private Record ib_value;

    public void setup(TaskContext context) throws IOException {
    	ucpair = context.createMapOutputKeyRecord();
    	ib_value = context.createMapOutputValueRecord();
    }

    public void map(long recordNum, Record record, TaskContext context) throws IOException {
    	String behavior_type = record.getString("behavior_type");
    	ucpair.setBigint("user_id", record.getBigint("user_id"));
    	ucpair.setBigint("item_category", record.getBigint("item_category"));
    	long hours = record.getBigint("hours");
    	if(hours >= 18){
    		ib_value.setBigint("item_id", record.getBigint("item_id"));
    		ib_value.setBigint("behavior_type", behavior_type.equals("3")? 3L:4L);
    	}
    	else{
    		ib_value.setBigint("item_id", 0L);
    		ib_value.setBigint("behavior_type", 0L);
    	}
    	
    	context.write(ucpair, ib_value);
    }

    public void cleanup(TaskContext context) throws IOException {

    }
}