package my.group.featureCross;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Mapper;

import java.io.IOException;

/**
 * Mapper模板。请用真实逻辑替换模板内容
 */
public class MyMapper implements Mapper {
    private Record key;
    private Record value;

    public void setup(TaskContext context) throws IOException {
        key = context.createMapOutputKeyRecord();
        value = context.createMapOutputValueRecord();
    }

    public void map(long recordNum, Record record, TaskContext context) throws IOException {
        //set key
    	key.setBigint("user_id", record.getBigint("user_id"));
    	key.setBigint("item_id", record.getBigint("item_id"));
    	key.setBigint("labelday", record.getBigint("labelday"));
    	
    	//set value
    	value.setBigint("iscartnobuy6h", record.getBigint("iscartnobuy6h"));
    	value.setBigint("isbuycate24h", record.getBigint("isbuycate24h"));
    	value.setBigint("isdouble12buy", record.getBigint("isdouble12buy"));
    	
    	value.setDouble("ui_bro_decay_cnt", record.getDouble("ui_bro_decay_cnt"));
    	value.setDouble("ui_fav_decay_cnt", record.getDouble("ui_fav_decay_cnt"));
    	value.setDouble("ui_cart_decay_cnt", record.getDouble("ui_cart_decay_cnt"));
    	value.setDouble("ui_buy_decay_cnt", record.getDouble("ui_buy_decay_cnt"));
    	
    	context.write(key, value);
    	
    	context.progress();
    }

    public void cleanup(TaskContext context) throws IOException {

    }
}