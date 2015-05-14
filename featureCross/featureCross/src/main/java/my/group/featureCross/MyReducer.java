package my.group.featureCross;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Reducer模板。请用真实逻辑替换模板内容
 */
public class MyReducer implements Reducer {
    private Record result;

    public void setup(TaskContext context) throws IOException {
        result = context.createOutputRecord();
    }

    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
        //set key
    	result.setBigint("user_id", key.getBigint("user_id"));
    	result.setBigint("item_id", key.getBigint("item_id"));
    	result.setBigint("labelday", key.getBigint("labelday"));
    	
    	//cross features  	
        long iscartnobuy6h = 0L, isbuycate24h = 0L, isdouble12buy = 0L;
        double ui_bro_decay_cnt = 0d, ui_fav_decay_cnt = 0d, ui_cart_decay_cnt = 0d, ui_buy_decay_cnt = 0d;
    	while(values.hasNext())
        {
        	Record val = values.next();
        	
        	iscartnobuy6h = val.getBigint("iscartnobuy6h");
        	isbuycate24h = val.getBigint("isbuycate24h");
        	isdouble12buy = val.getBigint("isdouble12buy");
        	
        	ui_bro_decay_cnt = val.getDouble("ui_bro_decay_cnt");
        	ui_fav_decay_cnt = val.getDouble("ui_fav_decay_cnt");
        	ui_cart_decay_cnt = val.getDouble("ui_cart_decay_cnt");
        	ui_buy_decay_cnt = val.getDouble("ui_buy_decay_cnt");
        }
    	
    	result.setBigint("iscartnobuy6h_isbuycate24h", iscartnobuy6h * isbuycate24h);
    	result.setDouble("isdouble12buy_ui_bro_decay_cnt", isdouble12buy * ui_bro_decay_cnt);
    	result.setDouble("isdouble12buy_ui_fav_decay_cnt", isdouble12buy * ui_fav_decay_cnt);
    	result.setDouble("isdouble12buy_ui_cart_decay_cnt", isdouble12buy * ui_cart_decay_cnt);
    	result.setDouble("isdouble12buy_ui_buy_decay_cnt", isdouble12buy * ui_buy_decay_cnt);
    	
    	
        context.write(result);
        
        context.progress();
    }

    public void cleanup(TaskContext arg0) throws IOException {

    }
}
