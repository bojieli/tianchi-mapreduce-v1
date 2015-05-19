package my.group.user_item_cate_day_o2o;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Mapper;

import java.io.IOException;

/**
 * Mapper模板。请用真实逻辑替换模板内容
 */
public class MyMapper implements Mapper {
    private Record key;
    private Record value;
    private Record value_label;
    private final int day_cnt = 32;
    private final int windowsize = 10, slidedays = 17;

    public void setup(TaskContext context) throws IOException {
    	key = context.createMapOutputKeyRecord();
    	value = context.createMapOutputValueRecord();
    	value_label = context.createMapOutputValueRecord();
    }
    
    //initialize values
    private void initRecord(Record outputValue)
    {
    	outputValue.setBigint("ui_bro", 0L);
    	outputValue.setBigint("ui_fav", 0L);
    	outputValue.setBigint("ui_cart", 0L);
    	outputValue.setBigint("ui_buy", 0L);
    	outputValue.setBigint("ui_label_buy", 0L);
    	
    	outputValue.setString("geohash", "");
    	outputValue.setBigint("curday", 0L);
    	outputValue.setBigint("curhour", 0L);
    }
    
    //labelday - free features.
    private void featureEngineering(Record originalRecord, Record outputValue)
    {
    	String days = originalRecord.getString("days");
    	long curhour = originalRecord.getBigint("hours");
    	String geohash = originalRecord.getString("user_geohash");
    	String act_type = originalRecord.getString("behavior_type");
    	
    	//convert type.
    	int iact = Integer.parseInt(act_type);
    	long curday = Long.parseLong(days);
    	
    	switch(iact)
    	{
    	//add browse action features here
    	case 1:
    		outputValue.setBigint("ui_bro", 1L);
    		break;
    	//add favorite action features here
    	case 2:
    		outputValue.setBigint("ui_fav", 1L);
    		break;
    	//add cart action features here
    	case 3:
    		outputValue.setBigint("ui_cart", 1L);
    		break;
    	//add buy action features here
    	case 4:
    		outputValue.setBigint("ui_buy", 1L);
    		break;
    	default:
    		break;
    	}

    	outputValue.setString("geohash", geohash);
    	outputValue.setBigint("curday", curday);
    	outputValue.setBigint("curhour", curhour);
    	
    	//add your feature here, using the orginalRecord, and write the output to outputValue
    }
    
    //labelday - related features.
    private void featureEngineering(Record originalRecord, Record outputValue, int labelday)
    {
    	// add your other features here, using the orginalRecord, and write the output to outputValue
    }

    public void map(long recordNum, Record record, TaskContext context) throws IOException {
    	//initialize values.
    	initRecord(value);
    	
    	//get informations.
    	long user_id = record.getBigint("user_id");
    	long item_id = record.getBigint("item_id");
    	long cate_id = record.getBigint("item_category");
    	String days = record.getString("days");
    	String act_type = record.getString("behavior_type");
    	
    	//convert type.
    	int iact = Integer.parseInt(act_type);
    	int curday = Integer.parseInt(days);
    	
    	//set key record.
    	key.setBigint("user_id", user_id);
    	key.setBigint("item_id", item_id);
    	key.setBigint("cate_id", cate_id);
    	
    	//!!---add your features map logic here.--!!
    	
    	//write your labelday-free features.
    	featureEngineering(record, value);
    	
    	//slide window generate features.
    	int labelday = day_cnt;
    	int slide_cnt = 0;
    	while(slide_cnt < slidedays)
    	{
    		labelday--;
    		//skip 12.10, 12.11, 12.12, 12.13.
    		if(labelday < 26 && labelday > 21)
    			continue;
    		
    		int firstday = labelday - windowsize;
    		//if current action day in window
    		if(curday < labelday && curday >= firstday)
    		{
    			key.setBigint("labelday", (long)labelday);
    			
    			//!!---add your features map logic here.--!!
    			
    			//write your labelday-related features.
    			featureEngineering(record, value, labelday);
    			
    			//map output key - value
    			context.write(key, value);
    		}
    		
    		//ui label
    		if(curday == labelday && iact == 4)
    		{
    			key.setBigint("labelday", (long)labelday);
    			initRecord(value_label);
    			value_label.setBigint("ui_label_buy", 1L);
    			context.write(key, value_label);
    		}
    		slide_cnt++;
    	}
    	
    	//heart beat.
    	context.progress();
    }

    public void cleanup(TaskContext context) throws IOException {
    	context.progress();
    }
}



