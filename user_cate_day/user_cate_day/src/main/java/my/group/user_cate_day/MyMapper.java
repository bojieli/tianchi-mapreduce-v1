package my.group.user_cate_day;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Mapper;

import java.io.IOException;

/**
 * Mapper模板。请用真实逻辑替换模板内容
 */
public class MyMapper implements Mapper {
    private Record key;
    private Record value;
    private final int day_cnt = 32;
    private int windowsize, slidedays;

    public void setup(TaskContext context) throws IOException {
    	key = context.createMapOutputKeyRecord();
    	value = context.createMapOutputValueRecord();
    	
    	windowsize = 10;
    	slidedays = 7;
    }
    
    //initialize values
    private void initRecord(Record outputValue)
    {
    	outputValue.setBigint("uc_bro", 0L);
    	outputValue.setBigint("uc_fav", 0L);
    	outputValue.setBigint("uc_cart", 0L);
    	outputValue.setBigint("uc_buy", 0L);
    	
    	outputValue.setString("geohash", "");
    	outputValue.setBigint("curday", 0L);
    	outputValue.setBigint("curhour", 0L);
    }
    
    //labelday - free features.
    private void featureEngineering(Record originalRecord, Record outputValue)
    {
    	String days = originalRecord.getString("days");
    	long curhour = originalRecord.getBigint("hours");
    	String act_type = originalRecord.getString("behavior_type");
    	String geohash = originalRecord.getString("user_geohash");
    	
    	//convert type.
    	int iact = Integer.parseInt(act_type);
    	long curday = Long.parseLong(days);
    	
    	switch(iact)
    	{
    	//add browse action features here
    	case 1:
    		outputValue.setBigint("uc_bro", 1L);
    		break;
    	//add favorite action features here
    	case 2:
    		outputValue.setBigint("uc_fav", 1L);
    		break;
    	//add cart action features here
    	case 3:
    		outputValue.setBigint("uc_cart", 1L);
    		break;
    	//add buy action features here
    	case 4:
    		outputValue.setBigint("uc_buy", 1L);
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
    	long cate_id = record.getBigint("item_category");
    	String days = record.getString("days");
    	
    	//convert type.
    	int curday = Integer.parseInt(days);
    	
    	//set key record.
    	key.setBigint("user_id", user_id);
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
    		slide_cnt++;
    	}
    	
    	//heart beat.
    	context.progress();
    }

    public void cleanup(TaskContext context) throws IOException {
    	context.progress();
    }
}



