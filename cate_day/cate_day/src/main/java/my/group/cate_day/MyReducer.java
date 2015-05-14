package my.group.cate_day;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Reducer模板。请用真实逻辑替换模板内容
 */
public class MyReducer implements Reducer {
    private Record result;
    private final int bro_type = 1, fav_type = 2, cart_type = 3, buy_type = 4;

    public void setup(TaskContext context) throws IOException {
    	result = context.createOutputRecord();
    }

    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
    	int labelday = key.getBigint("labelday").intValue();
    	//output key
    	result.setBigint("item_category", key.getBigint("cate_id"));
    	result.setBigint("labelday", (long)labelday);
    	
    	//!!---add your features count logic here.--!!
    	
    	//action num counting variable
    	double bro_decay_cnt = 0d, fav_decay_cnt = 0d, cart_decay_cnt = 0d, buy_decay_cnt = 0d;
    	
    	while(values.hasNext())
    	{
    		Record val = values.next();
    		int idays = val.getBigint("curday").intValue();
    		
    		//count action num
    		bro_decay_cnt += val.getBigint("c_bro") * decay(idays, labelday, bro_type) * double12(idays, bro_type);
    		fav_decay_cnt += val.getBigint("c_fav") * decay(idays, labelday, fav_type) * double12(idays, fav_type);
    		cart_decay_cnt += val.getBigint("c_cart") * decay(idays, labelday, cart_type) * double12(idays, cart_type);
    		buy_decay_cnt += val.getBigint("c_buy") * decay(idays, labelday, buy_type) * double12(idays, buy_type);
    	}
    	
    	//!!--output your features here.--!!
    	
    	//output counting features
    	result.setDouble("c_bro_decay_cnt", bro_decay_cnt);
    	result.setDouble("c_fav_decay_cnt", fav_decay_cnt);
    	result.setDouble("c_cart_decay_cnt", cart_decay_cnt);
    	result.setDouble("c_buy_decay_cnt", buy_decay_cnt);
    	
    	//filter non-interactive
    	if(bro_decay_cnt + fav_decay_cnt + cart_decay_cnt + buy_decay_cnt > 0.0)
    		context.write(result);
    	
    	//heart beat
    	context.progress();
    }

    public void cleanup(TaskContext arg0) throws IOException {
    	arg0.progress();
    }
    
    private double double12(int act_day, int act_type)
    {
    	double factor = 1.0;
    	switch(act_type)
    	{
    	case 1:
    		factor = 0.583;
    		break;
    	case 2:
    		factor = 0.730;
    		break;
    	case 3:
    		factor = 0.424;
    		break;
    	case 4:
    		factor = 0.217;
    		break;
    	default:
    		return 1.0;
    	}
    	return act_day == 24? factor : 1.0; 
    }

    private final double[] buy_factor = 
    		{1.,0.67376934,0.77492761,0.81859662,0.75831408,0.67515868,0.50912574,0.43394074,0.37491591,0.33358683,0.29382257,0.28165492,0.25822633,0.247916,0.24307526,0.20396911,0.19120185,0.17763023,0.17328673,0.16911872,0.16911872,0.16911872,0.16911872,0.16911872,0.16911872,0.16911872,0.16911872,0.16911872,0.16911872,0.16911872,0.16911872};
    private double decay(int act_day, int labelday, int act_type)
    {
    	int x = labelday - act_day;
    	double b = 0d;
    	switch(act_type)
    	{
    	case 1:
    		b = -1.885;
    		break;
    	case 2:
    		b = -1.143;
    		break;
    	case 3:
    		b = -1.531;
    		break;
    	case 4:
    		b = 0.0;
    		break;
    	default:
    		return 0d;
    	}
    	return act_type != 4? Math.pow(x, b) : buy_factor[x - 1];
    }
}
