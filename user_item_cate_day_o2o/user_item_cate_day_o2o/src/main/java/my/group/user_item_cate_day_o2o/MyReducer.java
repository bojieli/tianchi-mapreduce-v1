package my.group.user_item_cate_day_o2o;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Reducer模板。请用真实逻辑替换模板内容
 */
public class MyReducer implements Reducer {
    private Record result;
    private final int bro_type = 0, fav_type = 1, cart_type = 2, buy_type = 3;
    private final int windowsize = 10;
    private final int max_hour_gap = 744, min_hour_gap = 0;

    public void setup(TaskContext context) throws IOException {
    	result = context.createOutputRecord();
    }
    
    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
    	int labelday = key.getBigint("labelday").intValue();
    	//output key
    	result.setBigint("user_id", key.getBigint("user_id"));
    	result.setBigint("item_id", key.getBigint("item_id"));
    	result.setBigint("item_category", key.getBigint("cate_id"));
    	result.setBigint("labelday", (long)labelday);
    	
    	//!!---add your features count logic here.--!!
    	
    	//action num counting variable
    	double bro_decay_cnt = 0d, fav_decay_cnt = 0d,
    			cart_decay_cnt = 0d, buy_decay_cnt = 0d;
    	long bro = 0L, fav = 0L, cart = 0L, buy = 0L;
    	
    	boolean iscart6h = false, iscart12h = false, iscart24h = false;
    	boolean iscart3d = false, iscart5d = false;
    	boolean isnotbuy24h = true;
    	boolean isnotbuy3d = true, isnotbuy5d = true;
    	
    	boolean isdouble12buy = false;
    	
    	long cart_cnt = 0L;
    	
    	int ui_first_bro_hour_gap = min_hour_gap, ui_first_fav_hour_gap = min_hour_gap,
    			ui_first_cart_hour_gap = min_hour_gap, ui_first_buy_hour_gap = min_hour_gap;
    	int ui_last_bro_hour_gap = max_hour_gap, ui_last_fav_hour_gap = max_hour_gap, 
    			ui_last_cart_hour_gap = max_hour_gap, ui_last_buy_hour_gap = max_hour_gap;
    	
    	int[][] act_day = new int[4][windowsize];
    	
    	int[] ui_act_days = new int[4];
    	
    	//label flag
    	boolean label = false;
    	
    	while(values.hasNext())
    	{
    		Record val = values.next();
    		int idays = val.getBigint("curday").intValue();
    		int ihours = val.getBigint("curhour").intValue();
    		
    		bro = val.getBigint("ui_bro");
    		fav = val.getBigint("ui_fav");
    		cart = val.getBigint("ui_cart");
    		buy = val.getBigint("ui_buy");
    		
    		//count action num
    		int daygap = labelday - idays; 
    		int hourgap = daygap * 24 - ihours;
    		
    		if(bro != 0L)
    		{
    			bro_decay_cnt += bro * decay(idays, labelday, bro_type) * double12(idays, bro_type);
    			
    			ui_last_bro_hour_gap = hourgap < ui_last_bro_hour_gap ? hourgap : ui_last_bro_hour_gap;
    			ui_first_bro_hour_gap = hourgap > ui_first_bro_hour_gap ? hourgap : ui_first_bro_hour_gap; 
    			
    			act_day[bro_type][daygap - 1] += bro;
    		}
    		else if(fav != 0L)
    		{
    			fav_decay_cnt += fav * decay(idays, labelday, fav_type) * double12(idays, fav_type);
    			
    			ui_last_fav_hour_gap = hourgap < ui_last_fav_hour_gap ? hourgap : ui_last_fav_hour_gap;
    			ui_first_fav_hour_gap = hourgap > ui_first_fav_hour_gap ? hourgap : ui_first_fav_hour_gap;
    		
    			act_day[fav_type][daygap - 1] += fav;
    		}
       		else if(cart != 0L)
    		{
       			cart_decay_cnt += cart * decay(idays, labelday, cart_type) * double12(idays, cart_type);
       			
    			if(!iscart6h && daygap <= 1 && ihours >= 18)
    				iscart6h = true;
    			if(!iscart12h && daygap <= 1 && ihours >= 12)
    				iscart12h = true;
    			if(!iscart24h && daygap <=1)
    				iscart24h = true;
    			if(!iscart3d && daygap <= 3)
    				iscart3d = true;
    			if(!iscart5d && daygap <=5)
    				iscart5d = true;
    			
    			cart_cnt += cart;
    			
    			ui_last_cart_hour_gap = hourgap < ui_last_cart_hour_gap ? hourgap : ui_last_cart_hour_gap;
    			ui_first_cart_hour_gap = hourgap > ui_first_cart_hour_gap ? hourgap : ui_first_cart_hour_gap;
    		
    			act_day[cart_type][daygap - 1] += cart;
    		}
    		else if(buy != 0L)
    		{
    			buy_decay_cnt += buy * decay(idays, labelday, buy_type) * double12(idays, buy_type);
    			
    			if(isnotbuy24h && daygap <=1)
    				isnotbuy24h = false;
    			if(isnotbuy3d && daygap <=3)
    				isnotbuy3d = false;
    			if(isnotbuy5d && daygap <= 5)
    				isnotbuy5d = false;
    			
    			//idays == 24 --> double 12.
    			if(isdouble12buy && idays == 24)
    				isdouble12buy = false;
    			
    			ui_last_buy_hour_gap = hourgap < ui_last_buy_hour_gap ? hourgap : ui_last_buy_hour_gap;
    			ui_first_buy_hour_gap = hourgap > ui_first_buy_hour_gap ? hourgap : ui_first_buy_hour_gap;
    		
    			act_day[buy_type][daygap - 1] += buy;
    		}
    		
    		//check label
    		if(!label && val.getBigint("ui_label_buy") != 0L)
    			label = true;
    	}
    	
    	//count ui action days in window
    	for(int type = 0; type < 4; ++type)
    	{
    		ui_act_days[type] = 0;
    		for(int day = 0; day < windowsize; ++day)
    		{
    			if(act_day[type][day] != 0)
    				ui_act_days[type] ++;
    		}
    	}
    	
    	//!!--output your features here.--!!
    	
    	//output counting features
    	result.setDouble("ui_bro_decay_cnt", bro_decay_cnt);
    	result.setDouble("ui_fav_decay_cnt", fav_decay_cnt);
    	result.setDouble("ui_cart_decay_cnt", cart_decay_cnt);
    	result.setDouble("ui_buy_decay_cnt", buy_decay_cnt);
    	
    	result.setBigint("iscartnotbuy6h", iscart6h && isnotbuy24h ? 1L : 0L);
    	result.setBigint("iscartnotbuy12h", iscart12h && isnotbuy24h ? 1L : 0L);
    	result.setBigint("iscartnotbuy24h", iscart24h && isnotbuy24h ? 1L : 0L);
    	result.setBigint("iscartnotbuy3d", iscart3d && isnotbuy3d ? 1L : 0L);
    	result.setBigint("iscartnotbuy5d", iscart5d && isnotbuy5d ? 1L : 0L);
    	
    	result.setBigint("isnotdouble12buy", isdouble12buy ? 0L : 1L);
    	
    	result.setDouble("isnotdouble12buy_ui_bro_decay_cnt", 
    			isdouble12buy ? bro_decay_cnt : 0d);
    	result.setDouble("isnotdouble12buy_ui_fav_decay_cnt", 
    			isdouble12buy ? fav_decay_cnt : 0d);
    	result.setDouble("isnotdouble12buy_ui_cart_decay_cnt", 
    			isdouble12buy ? cart_decay_cnt : 0d);
    	result.setDouble("isnotdouble12buy_ui_buy_decay_cnt", 
    			isdouble12buy ? buy_decay_cnt : 0d);
    	
    	result.setBigint("ui_cart_cnt", cart_cnt);
    	
    	//reset default value
    	ui_first_bro_hour_gap = ui_first_bro_hour_gap == 0? 744 : ui_first_bro_hour_gap;
    	ui_first_fav_hour_gap = ui_first_fav_hour_gap == 0? 744 : ui_first_fav_hour_gap;
    	ui_first_cart_hour_gap = ui_first_cart_hour_gap == 0? 744 : ui_first_cart_hour_gap;
    	ui_first_buy_hour_gap = ui_first_buy_hour_gap == 0? 744 : ui_first_buy_hour_gap;
    	
    	result.setBigint("ui_first_bro_hour_gap", (long)ui_first_bro_hour_gap);
    	result.setBigint("ui_last_bro_hour_gap", (long)ui_last_bro_hour_gap);
    	result.setBigint("ui_first_last_bro_hour_gap",(long)(ui_first_bro_hour_gap - ui_last_bro_hour_gap));
    	result.setBigint("ui_first_fav_hour_gap", (long)ui_first_fav_hour_gap);
    	result.setBigint("ui_last_fav_hour_gap", (long)ui_last_fav_hour_gap);
    	result.setBigint("ui_first_last_fav_hour_gap",(long)(ui_first_fav_hour_gap - ui_last_fav_hour_gap));
    	result.setBigint("ui_first_cart_hour_gap", (long)ui_first_cart_hour_gap);
    	result.setBigint("ui_last_cart_hour_gap", (long)ui_last_cart_hour_gap);
    	result.setBigint("ui_first_last_cart_hour_gap",(long)(ui_first_cart_hour_gap - ui_last_cart_hour_gap));
    	result.setBigint("ui_first_buy_hour_gap", (long)ui_first_buy_hour_gap);
    	result.setBigint("ui_last_buy_hour_gap", (long)ui_last_buy_hour_gap);
    	result.setBigint("ui_first_last_buy_hour_gap",(long)(ui_first_buy_hour_gap - ui_last_buy_hour_gap));
    	
    	result.setBigint("ui_bro_days",(long)ui_act_days[bro_type]);
    	result.setBigint("ui_fav_days",(long)ui_act_days[fav_type]);
    	result.setBigint("ui_cart_days",(long)ui_act_days[cart_type]);
    	result.setBigint("ui_buy_days",(long)ui_act_days[buy_type]);
    	
    	//output label
    	result.setBigint("label", label? 1L : 0L);
    	
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
    	case 0:
    		factor = 0.632;
    		break;
    	case 1:
    		factor = 0.758;
    		break;
    	case 2:
    		factor = 0.453;
    		break;
    	case 3:
    		factor = 0.242;
    		break;
    	default:
    		return 1.0;
    	}
    	return act_day == 24? factor : 1.0; 
    }

    private final double[] buy_factor = 
    	{1.,0.52787544,0.5668006,0.59932195,0.54080864,0.48053742,0.36363636,0.30813661,0.27071823,0.23769463,0.19801607,0.19123556,0.17252637,0.15745856,0.15431944,0.1343546,0.12154696,0.1131341,0.11225515,0.10635359,0.10635359,0.10635359,0.10635359,0.10635359,0.10635359,0.10635359,0.10635359,0.10635359,0.10635359,0.10635359,0.10635359};
     private double decay(int act_day, int labelday, int act_type)
    {
    	int x = labelday - act_day;
    	double b = 0d;
    	switch(act_type)
    	{
    	case 0:
    		b = -1.823;
    		break;
    	case 1:
    		b = -1.147;
    		break;
    	case 2:
    		b = -1.523;
    		break;
    	case 3:
    		b = 0.0;
    		break;
    	default:
    		return 0d;
    	}
    	return act_type != 3? Math.pow(x, b) : buy_factor[x - 1];
    }
}
