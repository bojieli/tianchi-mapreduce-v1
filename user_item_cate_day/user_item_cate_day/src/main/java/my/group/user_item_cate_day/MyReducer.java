package my.group.user_item_cate_day;

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
    	result.setBigint("user_id", key.getBigint("user_id"));
    	result.setBigint("item_id", key.getBigint("item_id"));
    	result.setBigint("item_category", key.getBigint("cate_id"));
    	result.setBigint("labelday", (long)labelday);
    	
    	//!!---add your features count logic here.--!!
    	
    	//action num counting variable
    	double bro_decay_cnt = 0d, fav_decay_cnt = 0d, cart_decay_cnt = 0d, buy_decay_cnt = 0d;
    	long bro = 0L, fav = 0L, cart = 0L, buy = 0L;
    	
    	boolean iscart6h = false, iscart12h = false, iscart24h = false;
    	boolean iscart3d = false, iscart5d = false;
    	boolean isnotbuy24h = true;
    	boolean isnotbuy3d = true, isnotbuy5d = true;
    	
    	boolean isdouble12buy = false;
    	
    	long cart_cnt = 0L;
    	
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
    		bro_decay_cnt += bro * decay(idays, labelday, bro_type) * double12(idays, bro_type);
    		fav_decay_cnt += fav * decay(idays, labelday, fav_type) * double12(idays, fav_type);
    		cart_decay_cnt += cart * decay(idays, labelday, cart_type) * double12(idays, cart_type);
    		buy_decay_cnt += buy * decay(idays, labelday, buy_type) * double12(idays, buy_type);
    		
    		cart_cnt += cart;
    		
    		int daygap = labelday - idays; 
    		
    		if(cart == 1L)
    		{
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
    			
    		}
    		else if(buy == 1L)
    		{
    			if(isnotbuy24h && daygap <=1)
    				isnotbuy24h = false;
    			if(isnotbuy3d && daygap <=3)
    				isnotbuy3d = false;
    			if(isnotbuy5d && daygap <= 5)
    				isnotbuy5d = false;
    			
    			//idays == 24 --> double 12.
    			if(isdouble12buy && idays == 24)
    				isdouble12buy = false;
    		}
    		
    		//check label
    		if(!label && val.getBigint("ui_label_buy") != 0L)
    			label = true;
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
    	
    	result.setBigint("isdouble12buy", isdouble12buy ? 1L : 0L);
    	
    	result.setDouble("isdouble12buy_ui_bro_decay_cnt", isdouble12buy ? bro_decay_cnt : 0d);
    	result.setDouble("isdouble12buy_ui_fav_decay_cnt", isdouble12buy ? fav_decay_cnt : 0d);
    	result.setDouble("isdouble12buy_ui_cart_decay_cnt", isdouble12buy ? cart_decay_cnt : 0d);
    	result.setDouble("isdouble12buy_ui_buy_decay_cnt", isdouble12buy ? buy_decay_cnt : 0d);
    	
    	result.setBigint("ui_cart_cnt", cart_cnt);
    	
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
