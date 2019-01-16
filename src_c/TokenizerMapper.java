import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Hashtable;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
{ 
    private final IntWritable Counter = new IntWritable(1);
    private Text ResultKey = new Text();
    private Hashtable<Integer, String> countryFullName;
    private Hashtable<String, Integer> countryName;
    private Hashtable<String, Integer> countryISO2;
    private Hashtable<String, Integer> countryISO3;
    
    public void map(Object key, Text line, Context context)
    			throws IOException, InterruptedException
    {    	
    	// get the fields
    	//     0		  1		2	  3
    	// epoch_time;tweetId;tweet;device
    	//
    	// Example:
    	// 1469453965000;757570957502394369;Over 30 million women...;<a href="http://twitter.com/download/iphone" rel="nofollow">Twitter for iPhone</a>
    	//
    	// Rules:
    	// 1) The size of the epoch_time and tweetId is fixed:
    	//   a. epoch_time - size 13 digits
    	//   b. tweetId - size 18 digits
    	// 2) The device column always starts with: <a href=
    	// These rules are enough to help us get good lines of tweet messages.
    	//
    	String[] fields = line.toString().split(";");
    	
    	if (fields.length == 4) // good row
    	{
    		if (fields[0].length() == 13
    			&&
    			fields[1].length() == 18
    			&&
    			fields[3].startsWith("<a href="))
    		{
    			// good row
    			// Extract, Cleanup, Transform, Lookup and Emit data
    			ProcessRow(context, fields[2]);
    		}
    	}
    	else if (fields.length > 4)
    	{
    		if (fields[0].length() == 13
        			&&
        			fields[1].length() == 18
        			&&
        			fields[fields.length - 1].startsWith("<a href="))
        	{
        		// good row - more than 4 delimiters
    			// get the tweet message
    			int DeviceFieldPos = line.toString().lastIndexOf(";<a href=");
    			String TweetMsg = line.toString().substring(13 + 1 + 18 + 1, DeviceFieldPos);
    			
    			// Extract, Cleanup, Transform, Lookup and Emit data
    			ProcessRow(context, TweetMsg);
        	}
    	}
    	
    	// do nothing for other rows - filter
    }
    
    @Override
	protected void setup(Context context)
			throws IOException, InterruptedException
	{
    	URI fileUri = null;
		FileSystem fs = null;
		FSDataInputStream in = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
		//String AllDataStr = null;
        
		try
    	{
    		// create the lookups
    		countryFullName = new Hashtable<Integer, String>();
        	countryName = new Hashtable<String, Integer>();
        	countryISO2 = new Hashtable<String, Integer>();
        	countryISO3 = new Hashtable<String, Integer>();
        	
    		// We know there is only one cache file, so we only retrieve that URI
    		fileUri = context.getCacheFiles()[0];
    		fs = FileSystem.get(context.getConfiguration());
    		in = fs.open(new Path(fileUri));
    		isr = new InputStreamReader(in, "UTF8"); // the lookup file is saved as UTF-8
    		br = new BufferedReader(isr);
    		
    		// we discard the header row
			br.readLine();
			
			String line = null;
			while ((line = br.readLine()) != null)
			{
				context.getCounter(CustomCounters.NUM_COUNTRIES).increment(1);
				
				String[] fields = line.split("\t");
				// Fields are: 0:country_id 1:country_full_name 2:country_name 3:country_iso_2 4:country_iso_3
				if (fields.length <= 5)
				{
					countryFullName.put(Integer.valueOf(fields[0]), fields[0] + "-" + fields[1]);
					countryName.put((fields.length > 2?fields[2]:""), Integer.valueOf(fields[0]));
					countryISO2.put((fields.length > 3?fields[3]:""), Integer.valueOf(fields[0]));
					countryISO3.put((fields.length > 4?fields[4]:""), Integer.valueOf(fields[0]));
				}
			}
			
			br.close();

    	}
    	catch (Exception ex) { throw ex; }

		super.setup(context);
	}
    
    // helping functions
    private List<String> GetAllHashTags(String MsgStr)
    {
    	List<String> HTArr = new ArrayList<String>(0);
    	String CurrHTStr = "";
    	boolean IsHT = false;
    	
    	for(char c : MsgStr.toCharArray())
    	{
    		if (c == ' ' && IsHT) // hashTag ends with space
    		{
    			IsHT = false;
    			HTArr.add(CurrHTStr);
    			CurrHTStr = "";
    		}
    		
    		if (c == '#' && IsHT) // hashTag ends with #
    		{
    			HTArr.add(CurrHTStr);
    			CurrHTStr = "";
    		}
    		
    		if (c == '#' && !IsHT) IsHT = true;
    		if (IsHT) CurrHTStr += c;
    	}
    	
    	// hashtag at the end of line scenario 
    	if (IsHT) HTArr.add(CurrHTStr);
    	
    	return HTArr;
    }
    
    private String[] PreKeyWords =
	{
		"go"
		,"team"
		,"goteam"
		,"weare"
		,"weareteam"
		,"iamteam"
		,"ilove"
		,"only"
		,"letsgo"
		,"time"
		,"vai"
		,"vamos"
		,"eusoutime"
		,"seleccion"
		,"somos"
		,"somostodos"
	};
    private String[] PostKeyWords = {"team"};
    
    private void ProcessRow(Context context, String TweetMsgStr)
    		throws IOException, InterruptedException
    {
    	// get all hashtags from a Tweet    	
    	List<String> HTArr = GetAllHashTags(TweetMsgStr);

    	if (HTArr.size() > 0)
    	{
	    	for(String CurrHTStr : HTArr)
	    	{
	    		// remove punctuation (from beginning and end) and make it all lowercase
	    		String CleanHTVal = CurrHTStr.replaceAll("[\\p{Space}\\p{Punct}]+$|^[\\p{Space}\\p{Punct}]+", "").toLowerCase();
	    		// check for empty string (after removal of symbols)
	    		if (CleanHTVal.length() == 0) break;
	    		
	    		Integer CountryId = null;
	    		
	    		// compare with Country list #Bulgaria
	    		CountryId = LookupAll(CleanHTVal);

	    		// compare with Pre-word list in combination with Country list #GoBulgaria, #GoBG, #GoBGR
	    		if (CountryId == null)
	    		{
	    			for(String code : PreKeyWords)
	    			{
	    				if (CleanHTVal.length() > code.length() && CleanHTVal.startsWith(code))
	    				{
	    					String CleanHTValSub = CleanHTVal.substring(code.length()); // get the rest of the value
			    			// lookup all codes
			    			CountryId = LookupAll(CleanHTValSub);
			    			if (CountryId != null) break;
	    				}
	    			}
	    			
	    		}
	    			
	    		// compare with Post-word list in combination with Country list #BulgariaTeam
	    		if (CountryId == null)
	    		{
	    			for(String code : PostKeyWords)
	    			{
	    				if (CleanHTVal.length() > code.length() && CleanHTVal.endsWith(code))
	    				{
	    					String CleanHTValSub = CleanHTVal.substring(0, CleanHTVal.length()-code.length()); // get the start of the value
			    			// lookup all codes
			    			CountryId = LookupAll(CleanHTValSub);
			    			if (CountryId != null) break;
	    				}
	    			}
	    			
	    		}
	    		
	    		
	    		if (CountryId != null)
	    		{
	    			// get country full name
	    			String CountryNameID = countryFullName.get(CountryId);
	    			String CountryName = CountryNameID.substring(CountryNameID.indexOf("-")+1);
	    			
	    			// set result
	    			ResultKey.set(CountryName);
		    		
		    		// emit
		    		context.write(ResultKey, Counter);
	    			
	    			/*
	    			// check the quality of hashtags for USA
	    			if (CountryName.equals("USA"))
	    			{
	    				// set result
		    			ResultKey.set(CleanHTVal);
			    		
			    		// emit
			    		context.write(ResultKey, Counter);
	    			}
	    			*/
	    		}
	    	}
    	}
    }
    
    private Integer LookupAll(String LkpVal)
    {
    	Integer CountryId = null;
    	
    	// try with name: Bulgaria
		CountryId = countryName.get(LkpVal);
		// try with 2 code: BG
		if (CountryId == null) CountryId = countryISO2.get(LkpVal);
		// try with 3 code: BGR
		if (CountryId == null) CountryId = countryISO3.get(LkpVal);
		
		return CountryId;
    }
}