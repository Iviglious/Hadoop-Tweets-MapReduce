import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TokenizerMapper extends Mapper<Object, Text, IntWritable, IntWritable>
{ 
    private final IntWritable Counter = new IntWritable(1);
    private final IntWritable TweetLength = new IntWritable(0);
    
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
    			
    			// count characters
    			TweetLength.set(RoundCountChars(fields[2]));
    			
    			// emit
    			context.write(TweetLength, Counter);
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
    			
    			// count characters
    			TweetLength.set(RoundCountChars(TweetMsg));
    			
    			// emit
    			context.write(TweetLength, Counter);
        	}
    	}
    	
    	// do nothing for other rows - filter
    }
    
    // helping functions
    private int RoundCountChars(String StrVal)
    {
    	if (StrVal == null)
    	{
    		return 0;
    	}
    	else
    	{	
    		// round down to 5: 13 -> 10, 19 -> 15
    		return (int) Math.floor(StrVal.length()/5.00)*5;
    	}
    }
}