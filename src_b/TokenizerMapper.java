import java.io.IOException;
import java.sql.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
{ 
    private final IntWritable Counter = new IntWritable(1);
    private Text TweetDateText = new Text();
    private DateFormat GbDateFormat = new SimpleDateFormat("dd/MM/yyyy");
    
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
    			// Extract and Transform data
    			ProcessRow(fields[0]);
    			
    			// emit
    			context.write(TweetDateText, Counter);
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
    			// Extract and Transform data
    			ProcessRow(fields[0]);
    			
    			// emit
    			context.write(TweetDateText, Counter);
        	}
    	}
    	
    	// do nothing for other rows - filter
    }
    
    // helping functions
    private void ProcessRow(String DateTimeStr)
    {
    	Date TweetsDate = new Date(Long.parseLong(DateTimeStr));
		TweetDateText.set(GbDateFormat.format(TweetsDate));
    }
}