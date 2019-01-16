import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class IntSumReducer extends Reducer<Text, IntIntPair, Text, Text>
{
    private Text result = new Text();

    public void reduce(Text key, Iterable<IntIntPair> values, Context context)
    			throws IOException, InterruptedException 
    {

        long TotalTweetMsgLength = 0;
        long TotalNumOfTweets = 0;

        for (IntIntPair value : values)
        {
        	TotalTweetMsgLength += value.getFirst().get();
        	TotalNumOfTweets += value.getSecond().get();
        }

        result.set("\nTotal Tweet Messages length: " + String.valueOf(TotalTweetMsgLength) +
        		   "\nTotal Number of Tweet Messages: " + String.valueOf(TotalNumOfTweets)+
        		   "\nAverage Length of Tweet Messages: " + String.valueOf(TotalTweetMsgLength/TotalNumOfTweets));

        context.write(key, result);
    }

}