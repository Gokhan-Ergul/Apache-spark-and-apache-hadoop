import scala.Tuple2;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import java.util.Arrays;
import java.util.regex.Pattern;

public class JavaWordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: JavaWordCount <inputFile> <outputDirectory>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf()
            .setAppName("JavaWordCount")
            .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sc.textFile(args[0]);

        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(SPACE.split(line)).iterator());

        JavaPairRDD<String, Integer> wordCounts = words
            .mapToPair(word -> new Tuple2<>(word, 1))
            .reduceByKey((a, b) -> a + b);

        JavaPairRDD<String, Integer> sortedWordCounts = wordCounts
            .mapToPair(tuple -> tuple.swap())
            .sortByKey(false)
            .mapToPair(tuple -> tuple.swap());

        sortedWordCounts.saveAsTextFile(args[1]);

        sc.stop();
    }
}

      