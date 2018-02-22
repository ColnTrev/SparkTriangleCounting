/**
 * Created by colntrev on 2/22/18.
 * TODO: Check out sequence files OR convert MapReduce Implementation to TextFiles
 */
package main.java;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import scala.Tuple3;
import java.util.Arrays;
import java.util.Iterator;

public class TriangleCounting {
    public static void main(String[] args){
        if(args.length < 1){
            System.err.println("Usage: TriangleCounting <hdfs file>");
            System.exit(-1);
        }
        String hdfsFile = args[0];
        JavaSparkContext context = new JavaSparkContext();
        JavaRDD<String> lines =context.textFile(hdfsFile);

        // Getting the graph edges from the file
        JavaPairRDD<Long,Long> edges = lines.flatMapToPair(new PairFlatMapFunction<String, Long, Long>() {
            @Override
            public Iterator<Tuple2<Long, Long>> call(String s) throws Exception {
                String[] nodes = s.split(" ");
                long start = Long.parseLong(nodes[0]);
                long end = Long.parseLong(nodes[1]);
                return Arrays.asList(new Tuple2<Long,Long>(start,end),new Tuple2<Long,Long>(end,start)).iterator();
            }
        });

        JavaPairRDD<Long,Iterable<Long>> triangles = edges.groupByKey();

    }
}
