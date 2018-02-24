/**
 * Created by colntrev on 2/22/18.
 * TODO: Check out sequence files OR convert MapReduce Implementation to TextFiles
 */
package main.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.util.*;

public class TriangleCounting {
    public static void main(String[] args){
        if(args.length < 2){
            System.err.println("Usage: TriangleCounting <hdfs file> <output file>");
            System.exit(-1);
        }
        String hdfsFile = args[0];
        String outputFile = args[1];
        
        SparkConf conf = new SparkConf().setAppName("Triangle Counting");
        JavaSparkContext context = new JavaSparkContext(conf);
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

        JavaPairRDD<Tuple2<Long,Long>, Long> possibleTriangles = triangles.flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Iterable<Long>>, Tuple2<Long, Long>, Long>() {
            @Override
            public Iterator<Tuple2<Tuple2<Long, Long>, Long>> call(Tuple2<Long, Iterable<Long>> anglePairs) throws Exception {
                Iterable<Long> connectors = anglePairs._2;
                List<Tuple2<Tuple2<Long,Long>,Long>> result = new ArrayList<>();

                // Generate K,V pairs with null connector
                // reference MapReduce implementation
                for(Long connector : connectors){
                    long placeholder = 1;
                    Tuple2<Long, Long> triangle1 = new Tuple2<>(anglePairs._1,connector);
                    Tuple2<Tuple2<Long,Long>,Long> kvPair = new Tuple2<>(triangle1, placeholder);
                    result.add(kvPair);
                }

                //RDD values are immutable, we need to put them in a mutable structure
                List<Long> connectorCopy = new ArrayList<>();
                for(Long connector : connectors){
                    connectorCopy.add(connector);
                }

                Collections.sort(connectorCopy);
                // Generate V,V pairs with K connector
                // reference MapReduce implementation
                for(int i = 0; i < connectorCopy.size(); i++){
                    for(int j = i + 1; j < connectorCopy.size(); j++){
                        Tuple2<Long, Long> triangle2 = new Tuple2<>(connectorCopy.get(i),connectorCopy.get(j));
                        Tuple2<Tuple2<Long,Long>,Long> vvPair = new Tuple2<>(triangle2,anglePairs._1);
                        result.add(vvPair);
                    }
                }
                return result.iterator();
            }
        });
        JavaPairRDD<Tuple2<Long,Long>, Iterable<Long>> possibleGroup = possibleTriangles.groupByKey();

        JavaRDD<Tuple3<Long,Long,Long>> uniqueTriangles = possibleGroup.flatMap(new FlatMapFunction<Tuple2<Tuple2<Long, Long>, Iterable<Long>>, Tuple3<Long, Long, Long>>() {
            @Override
            public Iterator<Tuple3<Long, Long, Long>> call(Tuple2<Tuple2<Long, Long>, Iterable<Long>> angles) throws Exception {
                Tuple2<Long,Long> key = angles._1;
                Iterable<Long> connectors = angles._2;


                boolean seenZero = false;
                List<Long> connector = new ArrayList<>();
                for(Long node : connectors){
                    if(node == 0){ // sometimes zero is added in by Spark. It is not a valid node for this implementation
                        seenZero = true;
                    } else {
                        connector.add(node);
                    }
                }

                List<Tuple3<Long,Long,Long>> result = new ArrayList<>();
                if(seenZero){
                    if(!connector.isEmpty()) {
                        for (Long node : connector) {
                            long[] Triangle = {key._1, key._2, node};
                            Arrays.sort(Triangle);
                            result.add(new Tuple3<>(Triangle[0], Triangle[1], Triangle[2]));
                        }
                    }
                }

                return result.iterator();
            }
        }).distinct();

        uniqueTriangles.saveAsTextFile(outputFile);
        context.close();
    }
}
