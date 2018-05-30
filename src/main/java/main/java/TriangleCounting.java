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
        if(args.length < 3){
            System.err.println("Usage: TriangleCounting <hdfs file> <output file> <debug>");
            System.exit(-1);
        }
//        boolean debug = Boolean.parseBoolean(args[2]);
//        String hdfsFile = args[0];
//        String outputFile = args[1];

        SparkConf conf = new SparkConf().setAppName("Triangle Counting");
        JavaSparkContext context = new JavaSparkContext(conf);
        boolean debug = true;
        JavaRDD<String> lines =context.textFile("/home/colntrev/IdeaProjects/SparkTriangleCounting/src/main/java/main/java/bfsdata.txt");

        long startTime = System.currentTimeMillis();
        // Getting the graph edges from the file
        JavaPairRDD<Long,Long> edges = lines.flatMapToPair(s -> {
                String[] nodes = s.split(";");
                ArrayList<Tuple2<Long,Long>> pairs = new ArrayList<>();
                long source = Long.parseLong(nodes[0]);
                String[] connections = nodes[1].split(",");
                for(String con : connections){
                    long connection = Long.parseLong(con);
                    pairs.add(new Tuple2<>(source, connection));
                    pairs.add(new Tuple2<>(connection, source));
                }
                return pairs.iterator();
        });

        JavaPairRDD<Long,Iterable<Long>> triangles = edges.groupByKey();

        JavaPairRDD<Tuple2<Long,Long>, Long> possibleTriangles = triangles.flatMapToPair(anglePairs -> {
                Iterable<Long> connectors = anglePairs._2;
                List<Tuple2<Tuple2<Long,Long>,Long>> result = new ArrayList<>();
                // Generate K,V pairs with null connector
                // reference MapReduce implementation
                for(Long connector : connectors){
                    long placeholder = 0;
                    Tuple2<Long, Long> triangle1 = new Tuple2<>(anglePairs._1,connector);
                    Tuple2<Tuple2<Long,Long>,Long> kvPair = new Tuple2<>(triangle1, placeholder);
                    if(!result.contains(kvPair)) {
                        result.add(kvPair);
                    }
                }

                //RDD values are immutable, we need to put them in a mutable structure
                List<Long> connectorCopy = new ArrayList<>();
                for(Long connector : connectors){
                    connectorCopy.add(connector);
                }

                Collections.sort(connectorCopy);
                // Generate V,V pairs with K connector
                // reference MapReduce implementation
                for(int i = 0; i < connectorCopy.size() - 1; i++){
                    for(int j = i + 1; j < connectorCopy.size(); j++){
                        Tuple2<Long, Long> triangle2 = new Tuple2<>(connectorCopy.get(i),connectorCopy.get(j));
                        Tuple2<Tuple2<Long,Long>,Long> vvPair = new Tuple2<>(triangle2,anglePairs._1);
                        if(!result.contains(vvPair)) {
                            result.add(vvPair);
                        }
                    }
                }
                return result.iterator();
        });

        if(debug){
            System.out.println("Possible Triangles");
            possibleTriangles.foreach(item -> System.out.println(item));
        }
        JavaPairRDD<Tuple2<Long,Long>, Iterable<Long>> possibleGroup = possibleTriangles.groupByKey();

        if(debug) {
            System.out.println("Possible Triangles Grouped By Key");
            possibleGroup.foreach(item -> System.out.println(item));
        }

        JavaRDD<Tuple3<Long,Long,Long>> uniqueTriangles = possibleGroup.flatMap(angles-> {
                Tuple2<Long,Long> key = angles._1();
                Iterable<Long> connectors = angles._2();
                boolean seenZero = false;

                List<Long> connector = new ArrayList<>();
                for(Long node : connectors){
                    if(node == 0){
                        seenZero = true;
                    } else {
                        connector.add(node);
                    }
                }
                List<Tuple3<Long,Long,Long>> result = new ArrayList<>();
                if(seenZero){
                    if(!connector.isEmpty()) {
                        for (Long node : connector) {
                            long[] Triangle = {key._1(), key._2(), node};
                            Arrays.sort(Triangle);
                            result.add(new Tuple3<>(Triangle[0], Triangle[1], Triangle[2]));
                        }
                    } else {
                        return result.iterator();
                    }
                } else {
                    return result.iterator();
                }

                return result.iterator();
        }).distinct();
        if(debug){
            uniqueTriangles.foreach(item -> System.out.println(item));
        } else {
            //uniqueTriangles.saveAsTextFile(outputFile);
        }

        long endTime = System.currentTimeMillis();
        context.close();
        System.out.println("Elapsed Time: " + (endTime - startTime));
    }
}
