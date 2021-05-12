package JavaExample;


import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
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
    	
        /*SparkConf conf = new SparkConf().setAppName("Triangle Counting");
        JavaSparkContext context = new JavaSparkContext(conf);*/
    	SparkConf conf = new SparkConf().
    	           setAppName("TESTAPP").
    	           setMaster("local[*]").
    	           set("spark.driver.bindAddress","127.0.0.1");
    	JavaSparkContext context = new JavaSparkContext(conf);
        boolean debug = true;
        //JavaRDD<String> lines =context.textFile("/home/nataliia/Рабочий стол/CountTriangles/input.txt", 0);
        JavaRDD<String> lines =context.textFile("/home/nataliia/Рабочий стол/CountTriangles/graph к ЛР 3.txt", 0); 

        long startTime = System.currentTimeMillis();
        JavaPairRDD<Long,Long> edges = lines.flatMapToPair(s -> {
                String[] nodes = s.split(" ");
                //System.out.println("nodes, "+nodes[0]);
                ArrayList<Tuple2<Long,Long>> pairs = new ArrayList<>();
                long source = Long.parseLong(nodes[0]);
                String[] connections = nodes[1].split(" ");
                for(String con : connections){
                    long connection = Long.parseLong(con);
                    pairs.add(new Tuple2<>(source, connection));
                    pairs.add(new Tuple2<>(connection, source));
                }
                return pairs.iterator();
        });
        edges.foreach(item -> System.out.println("Edges"+item));
        JavaPairRDD<Long,Iterable<Long>> triangles = edges.groupByKey();
        triangles.foreach(item -> System.out.println("Triangles"+item));
        
        JavaPairRDD<Tuple2<Long,Long>, Long> possibleTriangles = triangles.flatMapToPair(iterPairs -> {
                Iterable<Long> connectors = iterPairs._2;
                System.out.println("iterPairs"+iterPairs._2);
                List<Tuple2<Tuple2<Long,Long>,Long>> result = new ArrayList<>();
              
                for(Long connector : connectors){
                    long placeholder = 0;
                    Tuple2<Long, Long> triangle1 = new Tuple2<>(iterPairs._1,connector);
                    Tuple2<Tuple2<Long,Long>,Long> kvPair = new Tuple2<>(triangle1, placeholder);
                    //c"Triangle+placeholder: "+triangle1+placeholder);
                    if(!result.contains(kvPair)) {
                        result.add(kvPair);
                    }
                }

       
                List<Long> connectorCopy = new ArrayList<>();
                for(Long connector : connectors){
                    connectorCopy.add(connector);
                }

                Collections.sort(connectorCopy);
                
                connectorCopy.forEach(item -> System.out.println("Copy "+item));
                for(int i = 0; i < connectorCopy.size() - 1; i++){
                    for(int j = i + 1; j < connectorCopy.size(); j++){
                        Tuple2<Long, Long> triangle2 = new Tuple2<>(connectorCopy.get(i),connectorCopy.get(j));
                        Tuple2<Tuple2<Long,Long>,Long> vvPair = new Tuple2<>(triangle2,iterPairs._1);
                        //System.out.println("Iterator: "+triangle2+iterPairs._1);
                        if(!result.contains(vvPair)) {
                            result.add(vvPair);
                        }
                    }
                }
                return result.iterator();
        });
        //possibleTriangles.foreach(item -> System.out.println("Possible triangles"+item));
        if(debug){
            //System.out.println("Possible Triangles");
            possibleTriangles.foreach(item -> System.out.println("Possible triangles"+item));
        }
        JavaPairRDD<Tuple2<Long,Long>, Iterable<Long>> possibleGroup = possibleTriangles.groupByKey();

        if(debug) {
            //System.out.println("Possible Triangles Grouped By Key");
            possibleGroup.foreach(item -> System.out.println("Triangles grouped by key"+item));
        }
        possibleGroup.foreach(item -> System.out.println("Possible group"+item));
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
        	int numberOfTriangles =0;
            uniqueTriangles.foreach(item -> System.out.println("Triangle (nodes): "+item));
          
            System.out.println("Number of triangles: "+uniqueTriangles.count());
        } else {
            //uniqueTriangles.saveAsTextFile(outputFile);
        }
   

        long endTime = System.currentTimeMillis();
        context.close();
        System.out.println("Elapsed Time: " + (endTime - startTime));
    }
}