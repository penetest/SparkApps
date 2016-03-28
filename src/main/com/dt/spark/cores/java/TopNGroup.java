package com.dt.spark.cores.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * java实现TopN分组
 * @author macal
 * Created by Administrator on 2016/1/25.
 */
public class TopNGroup {
    public static void main(String[] args)
    {
        SparkConf conf = new SparkConf().setAppName("TopNGroup").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> data = sc.textFile("E:\\workspases\\data\\GroupTopN.txt");
        
       JavaPairRDD<String,Integer> pairs = data.mapToPair(new PairFunction<String, String, Integer>() {
           @Override
           public Tuple2<String, Integer> call(String line) throws Exception {
               String[] splitedline = line.split(" ");
               return new Tuple2<String, Integer>(splitedline[0], Integer.valueOf(splitedline[1]));
           }
       });

        JavaPairRDD<String,Iterable<Integer>> groupPairs = pairs.groupByKey();

        JavaPairRDD<String,Iterable<Integer>> top5 =groupPairs.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>() {
            @Override
            public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> groupedData) throws Exception {
                Integer[] top5 = new Integer[5];
                String groupedKey = groupedData._1();
                Iterator<Integer> groupValue = groupedData._2().iterator();
                while (groupValue.hasNext()) {
                    Integer value = groupValue.next();
                    for (int i = 0; i < 5; i++) {//����ʵ���ڲ���topN
                        if (top5[i] == null) {
                            top5[i] = value;
                            break;
                        } else if (value > top5[i]) {
                            for (int j = 4; j > i; j--) {
                                top5[j] = top5[j - 1];
                            }
                            top5[i] = value;
                            break;
                        }
                    }
                }
                return new Tuple2<String, Iterable<Integer>>(groupedKey, Arrays.asList(top5));
            }
        });
        sc.setLogLevel("OFF");
        //��ӡ��������
        top5.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> topped) throws Exception {
                System.out.println("groupKey:" + topped._1());//��ȡgroupKey
                Iterator<Integer> toppedValue = topped._2().iterator();//��ȡgroupvalue
                while (toppedValue.hasNext())//�����ӡ��ÿ��Top N
                {
                    Integer value = toppedValue.next();
                    System.out.println(value);
                }
                System.out.println("***********************************************");
            }
        });
    }
}
