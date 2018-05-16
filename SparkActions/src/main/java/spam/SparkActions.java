package spam;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.SparkConf;

import java.util.List;
import java.util.Arrays;

public class SparkActions {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("actions").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List <String> list = Arrays.asList("Shunsui Kyoraku", "Suì-Feng", "Rojuro Otoribashi", 
				"Isane Kotetsu", "Shinji Hirako", "Byakuya Kuchiki", "Tetsuzaemon Iba", "Lisa Yadomaru", 
				"Kensei Muguruma", "Toshiro Hitsugaya", "Kenpachi Zaraki", "Mayuri Kurotsuchi", "Rukia Kuchiki");
		
		//Transforms the List in one String RDD
		JavaRDD<String> rdd = sc.parallelize(list);
		
		System.out.println(rdd.count());
		System.out.println(rdd.countByValue());
		
		List<String> collectList = rdd.collect();
		
		for(String c : collectList){
			System.out.println(c);
		}
		
		collectList = rdd.take(5);
		
		for (String c : collectList){
			System.out.println(c);
		}		

		List<Integer> list2 = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
		JavaRDD<Integer> rdd2 = sc.parallelize(list2);
		
		Integer value = rdd2.reduce(new Function2 <Integer, Integer, Integer> (){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				Integer b =  v1 * v2;
				System.out.println(b.intValue());
				return b;
			}
			
		});
		
		System.out.println(value.intValue());
		
		rdd.saveAsTextFile("/home/cloudera/workspace/SparkActions/src/main/java/spam/captains.txt");
		sc.close();
	}

}
