package org.datasyslab.geospark.spatialOperator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.datasyslab.geospark.spatialPartitioning.DuplicatesHandler;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;

import scala.Tuple2;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;

public class CustomJoinQuery {
		public static void demo()
		{
		System.out.println("....In Test Demo Method....");
		}
	 public static JavaPairRDD<Envelope, HashSet<Point>> NaiveSpatialJoinQuery(JavaSparkContext sc, PointRDD pointRDD,RectangleRDD rectangleRDD , boolean bool) throws Exception {
			

			System.out.println("!---GROUP BROGRAMMAR---!");	
			
			JavaRDD<Object> rects = rectangleRDD.getRawSpatialRDD();
			
			List<Object> listRects =  rects.collect();
			
			List<Tuple2<Envelope, HashSet<Point>>> result = new ArrayList<Tuple2<Envelope, HashSet<Point>>>();
			
			int count = 0;
			
			System.out.println("Number of envelopes "+listRects.size());
			
			for(Object obj: listRects)
			{
					
					
					JavaRDD<Point> temp = RangeQuery.SpatialRangeQuery(pointRDD, (Envelope)obj, 0, bool);
					
					List<Point> resultList = temp.collect();
					
					System.out.println("List<Point> size = "+ resultList.size() +", Current count = " + (++count));
					
					Tuple2<Envelope, HashSet<Point>> tupleTemp = new Tuple2<>((Envelope)obj, new HashSet<Point>(resultList));
					
					result.add(tupleTemp);
					
				
			}
			
			System.out.println("  Parallelizing Results ");
			JavaPairRDD<Envelope, HashSet<Point>> refinedResult = sc.parallelizePairs(result);
			JavaPairRDD<Envelope, HashSet<Point>> refinedResultwithoutDuplicate = DuplicatesHandler.removeDuplicatesPointByRectangle(refinedResult);

			return refinedResultwithoutDuplicate;

			
			}

}
