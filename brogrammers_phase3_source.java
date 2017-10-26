package phase3;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.vividsolutions.jts.geom.Envelope;

public class brogrammers_phase3_source {

	 public static void main(String args[])
	 {
		 System.out.println(" Hello");
	 }
	 public static HashSet<Envelope> envelopeGrids;
	 public static long totalNumberOfCells = 68200;
	 public static long xjsquare=0,xj=0;
	 public static void method(JavaSparkContext sc,String ip,String op)
	 {
		 try{
		
		 System.out.println("---phase3 Team Brogrammars: Phase---");
		 envelopeGrids= new HashSet<Envelope>();
		 
		 getAllEnvelopesfromrectangle();
		
		 System.out.println("generated");
	    
	    JavaPairRDD<Integer,HashMap<Envelope,Long>> fullMonthPointRDD=generatePointRDDEachDay(sc,ip);
	 
	    
	    HashMap<Integer,HashMap<Envelope,Long[]>> neighbors=mapNeighbors(fullMonthPointRDD,sc);
	      
	   
	    
	    
	        double xBar=(double)xj/(double)totalNumberOfCells;
	        
	      //  System.out.println("xBar"+ xBar);
	        
	        
	        
	        double SValue= Math.sqrt(((double)xjsquare/(double)totalNumberOfCells)-((double)xBar*(double)xBar));
	        
	      //  System.out.println("SValue = "+SValue);
	        
	        List<SpaceTimeCell> hotspotList  = new ArrayList<SpaceTimeCell>();
	    
	      
	        for(Integer date : neighbors.keySet()){
	        	
	        	//System.out.println("date :"+ date+"env size: "+ neighbors.get(date).size());
	        	
	        		
	            for(Envelope env : neighbors.get(date).keySet()){
	             
	            	
	            	
	            	int yesterDate = date-1;
	            	int tomorrowDate = date+1;
	            	
	            	
	                long count_yest = neighbors.get(yesterDate)==null?0: neighbors.get(yesterDate).get(env)==null?0:neighbors.get(yesterDate).get(env)[0];
	                
	                
	                
	                long yester_count = neighbors.get(tomorrowDate)==null?0: neighbors.get(tomorrowDate).get(env)==null?0:neighbors.get(tomorrowDate).get(env)[0];
	                
	                long spatialWeightSum = neighbors.get(date).get(env)[0] + count_yest + yester_count;
	                
	                double gNumerator,gDenominator;

	                long neighborWeightSum = neighbors.get(date).get(env)==null?0 : neighbors.get(date).get(env)[1] +
	                		(neighbors.get(yesterDate)==null?0:neighbors.get(yesterDate).get(env)==null?0:neighbors.get(yesterDate).get(env)[1]) 
	                		+ (neighbors.get(tomorrowDate)==null?0:neighbors.get(tomorrowDate).get(env)==null?0:neighbors.get(tomorrowDate).get(env)[1]);
	                
	                gNumerator = (double)spatialWeightSum - ((double)xBar * (double)neighborWeightSum);

	                gDenominator = Math.sqrt(((totalNumberOfCells * (double)neighborWeightSum) - ((double)neighborWeightSum * (double)neighborWeightSum))/(totalNumberOfCells-1));

	                double gScore = (double)gNumerator/((double)SValue * (double)gDenominator);
	               // System.out.println("gScore :"+ gScore);
	                if(!Double.isNaN(gScore))
	                {
	                	
	                	
	                	hotspotList.add(new SpaceTimeCell(gScore, date-1, env));
	                }
	              
	                
	               
	            
	        }
	       }
	        
	      
	       
	        Collections.sort(hotspotList, new Comparator<SpaceTimeCell>() {

				public int compare(SpaceTimeCell o1, SpaceTimeCell o2) {
					// TODO Auto-generated method stub
					if(o1.gScore== o2.gScore)
						return 0;
					
					return -1*(Double.compare(o1.gScore, o2.gScore));
				}
			});
	        LinkedList<String> resultArray=new LinkedList<String>();
	        int count = 1;
	        for(SpaceTimeCell cell:hotspotList)
	        {
	        	if(count>50)
	        		break;
	        	count++;
	          
	            String str = cell.envelope.getMinY()+","+ cell.envelope.getMinX()+","+cell.date+","+cell.gScore;
	            System.out.println(str);
	            resultArray.add(str);
	            
	        }
	    
	        JavaRDD<String> write_file=sc.parallelize(resultArray);
	        
	        write_file.coalesce(1).saveAsTextFile(op+"/output.csv");
	      
		 }catch(Exception e)
		 {
			 e.printStackTrace();
		 }
		
	 }
	 

	    public static JavaPairRDD<Integer, HashMap<Envelope, Long>> generatePointRDDEachDay(JavaSparkContext sc,String ip){

	        JavaRDD<String> input_file=sc.textFile(ip);
	        final String firstLine=input_file.first();

	        JavaRDD<String> inputtext = input_file.filter(new Function<String, Boolean>() {
	          
	            public Boolean call(String s) throws Exception {
	                return !s.equalsIgnoreCase(firstLine);
	            }
	        });
	        JavaPairRDD<Integer,Double[]> yellowTaxiCordinatesByDay=inputtext.mapToPair(new PairFunction<String, Integer, Double[]>() {
	            public Tuple2<Integer, Double[]> call(String arg0) throws Exception {
	            	
	            	  String[] content=arg0.split(",");
		                
		    	    			
		    	    			String date = content[1].trim().split(" ")[0];
		    	    			String days[] = date.trim().split("-");
		    	    			int day = 0;
		    	    	
		    	    			if(days[2].trim().length()==4)
		    	    			{
		    	    				 day = Integer.parseInt(days[0]);
		    	    			}
		    	    			else if(days[0].trim().length()==4)
		    	    			{
		    	    				day = Integer.parseInt(days[2]);
		    	    			}
		    	    			//System.out.println(" date: "+" "+date+" ,day: "+ day);
		    	    			
	             
	             
	                String[] s1=content[5].split("\\.");
	                String str1=s1[0];
	                String str_variable = s1[0];
	                if(s1.length>1){
	                    str1+=("."+s1[1].substring(0,Math.min(2, s1[1].length())));
	                    double xCord = Double.parseDouble(str1);
	                    xCord= doubleRoundUp(xCord-0.01);
	                    str_variable = String.valueOf(xCord);
	                }
	                
	                String[] s2=content[6].split("\\.");
	                String str2=s2[0];
	                if(s2.length>1)str2+=("."+s2[1].substring(0,Math.min(2, s2[1].length())));
	                
	                Double[] cordinates = new Double[]{ Double.parseDouble(str_variable), Double.parseDouble(str2)};
	                Tuple2<Integer, Double[]> rtuple=new Tuple2<Integer, Double[]>(day, cordinates);
	                return rtuple;
	       
	            }
	        });
	       
	       
	        JavaPairRDD<Integer, Iterable<Double[]>> yellowTaxiCordinatesRDD = yellowTaxiCordinatesByDay.groupByKey();
	        
	        JavaPairRDD<Integer,HashMap<Envelope,Long>> yellowTaxiCordEnvelopeRDD = yellowTaxiCordinatesRDD.mapToPair(new PairFunction<Tuple2<Integer,Iterable<Double[]>>, Integer, HashMap<Envelope,Long>>() {
	            public Tuple2<Integer, HashMap<Envelope, Long>> call(
	                    Tuple2<Integer, Iterable<Double[]>> arg0) throws Exception {
	            	
	                HashMap<Envelope,Long> map=new HashMap<Envelope,Long>();
	                
	                for(Double[] cordinate:arg0._2()) {
	                    double lowerx = cordinate[0];
	                    double lowery = cordinate[1];
	                    Envelope envelope=new Envelope(lowerx,doubleRoundUp(lowerx+0.01),lowery,(lowery+0.01));
	                    if(envelopeGrids.contains(envelope))
	                    {
	                    	
	                        if(map.containsKey(envelope))
	                        	map.put(envelope, map.get(envelope)+1);
	                        else
	                        	map.put(envelope, (long)1);
	                    }
	                }
	                return new Tuple2<Integer, HashMap<Envelope,Long>>(arg0._1(), map);
	            }
	        });    

	        return yellowTaxiCordEnvelopeRDD;
	    }
	    
	    public static HashMap<Integer,HashMap<Envelope,Long[]>> mapNeighbors(JavaPairRDD<Integer,HashMap<Envelope,Long>> map, JavaSparkContext sc){
	        Map<Integer, HashMap<Envelope, Long>> newHashMap=map.collectAsMap();
	        
	        HashMap<Integer,HashMap<Envelope,Long[]>> resultMap=new HashMap<Integer,HashMap<Envelope,Long[]>>();
	        
	        double min_x=-74.25;
	        double max_x=-73.7;
	        double min_y=40.5;
	        double max_y=40.9;
	        for(Integer date:newHashMap.keySet()){
	            HashMap<Envelope,Long[]> map_value=new HashMap<Envelope,Long[]>();
	            for(Envelope env:newHashMap.get(date).keySet()){
	                long val_x=newHashMap.get(date).get(env);
	                xj+=val_x;
	                xjsquare+=(val_x*val_x);
	             
	                double x1= env.getMinX();
	               
	               
	                double y1=env.getMinY();
	              
	                long count=0;
	                long neighbors = 0;
	                for(double i=doubleRoundUp(x1-0.01);i<=doubleRoundUp(x1+0.01);)
	                {
	                    for(double j=doubleRoundUp(y1-0.01);j<=doubleRoundUp(y1+0.01);)
	                    {
	                        if(i >= min_x && i < max_x && j >= min_y && j < max_y )
	                        {
	                        	
	                            Envelope newEnv=new Envelope(i,doubleRoundUp(i+0.01),j,doubleRoundUp(j+0.01));
	                            
	                            if(newHashMap.get(date).get(newEnv)!= null)
	                            {
	                                neighbors++;
	                                count = count + newHashMap.get(date).get(newEnv);
	                            }
	                        }
	                        j=doubleRoundUp(j+0.01);
	                    }i=doubleRoundUp(i+0.01);
	                }
	               
	            //    System.out.println(count);
	              
	                map_value.put(env, new Long[]{count,neighbors});
	            }
	            resultMap.put(date, map_value);
	        }
	        return resultMap;
	    }


 //Create all possible from the envelopes

	    public static void getAllEnvelopesfromrectangle(){       
            for(double i = -74.25; i<-73.7;)
            {
            	
                for(double j = 40.5; j<40.9;)
                {
                    double minX = i;
                    double minY = j;
                    double maxX = i+0.01;
                    double maxY = j+0.01;
                    envelopeGrids.add(new Envelope(doubleRoundUp(minX), doubleRoundUp(maxX), doubleRoundUp(minY), doubleRoundUp(maxY)));
                    j=doubleRoundUp(j+0.01);
                }
                i=doubleRoundUp(i+0.01);
            }
        }
	    
	    public static double doubleRoundUp(double value) {
	        return (double)Math.round(value * 100d) / 100d;
	    }

}
class SpaceTimeCell
{
	double gScore;
	int date;
	Envelope envelope;
	
	SpaceTimeCell(double gScore, int date, Envelope envelope)
	{
		this.gScore = gScore;
		this.date = date;
		this.envelope = envelope;
	}
	
}
