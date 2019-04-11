package com.anmpout.realtimemapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONArray;
import org.json.JSONObject;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author cloudera
 */
public class RealTimeGrabber {
         private static int MINUTES = 1; // The delay in minutes
	private final String USER_AGENT = "Mozilla/5.0";
      public static void main(String[] args) throws Exception {
          RealTimeGrabber grabber = new RealTimeGrabber();

Timer timer = new Timer();
 timer.schedule(new TimerTask() {
    @Override
    public void run() { try {
        grabber.callRealtimeData();
        } catch (Exception ex) {
            ex.printStackTrace();
            Logger.getLogger(RealTimeGrabber.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
 }, 0, 1000 * 60 * MINUTES);
  
    

    }  

    private  void callRealtimeData() throws Exception {

		String url = "http://feed.opendata.imet.gr:23577/fcd/gps.json?offset=0&limit=-1";
		
		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();

		// optional default is GET
		con.setRequestMethod("GET");

		//add request header
		con.setRequestProperty("User-Agent", USER_AGENT);

		int responseCode = con.getResponseCode();
		System.out.println("\nSending 'GET' request to URL : " + url);
		System.out.println("Response Code : " + responseCode);

		BufferedReader in = new BufferedReader(
		        new InputStreamReader(con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();

		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();
                 parseResposeAndSaveData(response.toString());
		//print result
		System.out.println(response.toString());
    
        
    }

    private void parseResposeAndSaveData(String toString) throws IOException {
       long unixTimestamp  = System.currentTimeMillis() / 1000L;
       long bucket =  unixTimestamp - (unixTimestamp % 3600);


        FileWriter writer = null;
        JSONArray jSONArray = new JSONArray(toString);
        for(int i=0;i<jSONArray.length();i++){
            JSONObject jSONObject = new JSONObject(jSONArray.get(i).toString());
            List<String> rowElements = new ArrayList<>();
            
            rowElements.add(jSONObject.optString("recorded_timestamp", ""));
            rowElements.add(jSONObject.optString("lon", ""));
            rowElements.add(jSONObject.optString("lat", ""));
            rowElements.add(jSONObject.optString("altitude", ""));
            rowElements.add(jSONObject.optString("speed", ""));
            rowElements.add(jSONObject.optString("orientation", ""));
            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
    Date date;
      long unixTime = 0;
            
     try {
         date = dateFormat.parse(jSONObject.optString("recorded_timestamp", ""));
     }catch(Exception ex){
         continue;
     
     }
        String dateDir = createDirectoryOptional(date);
        String csvFile = "/home/cloudera/Downloads/realTime/"+dateDir;
         writer = new FileWriter(csvFile,true);
        CSVUtils.writeLine(writer, rowElements);
         writer.flush();   
        }
        

        
        writer.close(); 
    }
    
      long unixTimestamp  = System.currentTimeMillis() / 1000L;

    private String createDirectoryOptional(Date myDate) {      
       // System.out.println(new SimpleDateFormat("HH").format(myDate));
       String dateDir = new SimpleDateFormat("yyyy-MM-dd").format(myDate);
             String directoryName ="/home/cloudera/Downloads/realTime/"+dateDir;
    File directory = new File(directoryName);
    if (! directory.exists()){
        directory.mkdir();
        // If you require it to make the entire directory path including parents,
        // use directory.mkdirs(); here instead.
    } 
    return dateDir+"/"+new SimpleDateFormat("H").format(myDate)+"/" ; 
    
    }
}
