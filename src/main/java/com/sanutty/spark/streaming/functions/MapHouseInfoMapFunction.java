package com.sanutty.spark.streaming.functions;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sanutty.spark.streaming.entities.HouseInfo;

public class MapHouseInfoMapFunction implements MapFunction<Row, HouseInfo> {

	private static final long serialVersionUID = 1L;

	@Override
	public HouseInfo call(Row row) throws Exception {

		HouseInfo house = new HouseInfo();
		/*
			{
			 0	"MedInc": 3.23,
			 1	"HouseAge": 23,
			 2	"numRooms": 6,
			 3	"numBR": 3,
			 4	"Population": 325,
			 5	"AveOccup": 3.0,
			 6	"Latitude": 37.86,
			 7	"Longitude": -122.23
			} 
		 */

		String jsonString = row.get(0).toString();
		JsonObject jsonObject = JsonParser.parseString(jsonString).getAsJsonObject();
System.out.println("-------------------------------");
System.out.println("In MapHouseInfoMapFunction.call, raw kafka message received is");
System.out.println("-------------------------------");		
System.out.println(jsonString); 		
System.out.println("-------------------------------");		

		double medInc = jsonObject.get("MedInc").getAsDouble();
		int houseAge = jsonObject.get("HouseAge").getAsInt();
		int numberOfRooms = jsonObject.get("numRooms").getAsInt();
		int numberOfBRs = jsonObject.get("numBR").getAsInt();
		double population = jsonObject.get("Population").getAsDouble();
		double avgOccupation = jsonObject.get("AveOccup").getAsDouble();
		double lat = jsonObject.get("Latitude").getAsDouble();
		double lon = jsonObject.get("Longitude").getAsDouble();

		Row result = Row.empty();
		
		
		house.setMediumIncome(medInc);
		house.setHouseAge(houseAge);
		house.setNumberOfRooms(numberOfRooms);
		house.setNumberOfBedrooms(numberOfBRs);
		house.setPopulation(population);
		house.setAverageOccupation(avgOccupation);
		house.setLatitude(lat);
		house.setLongitude(lon);
		
		return house;
	}

}
