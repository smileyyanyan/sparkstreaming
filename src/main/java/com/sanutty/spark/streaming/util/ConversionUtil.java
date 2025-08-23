package com.sanutty.spark.streaming.util;

import static com.sanutty.spark.streaming.util.Constants.column_name_AveOccup;
import static com.sanutty.spark.streaming.util.Constants.column_name_house_age;
import static com.sanutty.spark.streaming.util.Constants.column_name_lat;
import static com.sanutty.spark.streaming.util.Constants.column_name_lon;
import static com.sanutty.spark.streaming.util.Constants.column_name_med_inc;
import static com.sanutty.spark.streaming.util.Constants.column_name_number_of_bedrooms;
import static com.sanutty.spark.streaming.util.Constants.column_name_number_of_rooms;
import static com.sanutty.spark.streaming.util.Constants.column_name_population;
import static com.sanutty.spark.streaming.util.Constants.column_name_price;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sanutty.spark.streaming.entities.HouseInfo;

public class ConversionUtil {

	public static HouseInfo createHouseInfoFromJson(String json) {
		HouseInfo house = new HouseInfo();
		
		JsonObject jsonObject = JsonParser.parseString(json).getAsJsonObject();
		
		double medInc = jsonObject.get(column_name_med_inc).getAsDouble();
		int houseAge = jsonObject.get(column_name_house_age).getAsInt();
		int numberOfRooms = jsonObject.get(column_name_number_of_rooms).getAsInt();
		int numberOfBRs = jsonObject.get(column_name_number_of_bedrooms).getAsInt();
		double population = jsonObject.get(column_name_population).getAsDouble();
		double avgOccupation = jsonObject.get(column_name_AveOccup).getAsDouble();
		double lat = jsonObject.get(column_name_lat).getAsDouble();
		double lon = jsonObject.get(column_name_lon).getAsDouble();
		double price = 0d;
		
		if (jsonObject.get(column_name_price) != null) {
			price = jsonObject.get(column_name_price).getAsDouble();
		}
		
		house.setMediumIncome(medInc);
		house.setHouseAge(houseAge);
		house.setNumberOfRooms(numberOfRooms);
		house.setNumberOfBedrooms(numberOfBRs);
		house.setPopulation(population);
		house.setAverageOccupation(avgOccupation);
		house.setLatitude(lat);
		house.setLongitude(lon);
		house.setPrice(price);
		
		return house;
	}
	
}
