package com.sanutty.spark.streaming.functions;

import static com.sanutty.spark.streaming.util.Constants.*;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sanutty.spark.streaming.entities.HouseInfo;
import com.sanutty.spark.streaming.util.ConversionUtil;

public class MapCSVMessageToHouseInfoFunction implements MapFunction<Row, HouseInfo> {

	private static final long serialVersionUID = 1L;

	@Override
	public HouseInfo call(Row row) throws Exception {
		String jsonString = row.prettyJson();
		return ConversionUtil.createHouseInfoFromJson(jsonString);
	}

}
