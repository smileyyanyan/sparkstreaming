package com.sanutty.spark.streaming.functions;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import com.sanutty.spark.streaming.entities.HouseInfo;
import com.sanutty.spark.streaming.util.ConversionUtil;

public class MapKafkaMessageToHouseInfoFunction implements MapFunction<Row, HouseInfo> {

	private static final long serialVersionUID = 1L;

	@Override
	public HouseInfo call(Row row) throws Exception {
		String jsonString = row.get(0).toString();
		return ConversionUtil.createHouseInfoFromJson(jsonString);
	}

}
