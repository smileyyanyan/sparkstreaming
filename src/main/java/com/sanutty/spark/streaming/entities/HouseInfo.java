package com.sanutty.spark.streaming.entities;

public class HouseInfo {

	private double mediumIncome;
	private int houseAge;
	private int numberOfRooms;
	private int numberOfBedrooms;
	private double population;
	private double latitude;
	private double longitude;
	private double price;
	
	private double averageOccupation;
	
	public double getAverageOccupation() {
		return averageOccupation;
	}
	public void setAverageOccupation(double averageOccupation) {
		this.averageOccupation = averageOccupation;
	}
	public double getMediumIncome() {
		return mediumIncome;
	}
	public void setMediumIncome(double mediumIncome) {
		this.mediumIncome = mediumIncome;
	}
	public int getHouseAge() {
		return houseAge;
	}
	public void setHouseAge(int houseAge) {
		this.houseAge = houseAge;
	}
	public int getNumberOfRooms() {
		return numberOfRooms;
	}
	public void setNumberOfRooms(int numberOfRooms) {
		this.numberOfRooms = numberOfRooms;
	}
	public int getNumberOfBedrooms() {
		return numberOfBedrooms;
	}
	public void setNumberOfBedrooms(int numberOfBedrooms) {
		this.numberOfBedrooms = numberOfBedrooms;
	}
	public double getPopulation() {
		return population;
	}
	public void setPopulation(double population) {
		this.population = population;
	}
	public double getLatitude() {
		return latitude;
	}
	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}
	public double getLongitude() {
		return longitude;
	}
	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}
	public double getPrice() {
		return price;
	}
	public void setPrice(double price) {
		this.price = price;
	}
	
	@Override
	public String toString() {
		StringBuilder buf = new StringBuilder();
		buf.append("(lat, lon)=(" + latitude + "," + longitude+") total rooms= " + numberOfRooms + " BRs=" + numberOfBedrooms);
		return buf.toString();
	}

	
}
