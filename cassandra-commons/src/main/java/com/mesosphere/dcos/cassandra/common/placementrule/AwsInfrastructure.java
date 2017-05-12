package com.mesosphere.dcos.cassandra.common.placementrule;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;

public class AwsInfrastructure {

	private Map<String, List<String>> regionToZonesMap = new HashMap<String, List<String>>();

	private Map<String, String> zoneToRegionMap = new HashMap<String, String>();

	public AwsInfrastructure() {
		intialize();
	}
	
	private void intialize(){
		for (AwsRegion awsRegion : AwsRegion.values()) {
			String regionCode = awsRegion.getRegionCode();
			List<String> zones = new ArrayList<String>();
			for (String zone : awsRegion.avaliablityZones) {
				zones.add(zone);
				zoneToRegionMap.put(zone, regionCode);
			}
			regionToZonesMap.put(regionCode, zones);
		}
	}

	public enum AwsRegion {
		US_EAST_1("Northern Virginia", "us-east-1", "us-east-1a", "us-east-1b", "us-east-1c", "us-east-1d",
				"us-east-1e"),

		US_EAST_2("Ohio", "us-east-2", "us-east-2a", "us-east-2b", "us-east-2c"),

		US_WEST_1("Northern California", "us-west-1", "us-west-1a", "us-west-1b"),

		US_WEST_2("Oregon", "us-west-2", "us-west-2a", "us-west-2b", "us-west-2c"),

		CA_CENTRAL_1("Central", "ca-central-1", "ca-central-1a", "ca-central-1b", "ca-central-1c"),

		SA_EAST_1("São Paulo", "sa-east-1", "sa-east-1a", "sa-east-1b", "sa-east-1c"),

		EU_WEST_1("Ireland", "eu-west-1", "eu-west-1a", "eu-west-1b", "eu-west-1c"),

		EU_WEST_2("London", "eu-west-2", "eu-west-2a", "eu-west-2b"),

		EU_CENTRAL_1("Frankfurt", "eu-central-1", "eu-central-1a", "eu-central-1b"),

		AP_SOUTHEAST_1("Singapore", "ap-southeast-1", "ap-southeast-1a", "ap-southeast-1b", "ap-southeast-1c"),

		AP_SOUTHEAST_2("Sydney", "ap-southeast-2", "ap-southeast-2a", "ap-southeast-2b", "ap-southeast-2c"),

		AP_NORTH_EAST_1("Tokyo", "ap-northeast-1", "ap-northeast-1a", "ap-northeast-1b", "ap-northeast-1c"),

		AP_NORTH_EAST_2("Seoul", "ap-northeast-2", "ap-northeast-2a", "ap-northeast-2b"),

		AP_SOUTH_1("Mumbai", "ap-south-1", "ap-south-1a", "ap-south-1b");

		AwsRegion(String regionName, String regionCode, String... az) {
			this.regionCode = regionCode;
			this.regionName = regionName;
			this.avaliablityZones = az;
		}

		private final String regionName;
		private final String regionCode;
		private final String[] avaliablityZones;

		public String getRegionName() {
			return regionName;
		}

		public String getRegionCode() {
			return regionCode;
		}

		public String[] getAvaliablityZones() {
			return avaliablityZones;
		}

	}

	public List<String> getSiblingZones(String zone) {
		String region = zoneToRegionMap.get(zone);
		if (region != null) {
			return regionToZonesMap.get(region);
		}
		return null;
	}

	public List<String> getMySiblingZones() throws URISyntaxException, ClientProtocolException, IOException {
		URI url = new URI("http://169.254.169.254/latest/meta-data/placement/availability-zone");
		Content content = Request.Get(url).execute().returnContent();
		String zone = content.toString();
		return getSiblingZones(zone);
	}
}
