package com.voc.api.model;

public class ChannelRankingResult {
	private String channel_id;
	private String channel; // website_name and channel_name (Ex:壹蘋果網絡_AppleMotorFeature)
	private int count;
	
	public String getChannel_id() {
		return channel_id;
	}
	public void setChannel_id(String channel_id) {
		this.channel_id = channel_id;
	}
	public String getChannel() {
		return channel;
	}
	public void setChannel(String channel) {
		this.channel = channel;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
}
