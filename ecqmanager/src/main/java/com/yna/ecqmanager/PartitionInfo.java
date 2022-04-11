package com.yna.ecqmanager;

public class PartitionInfo {
	private String topicName;
	private Long partition;
	private String name;
	private Long lag;

	public PartitionInfo(String topicName, Long partition)
	{
		this.setName(topicName, partition);
	}
	
	public void setLag(Long lag)
	{
		this.lag = lag;
	}
	
	public Long getLag()
	{
		return this.lag;
	}
	
	private void setName(String topicName, Long partition)
	{
		StringBuffer temp = new StringBuffer(topicName).append("-").append(partition);
		this.name = temp.toString();
		this.topicName = topicName;
		this.partition = partition;
	}
	
	public String getName()
	{
		return this.name;
	}
	
	public String getTopicName()
	{
		return this.topicName;
	}
	
	public Long getPartition()
	{
		return this.partition;
	}
	public String getInfo()
	{
		StringBuffer temp = new StringBuffer("[").append(this.name).append("] lag : ").append(this.lag);
		return temp.toString();
	}
}
