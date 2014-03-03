package poke.server.management.managers;

import io.netty.channel.Channel;

import java.net.SocketAddress;

public class ElectionData {
	
	private String nodeId;
	private String host;
	private Integer port;
	private Integer mgmtport;
	public SocketAddress sa;
	public Channel channel;
	
	
	
	public String getNodeId() {
		return nodeId;
	}



	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}



	public String getHost() {
		return host;
	}



	public void setHost(String host) {
		this.host = host;
	}



	public Integer getPort() {
		return port;
	}



	public void setPort(Integer port) {
		this.port = port;
	}



	public Integer getMgmtport() {
		return mgmtport;
	}



	public void setMgmtport(Integer mgmtport) {
		this.mgmtport = mgmtport;
	}



	public SocketAddress getSa() {
		return sa;
	}



	public void setSa(SocketAddress sa) {
		this.sa = sa;
	}



	public Channel getChannel() {
		return channel;
	}



	public void setChannel(Channel channel) {
		this.channel = channel;
	}



	public ElectionData(String nodeId, String host, Integer port, Integer mgmtport){
		this.nodeId = nodeId;
		this.host = host;
		this.port = port;
		this.mgmtport = mgmtport;
	}
	
	public void setConnection(Channel channel, SocketAddress sa) {
		this.channel = channel;
		this.sa = sa;
	}

	

}
