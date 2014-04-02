/*
 * copyright 2014, gash 
 * 
 * Gash licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package poke.server.management.managers;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.monitor.HeartMonitor;
import poke.broadcast.*;
import poke.server.management.managers.LeaderElectionData;
import poke.server.management.ManagementInitializer;
import eye.Comm.LeaderElection;
import eye.Comm.LeaderElection.VoteAction;
import eye.Comm.Management;

/**
 * The election manager is used to determine leadership within the network.
 * 
 * @author gash
 * 
 */
public class ElectionManager /* extends Thread */{
	protected static Logger logger = LoggerFactory.getLogger("management");
	protected static AtomicReference<ElectionManager> instance = new AtomicReference<ElectionManager>();
	private static  ConcurrentLinkedQueue<LeaderElectionData> neighbours = new ConcurrentLinkedQueue<LeaderElectionData>();

	public ConcurrentLinkedQueue<LeaderElectionData> getNeighbours() {
		return neighbours;
	}

	

	private String nodeId;

	private static String leader = null;
	private boolean participant = false;
	

	private String previousLeader;

	

	private String destHost;
	private int destPort;
	
    private Channel channel;
   
  
    private  boolean electionMsg;
   

	private  boolean okMsgRecieved;
    int msgType=1;
    private String senderId;
    private boolean iInitiate;
   

	/** @brief the number of votes this server can cast */
	private int votes = 1;
	private int myPublicPort;

	public int getMyPublicPort() {
		return myPublicPort;
	}

	public void setMyPublicPort(int myPublicPort) {
		this.myPublicPort = myPublicPort;
	}

	

	public static ElectionManager getInstance(String id, int votes) {
		instance.compareAndSet(null, new ElectionManager(id, votes));
		return instance.get();
	}

	public static ElectionManager getInstance() {
		return instance.get();
	}
	
	public String getLeader(){
		return leader;
	}
	
	public void setLeader(String leaderValue){
		leader=leaderValue;
		
	}
	
	public boolean isParticipant() {
		return participant;
	}

	public void setParticipant(boolean participant) {
		this.participant = participant;
	}
	
	 public  boolean isElectionMsg() {
			return electionMsg;
		}

		public  void setElectionMsg(boolean electionMsg) {
			this.electionMsg = electionMsg;
		}

		public  boolean isOkMsgRecieved() {
			return okMsgRecieved;
		}

		public  void setOkMsgRecieved(boolean okMsgRecieved) {
			this.okMsgRecieved = okMsgRecieved;
		}

	/**
	 * initialize the manager for this server
	 * 
	 * @param nodeId
	 *            The server's (this) ID
	 */
	protected ElectionManager(String nodeId, int votes) {
		this.nodeId = nodeId;

		if (votes >= 0)
			this.votes = votes;

	}

	public void addConnectToThisNode(String nodeId, String host, int mgmtport,int status) {

		LeaderElectionData ld = new LeaderElectionData(nodeId, host, mgmtport);
		ld.setActive(status);
		neighbours.add(ld);
		logger.info("Election manager addConnectToThisNode --> Host is: "
				+ destHost + " and destPort is: " + destPort);
	}
	
	public String getPreviousLeader() {
		return previousLeader;
	}

	public void setPreviousLeader(String previousLeader) {
		this.previousLeader = previousLeader;
	}

//	public void setChannel(ChannelFuture f) {
//		channel = f.channel();
//	}

	private Management generateLE(VoteAction vote, String nodeId) {
		LeaderElection.Builder electionBuilder = LeaderElection.newBuilder();
		electionBuilder.setNodeId(nodeId);
		electionBuilder.setBallotId("0");
		electionBuilder.setDesc("election message");
		electionBuilder.setVote(vote);
		LeaderElection electionMsg = electionBuilder.build();

		Management.Builder mBuilder = Management.newBuilder();
		mBuilder.setElection(electionMsg);
		Management msg = mBuilder.build();

		return msg;
	}

	public Channel connect(String host, int port) {
		// Start the connection attempt.
		ChannelFuture channelFuture = null;
		EventLoopGroup group = new NioEventLoopGroup();

		try {
			ManagementInitializer mi = new ManagementInitializer(false);
			Bootstrap b = new Bootstrap();

			b.group(group).channel(NioSocketChannel.class).handler(mi);
			b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
			b.option(ChannelOption.TCP_NODELAY, true);
			b.option(ChannelOption.SO_KEEPALIVE, true);

			logger.info("destination host & port " + host + "& " + port);

			InetSocketAddress destination = new InetSocketAddress(host,
					port);

			channelFuture = b.connect(destination);
			channelFuture.awaitUninterruptibly(5000l);

			logger.info("connect successful");

		} catch (Exception ex) {
			logger.debug("failed to initialize the election connection");

		}

		if (channelFuture != null && channelFuture.isDone()
				&& channelFuture.isSuccess())
			return channelFuture.channel();
		else
			throw new RuntimeException(
					"Not able to establish connection to server");
	}
	//not used anymore
	public boolean checkIamGreater(){
		boolean possibleLeader=false;
		int myId=Integer.parseInt(nodeId);
		for (LeaderElectionData ld : neighbours){
			int neighbourId=Integer.parseInt(ld.getNodeId());
			if(myId>neighbourId){
				possibleLeader=true;
			}
		}
		return possibleLeader;
	}
	
	
	
	private void send(Management msg) {
        int myId=Integer.parseInt(nodeId);
		Channel channel; 
		try {
			
			switch(msgType){
			case 1:for (LeaderElectionData ld : neighbours){
				int neighbourId=Integer.parseInt(ld.getNodeId());
			
				//if(neighbourId>myId && !(ld.getNodeId().equals(previousLeader))){
				if(neighbourId>myId && (ld.getActive()==1)){
				   channel = connect(ld.getHost(),ld.getPort());
				   channel.writeAndFlush(msg);
				   participant = true;
				   logger.info("Election message (" + nodeId + ") sent to "
						+ ld.getNodeId() + " at " + ld.getPort());
			       }
			}
					break;
			case 2:for (LeaderElectionData ld : neighbours){
				if(senderId.equals(ld.getNodeId())){
				channel = connect(ld.getHost(),ld.getPort());
				channel.writeAndFlush(msg);
				logger.info("Ok message (" + nodeId + ") sent to "
						+ld.getHost() + " at " + ld.getPort() +""+ld.getNodeId());
			
				}
			}
			participant = true;
			break;
			
			case 3://co-rrdinator msg send to all lower ids
				for (LeaderElectionData ld : neighbours){
					int neighbourId=Integer.parseInt(ld.getNodeId());
					if(neighbourId<myId && (ld.getActive()==1)){

					   channel = connect(ld.getHost(),ld.getPort());
					   channel.writeAndFlush(msg);
					   participant = true;
					   logger.info("Co-ordinator message (" + nodeId + ") sent to "
							+ ld.getHost() + " at " + ld.getPort());
				       }
				}
				   break;
				   default : logger.info("unknown message type");break;
				}
			
			
			
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Failed to send leader election message");
		}
	
		
		
	}
	
	
	
	
	public void initiateElection() {
		iInitiate=true;
		logger.info("starting Election manager");

		Management msg = null;

		if (leader == null && !participant) {
			msgType=1;
			msg = generateLE(LeaderElection.VoteAction.ELECTION, nodeId);
			send(msg);
		}//in case it already recieved an election msg but still initiating
			if(!okMsgRecieved && electionMsg){
				//if no ok message is recieved
				logger.info("No ok message recieved");
				//I am the leader
				HeartMonitor.leaderDown=false;
				msg = generateLE(LeaderElection.VoteAction.DECLAREWINNER,
						nodeId);
				participant=false;
				leader=nodeId;
				msgType=3;		
				send(msg);
				BroadcastLeader broadcast=new BroadcastLeader(myPublicPort);
				broadcast.start();
				
			
		}

		
	}
	
	//retrieve the leader of the network
	public String whoIsTheLeader(){
		if(leader!=null)
		return leader;
		else
			return null;
		
	}

	// }

	// Shaji: ElectionManager now extends Thread. Overiding run() method of
	// thread
	// @Override
	public void run() {

		// to be used later

	}
	
	public void setChannel(ChannelFuture f) {
		channel = f.channel();
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public void processRequest(LeaderElection req) throws Exception {
		if (req == null)
			return;

		// logger.info("Received an election request..");
		Management msg = null;
		if (req.hasExpires()) {
			long ct = System.currentTimeMillis();
			if (ct > req.getExpires()) {
				// election is over
				return;
			}
		}

		if (req.getVote().getNumber() == VoteAction.ELECTION_VALUE) {
			// an election is declared!
			logger.info("Election declared!");
			if(!iInitiate){
				okMsgRecieved=false;
			}
			
			electionMsg=true;
		    senderId=req.getNodeId();
			msg = generateLE(LeaderElection.VoteAction.ABSTAIN, nodeId);
		    msgType=2;
	        send(msg);
	        participant=true;

		} else if (req.getVote().getNumber() == VoteAction.DECLAREVOID_VALUE) {
			// no one was elected, I am dropping into standby mode
			participant=false;
		} else if (req.getVote().getNumber() == VoteAction.DECLAREWINNER_VALUE) {
			// some node declared themself the leader
			HeartMonitor.leaderDown=false;
			//leaderElected=true;
			leader = req.getNodeId();
			logger.info("Winner declared! leader is :" + leader);
			 

		} else if (req.getVote().getNumber() == VoteAction.ABSTAIN_VALUE) {
			// for some reason, I decline to vote
			okMsgRecieved=true;
			participant=false;
		} 
		if(!okMsgRecieved && electionMsg){
			//if no ok message is recieved
			logger.info("No ok message recieved");
			//I am the leader
			HeartMonitor.leaderDown=false;
			msg = generateLE(LeaderElection.VoteAction.DECLAREWINNER,
					nodeId);
			participant=false;
			leader=nodeId;
			msgType=3;		
			send(msg);
			BroadcastLeader broadcast=new BroadcastLeader(myPublicPort);
			broadcast.start();
			
		}
	}
}
