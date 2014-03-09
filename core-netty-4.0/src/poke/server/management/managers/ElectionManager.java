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

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eye.Comm.LeaderElection;
import eye.Comm.LeaderElection.VoteAction;
import eye.Comm.Management;

/**
 * The election manager is used to determine leadership within the network.
 * 
 * @author gash
 * 
 */
public class ElectionManager extends Thread {
	protected static Logger logger = LoggerFactory.getLogger("management");
	protected static AtomicReference<ElectionManager> instance = new AtomicReference<ElectionManager>();

	private String nodeId;

	private String leader = null;
	private boolean participant = false;

	private String destHost;
	private int destPort;
	private String destNodeId;
	private Channel channel;

	/** @brief the number of votes this server can cast */
	private int votes = 1;

	ConcurrentHashMap<Channel, LeaderElectionData> outgoingLE = new ConcurrentHashMap<Channel, LeaderElectionData>();

	public static ElectionManager getInstance(String id, int votes) {
		instance.compareAndSet(null, new ElectionManager(id, votes));
		return instance.get();
	}

	public static ElectionManager getInstance() {
		return instance.get();
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

	public void addConnectToThisNode(String nodeId, String host, int mgmtport) {

		destHost = host;
		destPort = mgmtport;
		destNodeId = nodeId;
	}

	public void setChannel(ChannelFuture f) {
		channel = f.channel();
	}

	/*
	 * public Channel connect() { // Start the connection attempt. ChannelFuture
	 * channelFuture = null; //ManagementHandler handler = null; EventLoopGroup
	 * group = new NioEventLoopGroup();
	 * 
	 * try { //handler = new ManagementHandler(); ManagementInitializer mi = new
	 * ManagementInitializer(false);
	 * 
	 * Bootstrap b = new Bootstrap();
	 * 
	 * b.group(group).channel(NioSocketChannel.class).handler(mi);
	 * b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
	 * b.option(ChannelOption.TCP_NODELAY, true);
	 * b.option(ChannelOption.SO_KEEPALIVE, true);
	 * 
	 * logger.info("destination host & port "+destHost + "& " +destPort);
	 * 
	 * // Make the connection attempt. channelFuture = b.connect(destHost,
	 * destPort); channelFuture.awaitUninterruptibly(5000l);
	 * 
	 * } catch (Exception ex) {
	 * logger.debug("failed to initialize the election connection");
	 * 
	 * }
	 * 
	 * 
	 * if (channelFuture != null && channelFuture.isDone() &&
	 * channelFuture.isSuccess()) return channelFuture.channel(); else throw new
	 * RuntimeException("Not able to establish connection to server"); }
	 */

	// Shaji: not used anymore
	public void addOutgoingChannel(String nodeId, String host, int mgmtport,
			Channel ch, SocketAddress sa) {
		if (!outgoingLE.containsKey(ch)) {
			LeaderElectionData electionData = new LeaderElectionData(nodeId,
					host, null, mgmtport);
			electionData.setConnection(ch, sa);
			outgoingLE.put(ch, electionData);

			// when the channel closes, remove it from the outgoingLE

		} else {
			logger.error(
					"Received a LE connection unknown to the server, node ID = ",
					nodeId);
			// TODO actions?
		}
	}

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

	private void send(Management msg) {

		// for(LeaderElectionData leData: outgoingLE.values()) {

		LeaderElectionData electionData = new LeaderElectionData(nodeId,
				destHost, null, destPort);
		electionData.setChannel(channel);
		

		try {
			electionData.channel.writeAndFlush(msg);
			participant = true;

			logger.info("Election message (" + nodeId + ") sent to "
					+ destNodeId + " at " + destHost);
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Failed to send leader election message");
		}
	}

	// }

	// Shaji: ElectionManager now extends Thread. Overiding run() method of
	// thread
	@Override
	public void run() {

		logger.info("starting Election manager");

		Management msg = null;

	//	while(true) {

	

		if (leader == null && !participant) {
			msg = generateLE(LeaderElection.VoteAction.NOMINATE, nodeId);
		}

		send(msg);

//	}

	 }

	/**
	 * @param args
	 */
	public void processRequest(LeaderElection req) {
		if (req == null)
			return;

		// logger.info("Received an election request..");

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

		} else if (req.getVote().getNumber() == VoteAction.DECLAREVOID_VALUE) {
			// no one was elected, I am dropping into standby mode
		} else if (req.getVote().getNumber() == VoteAction.DECLAREWINNER_VALUE) {
			// some node declared themself the leader
			logger.info("Winner declared!");
			leader = req.getNodeId();
		} else if (req.getVote().getNumber() == VoteAction.ABSTAIN_VALUE) {
			// for some reason, I decline to vote
		} else if (req.getVote().getNumber() == VoteAction.NOMINATE_VALUE) {
			logger.info("Received a nomination!");
			Management msg = null;
			// LCR
			int comparedToMe = req.getNodeId().compareTo(nodeId);
			if (comparedToMe == -1) {

				logger.info("My nodeId is higher..so nominating myself if I am not a participant yet!");
				if (!participant) {
					msg = generateLE(LeaderElection.VoteAction.NOMINATE, nodeId);
					send(msg);
				}

			} else if (comparedToMe == 1) {

				logger.info("Forwarding the nomination!");
				msg = generateLE(LeaderElection.VoteAction.NOMINATE,
						req.getNodeId());
				send(msg);

			} else if (comparedToMe == 0) {
				logger.info("I am the leader..");
				msg = generateLE(LeaderElection.VoteAction.DECLAREWINNER,
						nodeId);
				send(msg);
				leader = nodeId;

			}
		}
	}
}
