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

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.server.conf.ServerConf;
import poke.server.management.managers.HeartbeatManager.CloseHeartListener;

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

	/** @brief the number of votes this server can cast */
	private int votes = 1;
	
	//Shaji: added electionData object & modified constructors
	//private LeaderElectionData electionData;
	
	ConcurrentHashMap<Channel, ElectionData> outgoingLE = new ConcurrentHashMap<Channel, ElectionData>();

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
		
		//this.electionData = electionData;
	}
	
	//Shaji: this is called from NetworkManager.processRequest
	public void addOutgoingChannel(String nodeId, String host, int mgmtport, Channel ch, SocketAddress sa) {
		if (!outgoingLE.containsKey(ch)) {
			ElectionData electionData = new ElectionData(nodeId, host, null, mgmtport);
			electionData.setConnection(ch, sa);
			outgoingLE.put(ch, electionData);

			// when the channel closes, remove it from the outgoingLE
			
		} else {
			logger.error("Received a LE connection unknown to the server, node ID = ", nodeId);
			// TODO actions?
		}
	}
	
	//Shaji: ElectionManager now extends Thread. Overiding run() method of thread
	@Override
	public void run() {
		logger.info("starting Election manager");
		
		LeaderElection.Builder electionBuilder = LeaderElection.newBuilder();
		electionBuilder.setNodeId(nodeId);
		electionBuilder.setBallotId("0");
		electionBuilder.setDesc("Starting the election");
		electionBuilder.setVote(LeaderElection.VoteAction.ELECTION);
		LeaderElection electionMsg = electionBuilder.build();
		
		Management.Builder mBuilder = Management.newBuilder();
		mBuilder.setElection(electionMsg);
		Management msg = mBuilder.build();
		
		logger.info("outgoingLE size is "+outgoingLE.size());
		
		for(ElectionData leData: outgoingLE.values()) {
			
			try {
				leData.channel.writeAndFlush(msg);
				
				logger.info("Election message ("+nodeId+ ") sent to "+leData.getNodeId()+ " at "+leData.getHost());
			}
			catch (Exception e) {
				logger.error("Failed to send leader election message");
			}
		}
		
		
	}
	
	

	/**
	 * @param args
	 */
	public void processRequest(LeaderElection req) {
		if (req == null)
			return;
		
		logger.info("Received an election request..");

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
			// no one was elected, I am dropping into standby mode`
		} else if (req.getVote().getNumber() == VoteAction.DECLAREWINNER_VALUE) {
			// some node declared themself the leader
		} else if (req.getVote().getNumber() == VoteAction.ABSTAIN_VALUE) {
			// for some reason, I decline to vote
		} else if (req.getVote().getNumber() == VoteAction.NOMINATE_VALUE) {
			int comparedToMe = req.getNodeId().compareTo(nodeId);
			if (comparedToMe == -1) {
				// Someone else has a higher priority, forward nomination
				// TODO forward
			} else if (comparedToMe == 1) {
				// I have a higher priority, nominate myself
				// TODO nominate myself
			}
		}
	}
}
