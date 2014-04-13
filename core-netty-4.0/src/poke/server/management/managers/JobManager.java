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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.resources.JobResource;
import poke.server.ServerInitializer;
import poke.server.conf.NodeDesc;
import poke.server.conf.ServerConf;
import poke.server.management.ManagementInitializer;
import poke.server.management.ManagementQueue;
import poke.server.management.ManagementQueue.ManagementQueueEntry;
import poke.server.queue.ChannelQueue;
import poke.server.queue.QueueFactory;
import poke.server.resources.ResourceUtil;
import eye.Comm.Header;
import eye.Comm.JobBid;
import eye.Comm.JobProposal;
import eye.Comm.Management;
import eye.Comm.Request;

/**
 * The job manager class is used by the system to assess and vote on a job. This
 * is used to ensure leveling of the servers take into account the diversity of
 * the network.
 * 
 * @author gash
 * 
 */
public class JobManager {
	protected static Logger logger = LoggerFactory.getLogger("management");
	protected static AtomicReference<JobManager> instance = new AtomicReference<JobManager>();
	private static final String competition = "competition";

	private String nodeId;
	private ServerConf configFile;
	LinkedBlockingDeque<JobBid> bidQueue;
	private static Map<String, JobBid> bidMap;
	private static final String getMoreCourses = "getmorecourses";
	private Map<String, Channel> channelMap = new HashMap<String, Channel>();

	public void addToChannelMap(String jobId, Channel ch) {
		channelMap.put(jobId, ch);
	}

	public static JobManager getInstance(String id, ServerConf configFile) {
		instance.compareAndSet(null, new JobManager(id, configFile));
		return instance.get();
	}

	public static JobManager getInstance() {
		return instance.get();
	}

	public JobManager(String nodeId, ServerConf configFile) {
		this.nodeId = nodeId;
		this.configFile = configFile;
		this.bidMap = new HashMap<String, JobBid>();
		bidQueue = new LinkedBlockingDeque<JobBid>();
	}

	/**
	 * a new job proposal has been sent out that I need to evaluate if I can run
	 * it
	 * 
	 * @param req
	 *            The proposal
	 */
	public void processRequest(JobProposal req) {

		String leaderId = ElectionManager.getInstance().getLeader();

		logger.info("\n**********\nRECEIVED NEW JOB PROPOSAL ID:"
				+ req.getJobId() + "\n**********");
		Management.Builder mb = Management.newBuilder();
		JobBid.Builder jb = JobBid.newBuilder();
		jb.setJobId(req.getJobId());
		jb.setNameSpace(req.getNameSpace());
		jb.setOwnerId(Long.parseLong(nodeId));
		int bid = (int)Math.floor(Math.random()+0.5);
		jb.setBid(bid); 

		mb.setJobBid(jb.build());

		Management jobBid = mb.build();

		if (req.getNameSpace().equals(getMoreCourses)||req.getNameSpace().equals(competition)) {

			Channel ch = channelMap.get(req.getJobId());
			ch.writeAndFlush(jobBid);
			channelMap.remove(req.getJobId());
			
			logger.info("\n**********sent a job bid with Job Id: **********"+req.getJobId());

		} else {

			NodeDesc leaderNode = configFile.getNearest().getNearestNodes()
					.get(leaderId);

			InetSocketAddress sa = new InetSocketAddress(leaderNode.getHost(),
					leaderNode.getMgmtPort());

			Channel ch = connectToManagement(sa);
			ch.writeAndFlush(jobBid);
			logger.info("\n**********sent a job bid with Job Id: **********"+req.getJobId());

		}
		// ManagementQueue.enqueueResponse(jobBid, ch);

	}

	public Channel connectToManagement(InetSocketAddress sa) {
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

			channelFuture = b.connect(sa);
			channelFuture.awaitUninterruptibly(5000l);

			

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

	/**
	 * a job bid for my job
	 * 
	 * @param req
	 *            The bid
	 */
	public void processRequest(JobBid req) {
		logger.info("\n**********\nRECEIVED NEW JOB BID" + "\n\n**********");
		logger.info("****************Bid value********"+req.getBid());
		
		String leaderId = ElectionManager.getInstance().getLeader();
		if (leaderId != null && leaderId.equalsIgnoreCase(nodeId)) {
			if (bidMap.containsKey(req.getJobId())) {
				return;
			}			
			if (req.getBid() == 1){
				bidQueue.add(req);
			    bidMap.put(req.getJobId(), req);
			}
			
			
			
			if (req.getBid() == 1) {
				Map<String, Request> requestMap = JobResource.getRequestMap();
				Request jobOperation = requestMap.get(req.getJobId());
				String toNodeId = req.getOwnerId() + "";
				
				Request.Builder rb = Request.newBuilder(jobOperation);
				Header.Builder hbldr = rb.getHeaderBuilder();
				hbldr.setToNode(toNodeId);
				hbldr.setRoutingId(Header.Routing.JOBS);
				rb.setHeader(hbldr.build());
				Request jobDispatched = rb.build();

				if (jobOperation.getBody().getJobOp().getData().getNameSpace()
						.equals(getMoreCourses)||jobOperation.getBody().getJobOp().getData().getNameSpace()
						.equals(competition)) {

					List<String> leaderList = new ArrayList<String>();
					leaderList.add(new String("192.168.0.61:5570"));
					leaderList.add(new String("192.168.0.60:5573"));
					
					//Channel ch = channelMap.get(req.getJobId());
					/*
					String destHost = ch.remoteAddress().
					logger.info("****************Job Request being dispatched to leaders********");
					
					ch.writeAndFlush(jobDispatched);
					channelMap.remove(req.getJobId());
					*/
					

					String destHost = null;
					int destPort = 0;
					for (String destination : leaderList) {
						String[] dest = destination.split(":");
						destHost = dest[0];
						destPort = Integer.parseInt(dest[1]);


						InetSocketAddress sa = new InetSocketAddress(destHost,
								destPort);

						Channel ch = connectToPublic(sa);
						ChannelQueue queue = QueueFactory.getInstance(ch);
						 
						logger.info("****************Job Request being dispatched to leaders********");

						queue.enqueueResponse(jobDispatched, ch);
					}

				} else {

					NodeDesc slaveNode = configFile.getNearest().getNode(
							toNodeId);

					InetSocketAddress sa = new InetSocketAddress(
							slaveNode.getHost(), slaveNode.getPort());

					Channel ch = connectToPublic(sa);

					ChannelQueue queue = QueueFactory.getInstance(ch);
					logger.info("****************Job Request being dispatched to slave node: ********"+toNodeId);
					queue.enqueueResponse(jobDispatched, ch);

				}

			}

		}

	}

	public Channel connectToPublic(InetSocketAddress sa) {
		// Start the connection attempt.
		ChannelFuture channelFuture = null;
		EventLoopGroup group = new NioEventLoopGroup();

		try {
			ServerInitializer initializer = new ServerInitializer(false);
			Bootstrap b = new Bootstrap();

			b.group(group).channel(NioSocketChannel.class).handler(initializer);
			b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
			b.option(ChannelOption.TCP_NODELAY, true);
			b.option(ChannelOption.SO_KEEPALIVE, true);

			channelFuture = b.connect(sa);
			channelFuture.awaitUninterruptibly(5000l);

			

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

}
