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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.resources.JobResource;
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

	private String nodeId;
	private ServerConf configFile;
	LinkedBlockingDeque<JobBid> bidQueue;
	private static Map<String,JobBid> bidMap;

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
		this.bidMap = new HashMap<String,JobBid>();
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

		logger.info("\n**********\nRECEIVED NEW JOB PROPOSAL ID:"+req.getJobId()+"\n**********");
		Management.Builder mb = Management.newBuilder();
		JobBid.Builder jb = JobBid.newBuilder();
		jb.setJobId(req.getJobId());
		jb.setNameSpace(req.getNameSpace());
		jb.setOwnerId(Long.parseLong(nodeId));
		jb.setBid(1); // TODO randomize this

		mb.setJobBid(jb.build());

		Management jobBid = mb.build();

		String leaderId = ElectionManager.getInstance().getLeader();

		NodeDesc leaderNode = configFile.getNearest().getNearestNodes().get(leaderId);

		InetSocketAddress sa = new InetSocketAddress(leaderNode.getHost(),
				leaderNode.getMgmtPort());

		Channel ch = connect(sa);
		ch.writeAndFlush(jobBid);
//		ManagementQueue.enqueueResponse(jobBid, ch);

	}

	public Channel connect(InetSocketAddress sa) {
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

	/**
	 * a job bid for my job
	 * 
	 * @param req
	 *            The bid
	 */
	public void processRequest(JobBid req) {
		logger.info("\n**********\nRECEIVED NEW JOB BID"+"\n\n**********");
		// TODO queuing is not proper. it send the same job to all nodes that respond positively 
		//leader receives bid
		String leaderId = ElectionManager.getInstance().getLeader();
		if (leaderId != null && leaderId.equalsIgnoreCase(nodeId)) {
			bidQueue.add(req);
			if (bidMap.containsKey(req.getJobId())) {
				return;
			}
			bidMap.put(req.getJobId(), req);
			if(req.getBid() == 1) {
				Map<String, Request> requestMap = JobResource.getRequestMap();
				Request jobOperation = requestMap.get(req.getJobId());
				String toNodeId = req.getOwnerId()+"";
				/*Header header = ResourceUtil.buildHeader(jobOperation.getHeader().getRoutingId(), null , "request dispatched", 
						                                 jobOperation.getHeader().getOriginator(), null, bid.getOwnerId()+"");
				*/
				Request.Builder rb = Request.newBuilder(jobOperation);					
				Header.Builder hbldr = rb.getHeaderBuilder();
				hbldr.setToNode(toNodeId);
				rb.setHeader(hbldr.build());
				Request jobDispatched = rb.build();
				NodeDesc slaveNode = configFile.getNearest().getNode(toNodeId);

				InetSocketAddress sa = new InetSocketAddress(slaveNode.getHost(),
						slaveNode.getPort());

				Channel ch = connect(sa);
				
				ChannelQueue queue = QueueFactory.getInstance(ch);
				ch.writeAndFlush(jobDispatched);
//					queue.enqueueResponse(jobDispatched, ch);

			}
		
				
		}
		
		
		
		
		
		
	}
}
