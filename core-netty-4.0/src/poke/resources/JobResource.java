/*
 * copyright 2012, gash
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
package poke.resources;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.server.Server;
import poke.server.ServerInitializer;
import poke.server.conf.NodeDesc;
import poke.server.conf.ServerConf;
import poke.server.management.ManagementInitializer;
import poke.server.management.ManagementQueue;
import poke.server.management.managers.ElectionManager;
import poke.server.resources.Resource;
import poke.server.resources.ResourceUtil;
import poke.server.storage.MongoStorage;
import eye.Comm.JobDesc;
import eye.Comm.JobDesc.JobCode;
import eye.Comm.JobOperation;
import eye.Comm.JobProposal;
import eye.Comm.JobStatus;
import eye.Comm.Management;
import eye.Comm.NameValueSet;
import eye.Comm.Payload;
import eye.Comm.Ping;
import eye.Comm.PokeStatus;
import eye.Comm.Request;
import eye.Comm.JobOperation.JobAction;

public class JobResource implements Resource {

	protected static Logger logger = LoggerFactory.getLogger("server");

	private static final String signUp = "sign_up";
	private static final String logIn = "sign_in";
	private static final String listCourses = "listcourses";
	private static final String getDescription = "getdescription";
	private static Map<String, Request> requestMap = new HashMap<String, Request>();
	private static ServerConf configFile;

	public JobResource() {
		super();
		

	}

	public static Map<String, Request> getRequestMap() {
		return requestMap;
	}

	@Override
	public void setCfg(ServerConf file) {
		configFile = file;
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

	@Override
	public Request process(Request request) {
		
		Request reply = null;
		
		String leaderId = ElectionManager.getInstance().getLeader();
		
		

		if (Server.getMyId().equals(leaderId)) {
			//Reply processing only done by the leader

			if (request.getBody().hasJobStatus()) {
				
				logger.info("\n**********\nRECEIVED JOB STATUS"+"\n\n**********");
				logger.info("RequestMap size is "+requestMap.size());
				
				
				
				//JobStatus
				String jobId = request.getBody().getJobStatus().getJobId();
				logger.info("JobStatus.JobId: "+ jobId+"\n");
				if(requestMap.containsKey(jobId)){
					//request identified
					
					Request clientRequest = requestMap.get(jobId);
					String hostAddress = clientRequest.getHeader().getOriginator();
					String ip = hostAddress.split(":")[0];
					String port = hostAddress.split(":")[1];
					logger.info("\n*******hostAddress of client is:"+hostAddress+"\n");
					logger.info("\n*******ip of client is:"+ip+"\n");
					logger.info("\n*******port of client is:"+port+"\n");
					
					requestMap.remove(jobId);	
					
					InetSocketAddress sa = new InetSocketAddress(ip,Integer.parseInt(port));
					
					Channel ch = connectToPublic(sa);
					
					ch.writeAndFlush(request);
					
					
				}
				

			} else if(request.getBody().hasJobOp()) {
				logger.info("\n**********\n LEADER RECEIVED NEW JOB REQUEST"+"\n\n**********");
				logger.info("\n**********\n CREATING A JOB PROPOSAL"+"\n\n**********");


				// Job proposal
				
				JobOperation jobOp = request.getBody().getJobOp();
				
				logger.info("\nJobOperation.JobId: "+ jobOp.getJobId());
				requestMap.put(jobOp.getJobId(), request);

				Management.Builder mb = Management.newBuilder();
				JobProposal.Builder jbr = JobProposal.newBuilder();
				jbr.setJobId(jobOp.getJobId());
				jbr.setNameSpace(jobOp.getData().getNameSpace());
				jbr.setOwnerId(jobOp.getData().getOwnerId());
				jbr.setWeight(5);

				mb.setJobPropose(jbr.build());

				Management jobProposal = mb.build();

				for (NodeDesc nn : configFile.getNearest().getNearestNodes()
						.values()) {
					String destHost = nn.getHost();
					int destPort = nn.getMgmtPort();

					logger.info("destination host & port " + destHost + "& "
							+ destPort);

					InetSocketAddress sa = new InetSocketAddress(destHost,
							destPort);

					Channel ch = connectToManagement(sa);
					ch.writeAndFlush(jobProposal);
					// ManagementQueue.enqueueResponse(jobProposal,ch);

				}

			}
		}

		else {
			//Done by only a node who is not a leader
			logger.info("\n**********\n RECEIVED NEW JOB REQUEST"+"\n\n**********");
			
			JobOperation jobOp = request.getBody().getJobOp();

			
			if (jobOp.hasAction()) {

				JobAction jobAction = request.getBody().getJobOp().getAction();

				if (jobAction.equals(JobAction.ADDJOB)) {

					logger.info("ADDJOB received");

					// check if it is signUp
					if (signUp.equals(jobOp.getData().getNameSpace())) {
						// sign up
						logger.info("it is a sign up job");

						HashMap<String, String> credentials = new HashMap<String, String>();
						NameValueSet nvSet = jobOp.getData().getOptions();
						List<NameValueSet> nvList = null;

						logger.info("nodeCount of nvSet is "
								+ nvSet.getNodeCount());

						if (nvSet != null) {
							nvList = nvSet.getNodeList();
						}
						String email, password, firstName, lastName;
						email = password = firstName = lastName = null;
						for (NameValueSet nvPair : nvList) {
							credentials
									.put(nvPair.getName(), nvPair.getValue());
							if (nvPair.getName().equals("email"))
								email = nvPair.getValue();
							if (nvPair.getName().equals("firstName"))
								firstName = nvPair.getValue();
							if (nvPair.getName().equals("lastName"))
								lastName = nvPair.getValue();
							if (nvPair.getName().equals("password"))
								password = nvPair.getValue();
						}
						MongoStorage.addUser(email, password, firstName,
								lastName);

						logger.info("credentials: " + credentials.toString());

						// reply success
						Request.Builder rb = Request.newBuilder();
						// metadata
						rb.setHeader(ResourceUtil.buildHeader(
								request.getHeader().getRoutingId(), PokeStatus.SUCCESS, 
								"sign up successful", request.getHeader().getOriginator(),request.getHeader().getTag(),leaderId));						

						// payload
						Payload.Builder pb = Payload.newBuilder();
						JobStatus.Builder jb = JobStatus.newBuilder();
						jb.setStatus(PokeStatus.SUCCESS);
						jb.setJobId(jobOp.getJobId());
						jb.setJobState(JobDesc.JobCode.JOBRECEIVED);
						pb.setJobStatus(jb.build());

						rb.setBody(pb.build());

						reply = rb.build();

					}

					if (logIn.equals(jobOp.getData().getNameSpace())) {
						// login

						if (jobOp.getData().getOptions() != null) {
							List<NameValueSet> nvList = jobOp.getData()
									.getOptions().getNodeList();

							HashMap<String, String> credentials = new HashMap<String, String>();

							for (NameValueSet nvPair : nvList) {
								credentials.put(nvPair.getName(),
										nvPair.getValue());
							}
							logger.info("#######Credentials: "
									+ credentials.toString() + "#######");

							// TODO get login details from DB & validate them

							// reply
							Request.Builder rb = Request.newBuilder();

							// metadata
							rb.setHeader(ResourceUtil.buildHeaderFrom(
									request.getHeader(), PokeStatus.SUCCESS,
									"login successful"));

							// payload
							Payload.Builder pb = Payload.newBuilder();
							JobOperation.Builder jb = JobOperation.newBuilder();
							jb.setAction(JobAction.ADDJOB);
							jb.setJobId(jobOp.getJobId());

							pb.setJobOp(jb.build());
							rb.setBody(pb.build());

							reply = rb.build();
						}

					}

				} else if (jobAction.equals(JobAction.LISTJOBS)) {
					if (listCourses.equals(jobOp.getData().getNameSpace())) {
						// list all courses

						if (jobOp.getData().getOptions() != null) {
							List<NameValueSet> nvList = jobOp.getData()
									.getOptions().getNodeList();

							HashMap<String, String> filters = new HashMap<String, String>();

							for (NameValueSet nvPair : nvList) {
								filters.put(nvPair.getName(), nvPair.getValue());
							}

							// TODO retrieve list of courses from DB

							// list of courses should be a list of NameValueSet
							List<NameValueSet> courseList = new ArrayList<NameValueSet>();

							// reply list
							Request.Builder rb = Request.newBuilder();

							// metadata
							rb.setHeader(ResourceUtil.buildHeaderFrom(
									request.getHeader(), PokeStatus.SUCCESS,
									null));

							// payload
							Payload.Builder pb = Payload.newBuilder();
							JobOperation.Builder jb = JobOperation.newBuilder();
							jb.setAction(JobAction.LISTJOBS);
							jb.setJobId(jobOp.getJobId());

							JobDesc.Builder jDescBuilder = JobDesc.newBuilder();
							jDescBuilder.setJobId(jobOp.getJobId());
							jDescBuilder.setNameSpace(listCourses);
							jDescBuilder.setOwnerId(jobOp.getData()
									.getOwnerId());
							jDescBuilder.setStatus(JobCode.JOBRECEIVED);

							// set courseList using jDescBuilder.setOptions()

							jb.setData(jDescBuilder.build());

							pb.setJobOp(jb.build());
							rb.setBody(pb.build());

							reply = rb.build();
						}

					}
				}

			}
		}
		return reply;

	}

}
