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
	private static final String listCourses = "list_courses";
	private static Map<String,Request> requestMap;
	private static ServerConf configFile;

	public JobResource() {
		super();
		this.requestMap = new HashMap<String,Request>();

	}
		
	
	public static Map<String, Request> getRequestMap() {
		return requestMap;
	}




	@Override
	public void setCfg(ServerConf file) {
		configFile = file;
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

	@Override
	public Request process(Request request) {
		// TODO seperate the task for leader and other nodes 
			Request reply = null;
	
			JobOperation jobOp = request.getBody().getJobOp();
	
			logger.info("Job: " + jobOp.getJobId());	
			
			if (Server.getMyId().equals(ElectionManager.getInstance().getLeader())){
			//TODO the following
			//check if the jobId exists in requestMap
			//if yes, check the job status
			//if success, remove the job request from requestMap
			
			//Job proposal
			requestMap.put(jobOp.getJobId(),request);		
			
			
			Management.Builder mb = Management.newBuilder();
			JobProposal.Builder jbr = JobProposal.newBuilder();
			jbr.setJobId(jobOp.getJobId());
			jbr.setNameSpace(jobOp.getData().getNameSpace());
			jbr.setOwnerId(jobOp.getData().getOwnerId());
			jbr.setWeight(5);
			
			mb.setJobPropose(jbr.build());
			
			Management jobProposal = mb.build();
			
			
			for (NodeDesc nn : configFile.getNearest().getNearestNodes().values()) {
				String destHost = nn.getHost();
				int destPort = nn.getMgmtPort();
				
				logger.info("destination host & port " + destHost + "& " + destPort);
	
				
				InetSocketAddress sa = new InetSocketAddress(destHost, destPort);
				
				Channel ch = connect(sa);
				ch.writeAndFlush(jobProposal);
	//			ManagementQueue.enqueueResponse(jobProposal,ch);
			}
		}		
		
			else{

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

					logger.info("nodeCount of nvSet is " + nvSet.getNodeCount());
					
					if (nvSet != null) {						
						 nvList = nvSet.getNodeList();
					}
					String email,password,firstName,lastName;
					email=password=firstName=lastName=null;
					for (NameValueSet nvPair : nvList){
						credentials.put(nvPair.getName(), nvPair.getValue());
						if (nvPair.getName().equals("email")) email = nvPair.getValue();
						if (nvPair.getName().equals("firstName")) firstName = nvPair.getValue();
						if (nvPair.getName().equals("lastName")) lastName = nvPair.getValue();
						if (nvPair.getName().equals("password")) password = nvPair.getValue();
					}
					MongoStorage.addUser(email, password, firstName, lastName);
					

					logger.info("credentials: " + credentials.toString());

					// TODO Save user details to DB

					// reply success
					Request.Builder rb = Request.newBuilder();
					// metadata
					rb.setHeader(ResourceUtil.buildHeaderFrom(
							request.getHeader(), PokeStatus.SUCCESS, "login successful"));

					// payload
					Payload.Builder pb = Payload.newBuilder();
					JobOperation.Builder jb = JobOperation.newBuilder();
					jb.setAction(JobAction.ADDJOB);
					jb.setJobId(jobOp.getJobId());

					pb.setJobOp(jb.build());
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
							credentials
									.put(nvPair.getName(), nvPair.getValue());
						}
						logger.info("#######Credentials: " + credentials.toString()+"#######");

						// TODO get login details from DB & validate them

						// reply
						Request.Builder rb = Request.newBuilder();

						// metadata
						rb.setHeader(ResourceUtil.buildHeaderFrom(
								request.getHeader(), PokeStatus.SUCCESS, "login successful"));

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
								request.getHeader(), PokeStatus.SUCCESS, null));

						// payload
						Payload.Builder pb = Payload.newBuilder();
						JobOperation.Builder jb = JobOperation.newBuilder();
						jb.setAction(JobAction.LISTJOBS);
						jb.setJobId(jobOp.getJobId());

						JobDesc.Builder jDescBuilder = JobDesc.newBuilder();
						jDescBuilder.setJobId(jobOp.getJobId());
						jDescBuilder.setNameSpace(listCourses);
						jDescBuilder.setOwnerId(jobOp.getData().getOwnerId());
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
