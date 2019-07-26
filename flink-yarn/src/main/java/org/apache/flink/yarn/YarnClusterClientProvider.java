/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.yarn;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.flink.client.ClusterClientProvider;
import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.client.cli.CliFrontendParser;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;

/**
 *
 */
public class YarnClusterClientProvider extends ClusterClientProvider {

	private ClusterClient clusterClient;

	public YarnClusterClientProvider(Configuration flinkConf) {
		super(flinkConf);
	}

	@Override
	public void createCluster() throws Exception {
		clusterClient = deployNewYarnCluster(flinkConf);
	}

	@Override
	public ClusterClient createClusterClient() throws Exception {
		if (clusterClient == null) {
			createCluster();
		}
		return clusterClient;
	}

	@Override
	public void shutDownCluster() throws Exception {
		clusterClient.shutDownCluster();
		clusterClient.shutdown();
	}

	private ClusterClient deployNewYarnCluster(Configuration flinkConf) throws Exception {

		String[] args = new String[] {"-m", "yarn-cluster"};

		// number of task managers is required.
//		yarnConfig.containers match {
//			case Some(containers) => args ++= Seq("-yn", containers.toString)
//			case None =>
//				throw new IllegalArgumentException("Number of taskmanagers must be specified.")
//		}
//
//		// set configuration from user input
//		yarnConfig.jobManagerMemory.foreach((jmMem) => args ++= Seq("-yjm", jmMem.toString))
//		yarnConfig.taskManagerMemory.foreach((tmMem) => args ++= Seq("-ytm", tmMem.toString))
//		yarnConfig.name.foreach((name) => args ++= Seq("-ynm", name.toString))
//		yarnConfig.queue.foreach((queue) => args ++= Seq("-yqu", queue.toString))
//		yarnConfig.slots.foreach((slots) => args ++= Seq("-ys", slots.toString))

		CliFrontend frontend = new CliFrontend(flinkConf,
			CliFrontend.loadCustomCommandLines(flinkConf, System.getenv("FLINK_CONF_DIR")));

		Options commandOptions = CliFrontendParser.getRunCommandOptions();
		Options commandLineOptions = CliFrontendParser.mergeOptions(commandOptions,
			frontend.getCustomCommandLineOptions());
		CommandLine commandLine = CliFrontendParser.parse(commandLineOptions, args, true);
		CustomCommandLine customCLI = frontend.getActiveCustomCommandLine(commandLine);
		ClusterDescriptor clusterDescriptor = customCLI.createClusterDescriptor(commandLine);
		ClusterSpecification clusterSpecification = customCLI.getClusterSpecification(commandLine);
		return clusterDescriptor.deploySessionCluster(clusterSpecification);
	}

//	def fetchDeployedYarnClusterInfo(
//		configuration: Configuration,
//		configurationDirectory: String) = {
//
//		val args = ArrayBuffer[String](
//			"-m", "yarn-cluster"
//    )
//
//		val commandLine = CliFrontendParser.parse(
//			CliFrontendParser.getRunCommandOptions,
//			args.toArray,
//			true)
//
//		val frontend = new CliFrontend(
//			configuration,
//			CliFrontend.loadCustomCommandLines(configuration, configurationDirectory))
//		val customCLI = frontend.getActiveCustomCommandLine(commandLine)
//
//		val clusterDescriptor = customCLI
//			.createClusterDescriptor(commandLine)
//			.asInstanceOf[ClusterDescriptor[Any]]
//
//		val clusterId = customCLI.getClusterId(commandLine)
//
//		val clusterClient = clusterDescriptor.retrieve(clusterId)
//
//		if (clusterClient == null) {
//			throw new RuntimeException("Yarn Cluster could not be retrieved.")
//		}
//
//		val jobManager = AkkaUtils.getInetSocketAddressFromAkkaURL(
//			clusterClient.getClusterConnectionInfo.getAddress)
//
//		(jobManager.getHostString, jobManager.getPort, false, clusterClient)
//	}

}
