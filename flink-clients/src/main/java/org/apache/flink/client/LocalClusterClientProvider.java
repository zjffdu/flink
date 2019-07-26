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

package org.apache.flink.client;

import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.MiniClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;

/**
 *
 */
public class LocalClusterClientProvider extends ClusterClientProvider {

	private MiniCluster cluster;

	public LocalClusterClientProvider(Configuration flinkConf) {
		super(flinkConf);
	}

	@Override
	public void createCluster() throws Exception {
		flinkConf.setInteger(JobManagerOptions.PORT, 0);
		MiniClusterConfiguration miniClusterConfig = new MiniClusterConfiguration.Builder()
			.setConfiguration(flinkConf)
			.build();
		this.cluster = new MiniCluster(miniClusterConfig);
		cluster.start();
	}

	@Override
	public ClusterClient createClusterClient() throws Exception {
		if (cluster == null) {
			createCluster();
		}
		return new MiniClusterClient(flinkConf, cluster);
	}

	@Override
	public void shutDownCluster() throws Exception {
		this.cluster.close();
		this.cluster = null;
		ClusterClientProviderFactory.removeClusterClientProvider(flinkConf);
	}
}
