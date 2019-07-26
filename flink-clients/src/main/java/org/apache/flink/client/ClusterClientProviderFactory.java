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

import org.apache.flink.configuration.Configuration;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class ClusterClientProviderFactory {

	private static Map<Configuration, ClusterClientProvider> cache = new HashMap<>();

	public static ClusterClientProvider getClusterClientProvider(Configuration flinkConf) {
		if (cache.containsKey(flinkConf)) {
			return cache.get(flinkConf);
		}
		String executionMode = flinkConf.getString("flink.execution.mode", "local");
		String clusterClientProviderClass = null;

		if (executionMode.equalsIgnoreCase("local")) {
			clusterClientProviderClass = "org.apache.flink.client.LocalClusterClientProvider";
		} else if (executionMode.equalsIgnoreCase("remote")) {
			clusterClientProviderClass = "org.apache.flink.client.RemoteClusterClientProvider";
		} else if (executionMode.equalsIgnoreCase("yarn")) {
			clusterClientProviderClass = "org.apache.flink.yarn.YarnClusterClientProvider";
		} else {
			throw new RuntimeException("Invalid execution mode:" + executionMode);
		}
		try {
			Constructor constructor =
				Class.forName(clusterClientProviderClass).getConstructor(Configuration.class);
			ClusterClientProvider clusterClientProvider =
				(ClusterClientProvider) constructor.newInstance(flinkConf);
			cache.put(flinkConf, clusterClientProvider);
			return clusterClientProvider;
		} catch (Exception e) {
			throw new RuntimeException("Fail to create ClusterClientProvider: " + clusterClientProviderClass, e);
		}
	}

	public static void removeClusterClientProvider(Configuration flinkConf) {
		cache.remove(flinkConf);
	}
}
