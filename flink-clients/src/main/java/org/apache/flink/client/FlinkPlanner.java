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

import org.apache.flink.api.common.Plan;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.runtime.jobgraph.JobGraph;

/**
 *
 */
public class FlinkPlanner {

	private Configuration flinkConf;

	public FlinkPlanner(Configuration flinkConf) {
		this.flinkConf = flinkConf;
	}

	public JobGraph convertPlanToJobGraph(Plan plan) {
		// TODO: Set job's default parallelism to max number of slots
		final int slotsPerTaskManager = flinkConf.getInteger(TaskManagerOptions.NUM_TASK_SLOTS, 1);
		final int numTaskManagers = flinkConf.getInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1);
		plan.setDefaultParallelism(slotsPerTaskManager * numTaskManagers);
		Optimizer pc = new Optimizer(new DataStatistics(), flinkConf);
		OptimizedPlan op = pc.compile(plan);
		JobGraphGenerator jgg = new JobGraphGenerator(flinkConf);
		return jgg.compileJobGraph(op, plan.getJobId());
	}
}
