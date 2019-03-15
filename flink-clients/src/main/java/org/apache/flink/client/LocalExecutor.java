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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.PlanExecutor;
import org.apache.flink.api.common.Program;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.dag.DataSinkNode;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plandump.PlanJSONDumpGenerator;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.JobExecutorService;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterClient;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;

import java.util.ArrayList;
import java.util.List;

/**
 * A PlanExecutor that runs Flink programs on a local embedded Flink runtime instance.
 *
 * <p>By simply calling the {@link #executePlan(Plan)} method,
 * this executor still start up and shut down again immediately after the program finished.</p>
 *
 * <p>To use this executor to execute many dataflow programs that constitute one job together,
 * then this executor needs to be explicitly started, to keep running across several executions.</p>
 */
public class LocalExecutor extends PlanExecutor {

	private static final boolean DEFAULT_OVERWRITE = false;

	private static final int DEFAULT_TASK_MANAGER_NUM_SLOTS = -1;

	/** we lock to ensure singleton execution. */
	private final Object lock = new Object();

	/** Custom user configuration for the execution. */
	private final Configuration configuration;

	private ClusterClient clusterClient;

	/** Config value for how many slots to provide in the local cluster. */
	private int taskManagerNumSlots = DEFAULT_TASK_MANAGER_NUM_SLOTS;

	/** Config flag whether to overwrite existing files by default. */
	private boolean defaultOverwriteFiles = DEFAULT_OVERWRITE;

	public LocalExecutor() {
		this(new Configuration());
	}

	public LocalExecutor(Configuration configuration) {
		this.configuration = configuration;
		this.configuration.setString("execution.mode", "local");
	}

	// ------------------------------------------------------------------------
	//  Configuration
	// ------------------------------------------------------------------------

	public boolean isDefaultOverwriteFiles() {
		return defaultOverwriteFiles;
	}

	public void setDefaultOverwriteFiles(boolean defaultOverwriteFiles) {
		this.defaultOverwriteFiles = defaultOverwriteFiles;
	}

	public void setTaskManagerNumSlots(int taskManagerNumSlots) {
		this.taskManagerNumSlots = taskManagerNumSlots;
	}

	public int getTaskManagerNumSlots() {
		return this.taskManagerNumSlots;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public void start() throws Exception {
		synchronized (lock) {
			MiniCluster miniCluster = new MiniCluster(new
				MiniClusterConfiguration.Builder()
				.setConfiguration(configuration)
				.setNumTaskManagers(1)
				.setNumSlotsPerTaskManager(1).build());
			miniCluster.start();
			configuration.setInteger(RestOptions.PORT, miniCluster.getRestAddress().get().getPort());
			this.clusterClient = new MiniClusterClient(miniCluster, configuration);
		}
	}

	@Override
	public void stop() throws Exception {
		synchronized (lock) {
			if (clusterClient != null) {
				clusterClient.shutdown();
				clusterClient = null;
			}
		}
	}

	@Override
	public boolean isRunning() {
		synchronized (lock) {
			return clusterClient != null;
		}
	}

	/**
	 * Executes the given program on a local runtime and waits for the job to finish.
	 *
	 * <p>If the executor has not been started before, this starts the executor and shuts it down
	 * after the job finished. If the job runs in session mode, the executor is kept alive until
	 * no more references to the executor exist.</p>
	 *
	 * @param plan The plan of the program to execute.
	 * @return The net runtime of the program, in milliseconds.
	 *
	 * @throws Exception Thrown, if either the startup of the local execution context, or the execution
	 *                   caused an exception.
	 */
	@Override
	public JobExecutionResult executePlan(Plan plan) throws Exception {
		if (plan == null) {
			throw new IllegalArgumentException("The plan may not be null.");
		}

		synchronized (this.lock) {
			Optimizer pc = new Optimizer(new DataStatistics(), configuration);
			OptimizedPlan op = pc.compile(plan);
			return clusterClient.run(op, new ArrayList<>(), new ArrayList<>(),
				Thread.currentThread().getContextClassLoader()).getJobExecutionResult();
		}
	}

	/**
	 * Creates a JSON representation of the given dataflow's execution plan.
	 *
	 * @param plan The dataflow plan.
	 * @return The dataflow's execution plan, as a JSON string.
	 * @throws Exception Thrown, if the optimization process that creates the execution plan failed.
	 */
	@Override
	public String getOptimizerPlanAsJSON(Plan plan) throws Exception {
		final int parallelism = plan.getDefaultParallelism() == ExecutionConfig.PARALLELISM_DEFAULT ? 1 : plan.getDefaultParallelism();

		Optimizer pc = new Optimizer(new DataStatistics(), this.configuration);
		pc.setDefaultParallelism(parallelism);
		OptimizedPlan op = pc.compile(plan);

		return new PlanJSONDumpGenerator().getOptimizerPlanAsJSON(op);
	}

	@Override
	public void endSession(JobID jobID) throws Exception {
		// no op
	}

	// --------------------------------------------------------------------------------------------
	//  Static variants that internally bring up an instance and shut it down after the execution
	// --------------------------------------------------------------------------------------------

	/**
	 * Executes the given program.
	 *
	 * @param pa The program.
	 * @param args The parameters.
	 * @return The execution result of the program.
	 *
	 * @throws Exception Thrown, if either the startup of the local execution context, or the execution
	 *                   caused an exception.
	 */
	public static JobExecutionResult execute(Program pa, String... args) throws Exception {
		return execute(pa.getPlan(args));
	}

	/**
	 * Executes the given dataflow plan.
	 *
	 * @param plan The dataflow plan.
	 * @return The execution result.
	 *
	 * @throws Exception Thrown, if either the startup of the local execution context, or the execution
	 *                   caused an exception.
	 */
	public static JobExecutionResult execute(Plan plan) throws Exception {
		return null;
		//		return new LocalExecutor2().executePlan(plan);
	}

	/**
	 * Creates a JSON representation of the given dataflow's execution plan.
	 *
	 * @param plan The dataflow plan.
	 * @return The dataflow's execution plan, as a JSON string.
	 * @throws Exception Thrown, if the optimization process that creates the execution plan failed.
	 */
	public static String optimizerPlanAsJSON(Plan plan) throws Exception {
		final int parallelism = plan.getDefaultParallelism() == ExecutionConfig.PARALLELISM_DEFAULT ? 1 : plan.getDefaultParallelism();

		Optimizer pc = new Optimizer(new DataStatistics(), new Configuration());
		pc.setDefaultParallelism(parallelism);
		OptimizedPlan op = pc.compile(plan);

		return new PlanJSONDumpGenerator().getOptimizerPlanAsJSON(op);
	}

	/**
	 * Creates a JSON representation of the given dataflow plan.
	 *
	 * @param plan The dataflow plan.
	 * @return The dataflow plan (prior to optimization) as a JSON string.
	 */
	public static String getPlanAsJSON(Plan plan) {
		List<DataSinkNode> sinks = Optimizer.createPreOptimizedPlan(plan);
		return new PlanJSONDumpGenerator().getPactPlanAsJSON(sinks);
	}
}
