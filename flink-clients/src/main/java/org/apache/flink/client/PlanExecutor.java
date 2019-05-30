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
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.JobWithJars;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plandump.PlanJSONDumpGenerator;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobResult;

import java.net.URL;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 *
 */
public class PlanExecutor {

	// ------------------------------------------------------------------------
	//  Config Options
	// ------------------------------------------------------------------------

	/**
	 * If true, all execution progress updates are not only logged, but also printed to System.out.
	 */
	private boolean printUpdatesToSysout = true;
	private boolean isRunning;
	private Configuration flinkConf;
	private FlinkPlanner flinkPlanner;
	private ClusterClientProvider clusterClientProvider;
	private ClusterClient clusterClient;

	/**
	 * Sets whether the executor should print progress results to "standard out" ({@link System#out}).
	 * All progress messages are logged using the configured logging framework independent of the value
	 * set here.
	 *
	 * @param printStatus True, to print progress updates to standard out, false to not do that.
	 */
	public void setPrintStatusDuringExecution(boolean printStatus) {
		this.printUpdatesToSysout = printStatus;
	}

	/**
	 * Gets whether the executor prints progress results to "standard out" ({@link System#out}).
	 *
	 * @return True, if the executor prints progress messages to standard out, false if not.
	 */
	public boolean isPrintingStatusDuringExecution() {
		return this.printUpdatesToSysout;
	}

	public PlanExecutor(Configuration flinkConf, ClusterClientProvider clusterClientProvider) {
		this.flinkConf = flinkConf;
		this.clusterClientProvider = clusterClientProvider;
	}

	// ------------------------------------------------------------------------
	//  Startup & Shutdown
	// ------------------------------------------------------------------------



	/**
	 * Starts the program executor. After the executor has been started, it will keep
	 * running until {@link #stop()} is called.
	 *
	 * @throws Exception Thrown, if the executor startup failed.
	 */
	public void start() throws Exception {
		this.clusterClient = clusterClientProvider.createClusterClient();
		this.flinkPlanner = new FlinkPlanner(flinkConf);
		this.isRunning = true;
	}

	/**
	 * Shuts down the plan executor and releases all local resources.
	 *
	 * <p>This method also ends all sessions created by this executor. Remote job executions
	 * may complete, but the session is not kept alive after that.</p>
	 *
	 * @throws Exception Thrown, if the proper shutdown failed.
	 */
	public void stop() throws Exception {
		this.isRunning = false;
		if (clusterClient != null) {
			this.clusterClient.shutdown();
			this.clusterClient = null;
		}
	}

	/**
	 * Checks if this executor is currently running.
	 *
	 * @return True is the executor is running, false otherwise.
	 */
	public boolean isRunning() {
		return this.isRunning;
	}

	public ClusterClient getClusterClient() {
		return clusterClient;
	}

	// ------------------------------------------------------------------------
	//  Program Execution
	// ------------------------------------------------------------------------

	/**
	 * Execute the given program.
	 *
	 * <p>If the executor has not been started before, then this method will start the
	 * executor and stop it after the execution has completed. This implies that one needs
	 * to explicitly start the executor for all programs where multiple dataflow parts
	 * depend on each other. Otherwise, the previous parts will no longer
	 * be available, because the executor immediately shut down after the execution.</p>
	 *
	 * @param plan The plan of the program to execute.
	 * @return The execution result, containing for example the net runtime of the program, and the accumulators.
	 *
	 * @throws Exception Thrown, if job submission caused an exception.
	 */
	public JobExecutionResult executePlan(Plan plan) throws Exception {
		JobGraph jobGraph = flinkPlanner.convertPlanToJobGraph(plan);
		CompletableFuture<JobSubmissionResult> jobSubmissionResultCompletableFuture =
			clusterClient.submitJob(jobGraph);
		CompletableFuture<JobResult> jobResultCompletableFuture =
			clusterClient.requestJobResult(jobSubmissionResultCompletableFuture.get().getJobID());
		return jobResultCompletableFuture.get().toJobExecutionResult(Thread.currentThread().getContextClassLoader());
	}

	public JobExecutionResult executePlan(Plan plan, List<URL> jarFiles, List<URL> globalClasspaths) throws Exception {
		JobWithJars p = new JobWithJars(plan, jarFiles, globalClasspaths);
		return clusterClient.run(p, 1).getJobExecutionResult();
	}

	public JobExecutionResult executeJobGraph(JobGraph jobGraph) throws Exception {
		CompletableFuture<JobSubmissionResult> jobSubmissionResultCompletableFuture =
			clusterClient.submitJob(jobGraph);
		CompletableFuture<JobResult> jobResultCompletableFuture =
			clusterClient.requestJobResult(jobSubmissionResultCompletableFuture.get().getJobID());
		return jobResultCompletableFuture.get().toJobExecutionResult(Thread.currentThread().getContextClassLoader());
	}

	/**
	 * Gets the programs execution plan in a JSON format.
	 *
	 * @param plan The program to get the execution plan for.
	 * @return The execution plan, as a JSON string.
	 *
	 * @throws Exception Thrown, if the executor could not connect to the compiler.
	 */
	public String getOptimizerPlanAsJSON(Plan plan) throws Exception {
		final int parallelism = plan.getDefaultParallelism() == ExecutionConfig.PARALLELISM_DEFAULT ?
			1 : plan.getDefaultParallelism();
		Optimizer pc = new Optimizer(new DataStatistics(), this.flinkConf);
		pc.setDefaultParallelism(parallelism);
		OptimizedPlan op = pc.compile(plan);
		return new PlanJSONDumpGenerator().getOptimizerPlanAsJSON(op);
	}

}
