package org.apache.flink.runtime.minicluster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.NewClusterClient;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.clusterframework.messages.GetClusterStatusResponse;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobResult;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class MiniClusterClient extends ClusterClient implements NewClusterClient {

	private MiniCluster miniCluster;

	public MiniClusterClient(Configuration conf) throws Exception {
		super(conf);
	}

	public MiniClusterClient(MiniCluster miniCluster, Configuration conf) throws Exception {
		this(conf);
		this.miniCluster = miniCluster;
	}

	@Override
	public void waitForClusterToBeReady() {

	}

	@Override
	public String getWebInterfaceURL() {
		return null;
	}

	@Override
	public GetClusterStatusResponse getClusterStatus() {
		return null;
	}

	@Override
	public List<String> getNewMessages() {
		return null;
	}

	@Override
	public Object getClusterId() {
		return null;
	}

	@Override
	public int getMaxSlots() {
		return 0;
	}

	@Override
	public JobSubmissionResult submitJob(JobGraph jobGraph, ClassLoader classLoader) throws ProgramInvocationException {
		try {
			return miniCluster.executeJobBlocking(jobGraph);
		} catch (JobExecutionException e) {
			throw new ProgramInvocationException(e);
		} catch (InterruptedException e) {
			throw new ProgramInvocationException(e);
		}
	}

	@Override
	public boolean hasUserJarsInClassPath(List userJarFiles) {
		return false;
	}

	@Override
	public CompletableFuture<JobSubmissionResult> submitJob(@Nonnull JobGraph jobGraph) {
		return miniCluster.submitJob(jobGraph);
	}

	@Override
	public CompletableFuture<JobResult> requestJobResult(@Nonnull JobID jobId) {
		return null;
	}
}
