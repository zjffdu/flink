package org.apache.flink.api.java;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;

/**
 * Hooks for job submission and execution which is invoked in client side.
 */
public interface JobListener {

	void onJobSubmitted(JobID jobId);

	void onJobExecuted(JobExecutionResult jobResult);

	void onJobCanceled(JobID jobId, String savepointPath);
}
