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

package org.apache.flink.api.common;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Hooks for job submission and execution which is invoked in client side.
 */
@PublicEvolving
public interface JobListener {

	/**
	 * Inovoked when job is submitted to cluster successfully.
	 *
	 * @param jobId
	 */
	void onJobSubmitted(JobID jobId);

	/**
	 * Invoked when job is completed.
	 *
	 * @param jobResult
	 */
	void onJobExecuted(JobExecutionResult jobResult);

	/**
	 * Invoked when job is canceled.
	 *
	 * @param jobId
	 * @param savepointPath
	 */
	void onJobCanceled(JobID jobId, String savepointPath);
}
