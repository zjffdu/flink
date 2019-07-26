/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.environment;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.MissingTypeInfo;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.client.ClusterClientProvider;
import org.apache.flink.client.ClusterClientProviderFactory;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ContextEnvironment;
import org.apache.flink.client.program.JobWithJars;
import org.apache.flink.client.program.OptimizerPlanEnvironment;
import org.apache.flink.client.program.PreviewPlanEnvironment;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.source.ContinuousFileMonitoringFunction;
import org.apache.flink.streaming.api.functions.source.ContinuousFileReaderOperator;
import org.apache.flink.streaming.api.functions.source.FileMonitoringFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.functions.source.FileReadFunction;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.streaming.api.functions.source.FromIteratorFunction;
import org.apache.flink.streaming.api.functions.source.FromSplittableIteratorFunction;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.StatefulSequenceSource;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ShutdownHookUtil;
import org.apache.flink.util.SplittableIterator;
import org.apache.flink.util.StringUtils;

import com.esotericsoftware.kryo.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The StreamExecutionEnvironment is the context in which a streaming program is executed. A
 * {@link LocalStreamEnvironment} will cause execution in the current JVM, a
 * {@link RemoteStreamEnvironment} will cause execution on a remote setup.
 *
 * <p>The environment provides methods to control the job execution (such as setting the parallelism
 * or the fault tolerance/checkpointing parameters) and to interact with the outside world (data access).
 *
 * @see org.apache.flink.streaming.api.environment.LocalStreamEnvironment
 * @see org.apache.flink.streaming.api.environment.RemoteStreamEnvironment
 */
@Public
public class FlinkStreamEnvironment extends StreamExecutionEnvironment {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkStreamEnvironment.class);

	private ClusterClientProvider clusterClientProvider;

	private ClusterClient clusterClient;

	private Configuration flinkConf;

	/** The jar files that need to be attached to each job. */
	private final List<URL> jarFiles;

	/** The classpaths that need to be attached to each job. */
	private final List<URL> globalClasspaths;

	/** The savepoint restore settings for job execution. */
	private final SavepointRestoreSettings savepointRestoreSettings;

	static {

	}

	// --------------------------------------------------------------------------------------------
	// Constructor and Properties
	// --------------------------------------------------------------------------------------------

	public FlinkStreamEnvironment(Configuration flinkConf) throws Exception {
		this.flinkConf = flinkConf;
		this.jarFiles = new ArrayList<>();
		this.globalClasspaths = new ArrayList<>();
		this.savepointRestoreSettings = SavepointRestoreSettings.none();
		this.clusterClientProvider = ClusterClientProviderFactory.getClusterClientProvider(flinkConf);
		this.clusterClient = clusterClientProvider.createClusterClient();
		ShutdownHookUtil.addShutdownHook(clusterClientProvider::shutDownCluster,
			clusterClientProvider.getClass().getSimpleName(), LOG);
	}

	/**
	 * Triggers the program execution. The environment will execute all parts of
	 * the program that have resulted in a "sink" operation. Sink operations are
	 * for example printing results or forwarding them to a message queue.
	 *
	 * @param streamGraph the stream graph representing the transformations
	 * @return The result of the job execution, containing elapsed time and accumulators.
	 * @throws Exception which occurs during job execution.
	 */
	@Internal
	public JobExecutionResult execute(StreamGraph streamGraph) throws Exception {
		ClusterClient clusterClient = clusterClientProvider.createClusterClient();
		ClassLoader userCodeClassLoader = JobWithJars.buildUserCodeClassLoader(
			jarFiles, globalClasspaths, this.getClass().getClassLoader());

		return clusterClient.run(streamGraph, jarFiles, globalClasspaths, userCodeClassLoader, savepointRestoreSettings)
			.getJobExecutionResult();
	}

	public void stop() throws Exception {
		this.clusterClientProvider.shutDownCluster();
	}


}
