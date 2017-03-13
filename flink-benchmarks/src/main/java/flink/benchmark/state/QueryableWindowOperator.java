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
package flink.benchmark.state;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.typesafe.config.Config;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.migration.streaming.runtime.tasks.StreamTaskState;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.state.*;
import org.apache.flink.streaming.api.functions.util.StreamingFunctionUtils;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Some;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * Custom operator that computes windows and also makes that state directly queryable from
 * outside the streaming system via Akka.
 */
class QueryableWindowOperator
        extends AbstractStreamOperator<Tuple3<String, Long, Long>>
        implements OneInputStreamOperator<Tuple2<String, Long>, Tuple3<String, Long, Long>>,
        QueryableKeyValueState<String, String> /* key: campaign_id (String), value: long (window count) */ {

    private static final Logger LOG = LoggerFactory.getLogger(QueryableWindowOperator.class);

    private final long windowSize;

    private long lastWatermark = 0;

    // first key is the window key, i.e. the end of the window
    // second key is the key of the element (campaign_id), value is count
    // these are checkpointed, i.e. fault-tolerant

    // (campaign_id --> (window_end_ts, count)
    private Map<String, Map<Long, CountAndAccessTime>> windows;

    // we retain the windows for a bit after emitting to give
    // them time to propagate to their final storage location
    // they are not stored as part of checkpointing
    // private Map<Long, Map<Long, Long>> retainedWindows;
    // private long lastSetTriggerTime = 0;

    private final FiniteDuration timeout = new FiniteDuration(20, TimeUnit.SECONDS);
    private final RegistrationService registrationService;

    private static ActorSystem actorSystem;
    private static int actorSystemUsers = 0;
    private static final Object actorSystemLock = new Object();

    QueryableWindowOperator(
            long windowSize,
            RegistrationService registrationService) {
        this.windowSize = windowSize;
        this.registrationService = registrationService;
    }

    @Override
    public void open() throws Exception {
        super.open();

        LOG.info("Opening QueryableWindowOperator {}.", this);

        // don't overwrite if this was initialized in restoreState()
        if (windows == null) {
            windows = new HashMap<>();
        }

        // retainedWindows = new HashMap<>();

        if (registrationService != null) {
            registrationService.start();

            String hostname = registrationService.getConnectingHostname();
            String actorName = "responseActor_" + getRuntimeContext().getIndexOfThisSubtask() + System.nanoTime();

            initializeActorSystem(hostname);

            ActorRef responseActor = actorSystem.actorOf(Props.create(ResponseActor.class, this), actorName);

            String akkaURL = AkkaUtils.getAkkaURL(actorSystem, responseActor);

            registrationService.registerActor(getRuntimeContext().getIndexOfThisSubtask(), akkaURL);
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing QueyrableWindowOperator {}.", this);
        super.close();

        if (registrationService != null) {
            registrationService.stop();
        }

        closeActorSystem(timeout);
    }

    @Override
    public void processElement(StreamRecord<Tuple2<String, Long>> streamRecord) throws Exception {
        long timestamp = streamRecord.getValue().f1;
        long windowStart = timestamp - (timestamp % windowSize);
        long windowEnd = windowStart + windowSize;

        String campaign_id = streamRecord.getValue().f0;

        synchronized (windows) {
            Map<Long, CountAndAccessTime> window = windows.get(campaign_id);
            if (window == null) {
                window = new HashMap<>();
                windows.put(campaign_id, window);
            }

            CountAndAccessTime previous = window.get(windowEnd);
            if (previous == null) {
                previous = new CountAndAccessTime();
                window.put(windowEnd, previous);
                previous.count = 1L;
                previous.lastEventTime = timestamp;
            } else {
                previous.count++;
                previous.lastEventTime = Math.max(previous.lastEventTime, timestamp);
            }
            previous.lastAccessTime = System.currentTimeMillis();
        }
    }

    @Override
    public void processWatermark(Watermark watermark) throws Exception {
        // we'll keep state forever in the operator

	/*	StreamRecord<Tuple3<String, Long, Long>> result = new StreamRecord<>(null, -1);

		Iterator<Map.Entry<String, Map<Long, CountAndAccessTime>>> iterator = windows.entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry<String, Map<Long, CountAndAccessTime>> campaignWindows = iterator.next();
			for(Map.Entry<Long, CountAndAccessTime> window: campaignWindows.getValue().entrySet()) {
				if(window.getKey() < watermark.getTimestamp() && window.getKey() >= lastWatermark) {
					// emit window
					Tuple3<String, Long, Long> resultTuple = Tuple3.of(campaignWindows.getKey(), window.getKey(), window.getValue().count);
					output.collect(result.replace(resultTuple));
				}
			}
		}
		lastWatermark = watermark.getTimestamp(); **/
    }

    @Override
    public void processLatencyMarker(LatencyMarker latencyMarker) {
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);

        if (getOperatorStateBackend() != null) {
            OperatorStateCheckpointOutputStream out;

            try {
                out = context.getRawOperatorStateOutput();
            } catch (Exception e) {
                throw new Exception("Could not open raw operator state stream for " +
                        getOperatorName() + ".", e);
            }

            try {
                DataOutputViewStreamWrapper dov = new DataOutputViewStreamWrapper(out);
                dov.writeInt(windows.size());

                for (Map.Entry<String, Map<Long, CountAndAccessTime>> campaign : windows.entrySet()) {
                    dov.writeUTF(campaign.getKey());
                    dov.writeInt(campaign.getValue().size());

                    for (Map.Entry<Long, CountAndAccessTime> window : campaign.getValue().entrySet()) {
                        dov.writeLong(window.getKey());
                        dov.writeLong(window.getValue().count);
                        dov.writeLong(window.getValue().lastAccessTime);
                        dov.writeLong(window.getValue().lastEventTime);
                    }
                }
            } catch (Exception e) {
                throw new Exception("Could not write windows of " + getOperatorName() +
                        " to checkpoint state stream.", e);
            } finally {
                try {
                    out.close();
                } catch (Exception closeException) {
                    LOG.warn("Could not close raw operator state stream for {}. This " +
                            "might have prevented deleting some state data.", getOperatorName(), closeException);
                }
            }
        }
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        if (getOperatorStateBackend() != null) {
            this.windows.clear();

            for (StatePartitionStreamProvider streamProvider : context.getRawOperatorStateInputs()) {
                DataInputViewStreamWrapper div = new DataInputViewStreamWrapper(streamProvider.getStream());

                int numOfCampaign = div.readInt();
                for (int i = 0; i < numOfCampaign; i++) {
                    String campaign = div.readUTF();
                    Map<Long, CountAndAccessTime> window = new HashMap<>();
                    windows.put(campaign, window);

                    int numOfKeys = div.readInt();

                    for (int j = 0; j < numOfKeys; j++) {
                        long key = div.readLong();
                        CountAndAccessTime value = new CountAndAccessTime();
                        value.count = div.readLong();
                        value.lastAccessTime = div.readLong();
                        value.lastEventTime = div.readLong();
                        window.put(key, value);
                    }
                }
            }
        }
    }

    @Override
    public void notifyOfCompletedCheckpoint(long checkpointId) throws Exception {
        super.notifyOfCompletedCheckpoint(checkpointId);
    }

    /**
     * Note: This method has nothing to do with a regular getValue() implementation.
     * Its more designed as a remote debugger
     *
     * @throws WrongKeyPartitionException
     */
    @Override
    public String getValue(Long timestamp, String key) throws WrongKeyPartitionException {
        LOG.info("Query for timestamp {} and key {}", timestamp, key);
        if (Math.abs(key.hashCode() % getRuntimeContext().getNumberOfParallelSubtasks()) != getRuntimeContext().getIndexOfThisSubtask()) {
            throw new WrongKeyPartitionException("Key " + key + " is not part of the partition " +
                    "of subtask " + getRuntimeContext().getIndexOfThisSubtask());
        }

        if (windows == null) {
            return "No windows created yet";
        }

        synchronized (windows) {
            Map<Long, CountAndAccessTime> window = windows.get(key);
            if (window == null) {
                return "Key is not known. Available campaign IDs " + windows.keySet().toString();
            }
            if (timestamp == null) {
                // return the latency of the last window:
                TreeMap<Long, CountAndAccessTime> orderedMap = new TreeMap<>(window);
                Map.Entry<Long, CountAndAccessTime> first = orderedMap.lastEntry();
                return Long.toString(first.getValue().lastAccessTime - first.getValue().lastEventTime);
            } else {
                // query with timestamp:
                long windowStart = timestamp - (timestamp % windowSize);
                long windowEnd = windowStart + windowSize;
                CountAndAccessTime cat = window.get(windowEnd);
                if (cat == null) {
                    return "Timestamp not available";
                }
                return Long.toString(cat.lastAccessTime - cat.lastEventTime);
            }
        }
    }

    @Override
    public String toString() {
        RuntimeContext ctx = getRuntimeContext();

        return ctx.getTaskName() + " (" + ctx.getIndexOfThisSubtask() + "/" + ctx.getNumberOfParallelSubtasks() + ")";
    }

    private static class CountAndAccessTime implements Serializable {
        long count;
        long lastAccessTime;
        long lastEventTime;

        @Override
        public String toString() {
            return "CountAndAccessTime{count=" + count + ", lastAccessTime=" + lastAccessTime + '}';
        }
    }


    private static void initializeActorSystem(String hostname) throws UnknownHostException {
        synchronized (actorSystemLock) {
            if (actorSystem == null) {
                Configuration config = new Configuration();
                Option<scala.Tuple2<String, Object>> remoting = new Some<>(new scala.Tuple2<String, Object>(hostname, 0));

                Config akkaConfig = AkkaUtils.getAkkaConfig(config, remoting);

                LOG.info("Start actory system.");
                actorSystem = ActorSystem.create("queryableWindow", akkaConfig);
                actorSystemUsers = 1;
            } else {
                LOG.info("Actor system has already been started.");
                actorSystemUsers++;
            }
        }
    }

    private static void closeActorSystem(FiniteDuration timeout) {
        synchronized (actorSystemLock) {
            actorSystemUsers--;

            if (actorSystemUsers == 0 && actorSystem != null) {
                actorSystem.shutdown();
                actorSystem.awaitTermination(timeout);

                actorSystem = null;
            }
        }
    }
}
