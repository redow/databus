package com.linkedin.databus2.producers.db;

/*
 *
 * Copyright 2013 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.log4j.Logger;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import com.linkedin.databus.core.DbusEventBufferAppendable;
import com.linkedin.databus.core.UnsupportedKeyException;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.seq.MaxSCNWriter;
import com.linkedin.databus2.producers.EventCreationException;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig.ChunkingType;

public class HbaseWALEventReader implements SourceDBEventReader {
	public static final String MODULE = HbaseWALEventReader.class.getName();

	private final String _name;
	private final List<HbaseWALMonitoredSouceInfo> _sources; // make sure
																// all
																// source in
																// ONE hbase
																// cluster...
	private HTable hTable = null;
	private final DbusEventBufferAppendable _eventBuffer;
	private final boolean _enableTracing;

	/** Logger for error and debug messages. */
	private final Logger _log;
	private final Logger _eventsLog;

	private final DbusEventsStatisticsCollector _relayInboundStatsCollector;
	private final MaxSCNWriter _maxScnWriter;
	private long _lastSeenEOP = EventReaderSummary.NO_EVENTS_SCN;

	private static final String MAX_SCN_TABLE = "dbus4hbase_scn";
	private static final String MAX_SCN_ROWKEY = "1";

	public HbaseWALEventReader(String name,
			List<HbaseWALMonitoredSouceInfo> sources,
			DbusEventBufferAppendable eventBuffer, boolean enableTracing,
			DbusEventsStatisticsCollector dbusEventsStatisticsCollector,
			MaxSCNWriter maxScnWriter, long slowQuerySourceThreshold,
			ChunkingType chunkingType, long txnsPerChunk, long scnChunkSize,
			long chunkedScnThreshold, long maxScnDelayMs) {
		List<HbaseWALMonitoredSouceInfo> sourcesTemp = new ArrayList<HbaseWALMonitoredSouceInfo>();
		sourcesTemp.addAll(sources);
		_name = name;
		_sources = Collections.unmodifiableList(sourcesTemp);
		_eventBuffer = eventBuffer;
		_enableTracing = enableTracing;
		_relayInboundStatsCollector = dbusEventsStatisticsCollector;
		_maxScnWriter = maxScnWriter;
		_log = Logger.getLogger(getClass().getName() + "." + _name);
		_log.info("HbaseLogEventReader is starting...");
		_eventsLog = Logger
				.getLogger("com.linkedin.databus2.producers.db.events." + _name);
		try {
			if (hTable == null) {
				_log.info("Creating MAX_SCN_TABLE for each table...");
				// make sure all the table in sourceinfo in ONE HbaseCluster
				String hbaseZkQuorum = _sources.get(0).getZkQuorum();
				String hbaseZkPort = sources.get(0).getZkPort();
				Configuration conf = HBaseConfiguration.create();
				conf.set("hbase.zookeeper.quorum", hbaseZkQuorum);
				conf.set("hbase.zookeeper.property.clientPort", hbaseZkPort);
				hTable = new HTable(conf, MAX_SCN_TABLE);
			}
		} catch (Exception e) {
			_log.error("Error creating data source", e);
			hTable = null;
		}
	}

	@Override
	public ReadEventCycleSummary readEventsFromAllSources(long sinceSCN)
			throws DatabusException, EventCreationException,
			UnsupportedKeyException {
		boolean eventBufferNeedsRollback = true;
		boolean debugEnabled = _log.isDebugEnabled();
		List<EventReaderSummary> summaries = new ArrayList<EventReaderSummary>();
		long cycleStartTS = System.currentTimeMillis();
		_eventBuffer.startEvents();
		if (sinceSCN <= 0) {
			// sinceSCN = getMaxSCN(); TODO i don't konw why here to get the max
			// scn by redow
			sinceSCN = 0;
			_log.debug("sinceSCN was <= 0. Overriding with the current max SCN="
					+ sinceSCN);
			_eventBuffer.setStartSCN(sinceSCN);
		}
		long endOfPeriodScn = EventReaderSummary.NO_EVENTS_SCN;
		Map<String, Map<String, List<KeyValue>>> keyValueMap = new HashMap<String, Map<String, List<KeyValue>>>();
		Map<String, Long> maxSCNMap = new HashMap<String, Long>();
		Set<String> WALPathSet = new HashSet<String>();
		for (HbaseWALMonitoredSouceInfo source : _sources) {
			String tableName = source.getEventTable();
			if (keyValueMap.containsKey(tableName) == false) {
				keyValueMap.put(tableName,
						new HashMap<String, List<KeyValue>>());
			}
			for (String columnFamily : source.getColumnFamily()) {
				if (keyValueMap.get(tableName).containsKey(columnFamily) == false) {
					keyValueMap.get(tableName).put(columnFamily,
							new ArrayList<KeyValue>());
				}
			}
			String identString = source.getHDFSIP() + "$"
					+ source.getHDFSPort() + "$" + source.getWALPath();
			WALPathSet.add(identString);
			maxSCNMap.put(source.getEventTable(), -1L);
		}
		getMaxSCN(maxSCNMap);
		long startTS = System.currentTimeMillis();
		for (String identString : WALPathSet) {
			fillKeyValueMap(keyValueMap, sinceSCN, identString.substring(0,
					identString.indexOf("$")),
					identString.substring(identString.indexOf("$") + 1,
							identString.lastIndexOf("$")),
					identString.substring(identString.lastIndexOf("$") + 1),
					maxSCNMap);
		}
		setMaxlogSCN(maxSCNMap);
		long endTS = System.currentTimeMillis();
		for (HbaseWALMonitoredSouceInfo source : _sources) {
			Map<String, List<KeyValue>> kvResults = keyValueMap.get(source.getEventTable());
			long queryExecTime = (endTS - startTS) / _sources.size();
			int numRowsFetched = 0;
			long totalEventSerializeTime = 0;
			long totalEventSize = 0;
			long tsWindowStart = Long.MAX_VALUE;
			long tsWindowEnd = Long.MIN_VALUE;
			long endOfPeriodSCN = EventReaderSummary.NO_EVENTS_SCN;
			for (String r : kvResults.keySet()) {
				List<KeyValue> kvList = kvResults.get(r);
				for (KeyValue kv : kvList) {
					long scn = kv.getTimestamp();
					long timestamp = scn;
					if (scn < sinceSCN)
						continue;
					tsWindowEnd = Math.max(timestamp, tsWindowEnd);
					tsWindowStart = Math.min(timestamp, tsWindowStart);
					long tsStart = System.currentTimeMillis();
					long eventSize = source.getFactory().createAndAppendEvent(
							scn, timestamp, kv, _eventBuffer, _enableTracing,
							_relayInboundStatsCollector);

					totalEventSerializeTime += System.currentTimeMillis()
							- tsStart;
					totalEventSize += eventSize;
					endOfPeriodSCN = Math.max(endOfPeriodSCN, scn);
					numRowsFetched++;
				}
			}
			EventReaderSummary summary = new EventReaderSummary(
					source.getSourceId(), source.getSourceName(),
					endOfPeriodSCN, numRowsFetched, totalEventSize,
					queryExecTime, totalEventSerializeTime, tsWindowStart,
					tsWindowEnd, queryExecTime);
			if (summary != null && summary.getNumberOfEvents() > 0) {
				summaries.add(summary);
				endOfPeriodScn = Math.max(endOfPeriodScn,
						summary.getEndOfPeriodSCN());
				endTS = System.currentTimeMillis();
				source.getStatisticsBean().addTimeOfLastDBAccess(endTS);
				if (_eventsLog.isDebugEnabled()
						|| (_eventsLog.isInfoEnabled() && summary
								.getNumberOfEvents() > 0)) {
					_eventsLog.info(summary.toString());
				}
				source.getStatisticsBean().addEventCycle(
						summary.getNumberOfEvents(), endTS - startTS,
						summary.getSizeOfSerializedEvents(),
						summary.getEndOfPeriodSCN());
			} else {
				source.getStatisticsBean().addEmptyEventCycle();
			}
		}
		_lastSeenEOP = Math.max(_lastSeenEOP,
				Math.max(endOfPeriodScn, sinceSCN));
		if (endOfPeriodScn == EventReaderSummary.NO_EVENTS_SCN) {
			if (debugEnabled) {
				_log.debug("No events processed. Read max SCN from txlog table for endOfPeriodScn. endOfPeriodScn="
						+ endOfPeriodScn);
			}
			eventBufferNeedsRollback = true;
		} else {
			_eventBuffer.endEvents(endOfPeriodScn, _relayInboundStatsCollector);
			if (debugEnabled) {
				_log.debug("End of events: " + endOfPeriodScn
						+ " windown range= " + _eventBuffer.getMinScn() + ","
						+ _eventBuffer.lastWrittenScn());
			}
			// no need to roll back
			eventBufferNeedsRollback = false;
		}

		if (endOfPeriodScn != EventReaderSummary.NO_EVENTS_SCN) {
			if (null != _maxScnWriter && (endOfPeriodScn != sinceSCN)) {
				_maxScnWriter.saveMaxScn(endOfPeriodScn);
			}
			for (HbaseWALMonitoredSouceInfo source : _sources) {
				// update maxDBScn here
				source.getStatisticsBean().addMaxDBScn(endOfPeriodScn);
				source.getStatisticsBean().addTimeOfLastDBAccess(
						System.currentTimeMillis());
			}
		}
		long cycleEndTS = System.currentTimeMillis();
		ReadEventCycleSummary summary = new ReadEventCycleSummary(_name,
				summaries, Math.max(endOfPeriodScn, sinceSCN),
				(cycleEndTS - cycleStartTS));
		if (eventBufferNeedsRollback) {
			if (_log.isDebugEnabled()) {
				_log.debug("Rolling back the event buffer because eventBufferNeedsRollback is true.");
			}
			_eventBuffer.rollbackEvents();
		}
		return summary;
	}

	@Override
	public List<? extends EventSourceStatisticsIface> getSources() {
		return _sources;
	}

	public static void main(String[] args) throws IOException {
		Configuration cfg = new Configuration();
		FileSystem fs = null;
		Map<String, Map<String, List<KeyValue>>> keyValueMap = new HashMap<String, Map<String, List<KeyValue>>>();
		try {
			fs = FileSystem.get(
					URI.create("hdfs://" + "172.21.1.203" + ':' + "8020"
							+ "/hbase/.logs"), cfg);
			FileStatus[] fileStatus = fs.listStatus(new Path("/hbase/.logs"));
			Path[] listPath = FileUtil.stat2Paths(fileStatus);
			for (Path p : listPath) {
				HLog.Reader reader = HLog.getReader(fs, p, cfg);
				HLog.Entry entry = null;
				while ((entry = reader.next()) != null) {
					WALEdit wa = entry.getEdit();
					HLogKey wk = entry.getKey();
					if (keyValueMap.containsKey(wk.getTablename()) == false)
						continue;
					List<KeyValue> kvList = wa.getKeyValues();
					Map<String, List<KeyValue>> map = keyValueMap.get(wk
							.getTablename());
					for (KeyValue kv : kvList) {
						if (kv.getTimestamp() < 1400000000)
							continue;
						if (map.containsKey(Bytes.toString(kv.getFamily())) == false)
							continue;
						map.get(Bytes.toString(kv.getFamily())).add(kv);
					}
				}
			}
		} catch (Throwable e) {
			System.out.println(e.toString());
		}
	}

	private void fillKeyValueMap(
			Map<String, Map<String, List<KeyValue>>> keyValueMap,
			long sinceSCN, String hdfsIp, String hdfsPort, String WALPath,
			Map<String, Long> maxSCNMap) {
		Configuration cfg = new Configuration();
		FileSystem fs = null;
		cfg.set("fs.hdfs.impl",
				org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		cfg.set("fs.file.impl",
				org.apache.hadoop.fs.LocalFileSystem.class.getName());
		long maxSCN = -1;
		try {
			fs = FileSystem.get(
					URI.create("hdfs://" + hdfsIp + ':' + hdfsPort + WALPath),
					cfg);
			FileStatus[] fileStatus = fs.listStatus(new Path(WALPath));
			Path[] listPath = FileUtil.stat2Paths(fileStatus);
			for (Path dirPath : listPath) {
				FileStatus[] subFileStatus = fs.listStatus(new Path(dirPath
						.toString()));
				Path[] subListPath = FileUtil.stat2Paths(subFileStatus);
				for (Path p : subListPath) {
					HLog.Reader reader = HLog.getReader(fs, p, cfg);
					HLog.Entry entry = null;
					while ((entry = reader.next()) != null) {
						WALEdit wa = entry.getEdit();
						HLogKey wk = entry.getKey();
						String tableName = Bytes.toString(wk.getTablename());
						if (keyValueMap.containsKey(tableName) == false)
							continue;
						List<KeyValue> kvList = wa.getKeyValues();
						Map<String, List<KeyValue>> map = keyValueMap.get(tableName);
						for (KeyValue kv : kvList) {
							if (kv.getTimestamp() < sinceSCN)
								continue;
							if (kv.getTimestamp() > maxSCNMap.get(tableName)) {
								maxSCNMap.put(tableName, kv.getTimestamp());
								if (maxSCN < kv.getTimestamp()) {
									maxSCN = kv.getTimestamp();
								}
							}
							if (map.containsKey(Bytes.toString(kv.getFamily())) == false)
								continue;
							map.get(Bytes.toString(kv.getFamily())).add(kv);
						}
					}
				}
			}
		} catch (IOException e) {
			_eventsLog.error("", e);
		}
		maxSCNMap.put(MAX_SCN_TABLE, maxSCN);
	}

	private void getMaxSCN(Map<String, Long> maxSCNMap) // read the max_scn of
														// all hbase table
	{
		if (hTable == null) {
			_log.error("Giving up to get maxSCN. Because the htable is null...");
			return;
		}
		_log.info("Querying Max_Scn Table...");
		Get get = new Get(Bytes.toBytes("1")); // (1,max_scn:"")
		try {
			Result rs = hTable.get(get);
			if (rs.isEmpty()) {
				_log.warn("Nothing found in Max_Scn Table...");
				return;
			}
			List<KeyValue> kvList = rs.list();
			for (KeyValue kv : kvList) {
				String family = Bytes.toString(kv.getFamily());
				String column = Bytes.toString(kv.getQualifier());
				long scn = Bytes.toLong(kv.getValue());
				if (family.equals("table")) {
					maxSCNMap.put(column, scn);
				} else {
					maxSCNMap.put(MAX_SCN_TABLE, scn);
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			_log.error("I can't read max_scn from max_scn_table");
		}
		return;
	}

	private void setMaxlogSCN(Map<String, Long> maxSCNMap) {
		if (hTable == null) {
			_log.error("Giving up to set maxSCN. Because the htable is null...");
			return;
		}
		Put put = new Put(Bytes.toBytes("1"));
		for (String tableName : maxSCNMap.keySet()) {
			long scn = maxSCNMap.get(tableName);
			if (scn < 0)
				continue;
			if (tableName.equals(MAX_SCN_TABLE)) {
				put.add(Bytes.toBytes("max_scn"), Bytes.toBytes(""),
						Bytes.toBytes(scn));
			} else {
				put.add(Bytes.toBytes("table"), Bytes.toBytes(tableName),
						Bytes.toBytes(scn));
			}
		}
		try {
			hTable.put(put);
		} catch (IOException e) {
			_log.error("", e);
		}
	}
}
