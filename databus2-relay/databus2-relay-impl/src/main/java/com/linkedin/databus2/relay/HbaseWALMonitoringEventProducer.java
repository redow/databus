package com.linkedin.databus2.relay;

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
import java.util.List;

import javax.management.MBeanServer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.linkedin.databus.monitoring.mbean.DBStatistics;
import com.linkedin.databus.monitoring.mbean.DBStatisticsMBean;
import com.linkedin.databus.monitoring.mbean.SourceDBStatistics;
import com.linkedin.databus2.producers.EventProducer;
import com.linkedin.databus2.producers.db.EventReaderSummary;
import com.linkedin.databus2.producers.db.HbaseWALMonitoredSouceInfo;

public class HbaseWALMonitoringEventProducer implements EventProducer, Runnable {

	private final List<HbaseWALMonitoredSouceInfo> _sources;
	private final String _name;

	protected enum MonitorState {
		INIT, RUNNING, PAUSE, SHUT
	}

	MonitorState _state;
	static protected final long MAX_SCN_POLL_TIME = 10 * 60 * 1000;
	static protected final long PER_SRC_MAX_SCN_POLL_TIME = 30 * 1000;
	private Thread _curThread;
	private final String _uri;
	/** Logger for error and debug messages. */
	private final Logger _log = Logger.getLogger(getClass());
	private String _schema;
	private HTable hTable = null;
	// stored as state ; as it may be required for graceful shutdown ; in case
	// of exception
	private final DBStatistics _dbStats;
	private final MBeanServer _mbeanServer;
	private static final String MAX_SCN_TABLE = "dbus4hbase_maxscn";

	public HbaseWALMonitoringEventProducer(String name, String dbname,
			String uri, List<HbaseWALMonitoredSouceInfo> sources,
			MBeanServer mbeanServer) {
		_log.info("HbaseWALMonitoringEven is starting...");
		_sources = sources;
		_name = name;
		_state = MonitorState.INIT;
		_uri = uri;
		_schema = null;
		_mbeanServer = mbeanServer;

		_dbStats = new DBStatistics(dbname);
		for (HbaseWALMonitoredSouceInfo sourceInfo : sources) {
			if (null == _schema) {
				// all logical sources have same schema
				_schema = sourceInfo.getEventSchema() == null ? "" : sourceInfo
						.getEventSchema() + ".";
				_log.info("Reading source: _schema =  |" + _schema + "|");

			}
			_dbStats.addSrcStats(new SourceDBStatistics(sourceInfo
					.getSourceName()));
		}
		_dbStats.registerAsMbean(_mbeanServer);
		_log.info("Created " + name + " producer ");
	}

	@Override
	public String getName() {
		return _name;
	}

	@Override
	public long getSCN() {
		return 0;
	}

	public DBStatisticsMBean getDBStats() {
		return _dbStats;
	}

	public void unregisterMBeans() {
		_dbStats.unregisterAsMbean(_mbeanServer);
	}

	@Override
	public synchronized void start(long sinceSCN) {
		if (_state == MonitorState.INIT || _state == MonitorState.SHUT) {
			_state = MonitorState.RUNNING;
			_curThread = new Thread(this);
			_curThread.start();
		}
	}

	@Override
	public synchronized boolean isRunning() {
		return _state == MonitorState.RUNNING;
	}

	@Override
	public synchronized boolean isPaused() {
		return false;
	}

	@Override
	public synchronized void unpause() {
		_state = MonitorState.RUNNING;
	}

	@Override
	public synchronized void pause() {
	}

	@Override
	public synchronized void shutdown() {
		_state = MonitorState.SHUT;
	}

	protected boolean createHTable() { // create HBaseTable to store max_scn for
										// all hbase table.
		try {
			if (hTable == null) {
				_log.info("Creating MAX_SCN_TABLE for each table...");
				String uri = _uri;
				uri = uri.substring(6);
				String hbaseZkQuorum = null;
				String hbaseZkPort = null;

				int idxColon = uri.indexOf(':');
				int idxSlash = uri.indexOf('/');

				uri.substring(idxSlash + 1);
				hbaseZkQuorum = uri.substring(0, idxColon);
				hbaseZkPort = uri.substring(idxColon + 1, idxSlash);
				Configuration conf = HBaseConfiguration.create();
				conf.set("hbase.zookeeper.quorum", hbaseZkQuorum);
				conf.set("hbase.zookeeper.property.clientPort", hbaseZkPort);
				hTable = new HTable(conf, MAX_SCN_TABLE);
			}
		} catch (Exception e) {
			_log.error("Error creating data source", e);
			hTable = null;
			return false;
		}
		return true;
	}

	@Override
	public void run() { // get the max_scn of all tables and max_scn of each
						// tables store them in dbstats...
		// check state and behave accordingly
		if (createHTable()) {
			do {
				try {
					long maxDBScn = getMaxlogSCN();
					_log.info("Max DB Scn =  " + maxDBScn);
					_dbStats.setMaxDBScn(maxDBScn);
					for (HbaseWALMonitoredSouceInfo source : _sources) {
						Get get = new Get(Bytes.toBytes("1"));
						// get max scn - exactly one row;
						Result rs = hTable.get(get);
						if (rs.isEmpty() == false) {
							long maxScn = Bytes.toLong((rs.getValue(
									Bytes.toBytes("table"),
									Bytes.toBytes(source.getEventTable()))));
							_log.info("Source: " + source.getSourceId()
									+ " Max Scn=" + maxScn);
							_dbStats.setSrcMaxScn(source.getSourceName(),
									maxScn);
						}
						if (_state != MonitorState.SHUT) {
							Thread.sleep(PER_SRC_MAX_SCN_POLL_TIME);
						}
					}
					if (_state != MonitorState.SHUT) {
						Thread.sleep(MAX_SCN_POLL_TIME);
					}
				} catch (InterruptedException e) {
					_log.error("Exception trace", e);
					shutDown();
				} catch (IOException e) {
					_log.error(e.getStackTrace());
					shutDown();
				}
			} while (_state != MonitorState.SHUT);
			_log.info("Shutting down dbMonitor thread");
		}
	}

	protected synchronized void shutDown() {
		_state = MonitorState.SHUT;
		_curThread = null;
	}

	private long getMaxlogSCN() // read the max_scn of all hbase table
	{
		_log.info("Querying Max_Scn Table...");
		// make a hbase table called 'dbus4hbase_maxscn'
		// the max scn record is the only row' row-key
		long maxScn = EventReaderSummary.NO_EVENTS_SCN;
		try {
			Get get = new Get(Bytes.toBytes("1")); // (1,max_scn:"")
			Result rs = hTable.get(get);
			maxScn = Long.parseLong(Bytes.toString(rs.getValue(
					Bytes.toBytes("max_scn"), Bytes.toBytes(""))));
			_log.info("Got max_scn from dbus4hbase :" + maxScn);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			_log.error("I can't read max_scn from max_scn_table");
		}
		return maxScn;
	}

	@Override
	public void waitForShutdown() throws InterruptedException,
			IllegalStateException {
		// TODO Auto-generated method stub

	}

	@Override
	public void waitForShutdown(long arg0) throws InterruptedException,
			IllegalStateException {
		// TODO Auto-generated method stub

	}
}
