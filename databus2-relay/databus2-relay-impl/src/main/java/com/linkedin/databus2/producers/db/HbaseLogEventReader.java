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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;
import org.apache.hadoop.hbase.util.Bytes;

import com.linkedin.databus.core.DbusEventBufferAppendable;
import com.linkedin.databus.core.UnsupportedKeyException;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.seq.MaxSCNWriter;
import com.linkedin.databus2.producers.EventCreationException;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig.ChunkingType;


public class HbaseLogEventReader
	implements SourceDBEventReader
{
  public static final String MODULE = HbaseEventProducer.class.getName();

  private final String _name;
  private final List<HbaseTriggerMonitoredSourceInfo> _sources; //make sure all source in ONE hbase cluster...

  private final DbusEventBufferAppendable _eventBuffer;
  private final boolean _enableTracing;

  /** Logger for error and debug messages. */
  private final Logger _log;
  private final Logger _eventsLog;

  private final DbusEventsStatisticsCollector _relayInboundStatsCollector;
  private final MaxSCNWriter _maxScnWriter;
  private long _lastSeenEOP = EventReaderSummary.NO_EVENTS_SCN;
  private long _MAXSCN = 9999999999999L;

  private static final String MAX_SCN_TABLE = "dbus4hbase_scn";
  private static final String MAX_SCN_ROWKEY = "1";
  public HbaseLogEventReader(String name,
                              List<HbaseTriggerMonitoredSourceInfo> sources,
                             DbusEventBufferAppendable eventBuffer,
                             boolean enableTracing,
                             DbusEventsStatisticsCollector dbusEventsStatisticsCollector,
                             MaxSCNWriter maxScnWriter,
                             long slowQuerySourceThreshold,
                             ChunkingType chunkingType,
                             long txnsPerChunk,
                             long scnChunkSize,
                             long chunkedScnThreshold,
                             long maxScnDelayMs)
  {
	List<HbaseTriggerMonitoredSourceInfo> sourcesTemp = new ArrayList<HbaseTriggerMonitoredSourceInfo>();
	sourcesTemp.addAll(sources);
	_name = name;
    _sources = Collections.unmodifiableList(sourcesTemp);
    _eventBuffer = eventBuffer;
    _enableTracing = enableTracing;
    _relayInboundStatsCollector = dbusEventsStatisticsCollector;
    _maxScnWriter = maxScnWriter;
    _log = Logger.getLogger(getClass().getName() + "." + _name);
	  _log.info("HbaseLogEventReader is starting...");
    _eventsLog = Logger.getLogger("com.linkedin.databus2.producers.db.events." + _name);
  }

  @Override
  public ReadEventCycleSummary readEventsFromAllSources(long sinceSCN) 
	throws DatabusException, EventCreationException, UnsupportedKeyException
  {
	  boolean eventBufferNeedsRollback = true;
	  boolean debugEnabled = _log.isDebugEnabled();
	  List<EventReaderSummary> summaries = new ArrayList<EventReaderSummary>();
	  long cycleStartTS = System.currentTimeMillis();
      _eventBuffer.startEvents();
	  if (sinceSCN <= 0) {
		//  sinceSCN = getMaxSCN(); TODO i don't konw why here to get the  max scn by redow
		  sinceSCN = 0;
	      _log.debug("sinceSCN was <= 0. Overriding with the current max SCN=" + sinceSCN);
	      _eventBuffer.setStartSCN(sinceSCN);
	  }
      
	  long endOfPeriodScn = EventReaderSummary.NO_EVENTS_SCN;
	  for (HbaseTriggerMonitoredSourceInfo source:_sources) {
		  long startTS = System.currentTimeMillis();
		  EventReaderSummary summary = ReadEventCycleSummaryFromOneSources(source, sinceSCN);
		  if (summary != null && summary.getNumberOfEvents() > 0) {
			  summaries.add(summary);
			  endOfPeriodScn = Math.max(endOfPeriodScn, summary.getEndOfPeriodSCN());
			  long endTS = System.currentTimeMillis();
			  source.getStatisticsBean().addTimeOfLastDBAccess(endTS);
			  if (_eventsLog.isDebugEnabled() || (_eventsLog.isInfoEnabled() && summary.getNumberOfEvents() >0))
	          {
	            _eventsLog.info(summary.toString());
	          }
			  source.getStatisticsBean().addEventCycle(summary.getNumberOfEvents(), endTS - startTS,
                      summary.getSizeOfSerializedEvents(),
                      summary.getEndOfPeriodSCN());
		  } else {
			  source.getStatisticsBean().addEmptyEventCycle();
		  }
	  }
	  _lastSeenEOP = Math.max(_lastSeenEOP, Math.max(endOfPeriodScn, sinceSCN));
	  
	  if (endOfPeriodScn == EventReaderSummary.NO_EVENTS_SCN) {
		  if (debugEnabled)
		  {
		    _log.debug("No events processed. Read max SCN from txlog table for endOfPeriodScn. endOfPeriodScn=" + endOfPeriodScn);
		  }
		  eventBufferNeedsRollback = true;
	  } else {
		  _eventBuffer.endEvents(endOfPeriodScn, _relayInboundStatsCollector);
		  if (debugEnabled)
	        {
	          _log.debug("End of events: " + endOfPeriodScn + " windown range= "
	                       + _eventBuffer.getMinScn() + "," + _eventBuffer.lastWrittenScn());
	        }
	        //no need to roll back
	        eventBufferNeedsRollback = false;
	  }
	  
	  
	  if (endOfPeriodScn != EventReaderSummary.NO_EVENTS_SCN)
	  {
		  if (null != _maxScnWriter && (endOfPeriodScn != sinceSCN))
		  {
			  _maxScnWriter.saveMaxScn(endOfPeriodScn);
		  }
		  for(HbaseTriggerMonitoredSourceInfo source : _sources) {
	          //update maxDBScn here
	          source.getStatisticsBean().addMaxDBScn(endOfPeriodScn);
	          source.getStatisticsBean().addTimeOfLastDBAccess(System.currentTimeMillis());
	       }
	  }
      long cycleEndTS = System.currentTimeMillis();
	  ReadEventCycleSummary summary = new ReadEventCycleSummary(_name, summaries,
              Math.max(endOfPeriodScn, sinceSCN),
              (cycleEndTS - cycleStartTS));
	  if (eventBufferNeedsRollback) 
	  {
		  if (_log.isDebugEnabled())
	      {
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
  
  private EventReaderSummary ReadEventCycleSummaryFromOneSources(HbaseTriggerMonitoredSourceInfo source,long sinceSCN)
  {
	  _log.info("Get Event for hbase table:" + source.getSourceName() + ",SinceSCN:" + sinceSCN);
	  int numRowsFetched = 0;
	  long startTS = System.currentTimeMillis();
	  long totalEventSerializeTime = 0;
	  long totalEventSize = 0;
	  long tsWindowStart = Long.MAX_VALUE;long tsWindowEnd = Long.MIN_VALUE;
	  
	  String hbaseZkQuorum = source.getZkQuorum();
	  String hbaseZkPort = source.getZkPort();
	  String hbaseTable = source.getEventTable();
	  ArrayList<String> hbaseTableCFList = source.getColumnFamily();
	  
	  org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
	  conf.set("hbase.zookeeper.quorum", hbaseZkQuorum);
	  conf.set("hbase.zookeeper.property.clientPort", hbaseZkPort);
	  HTable htable = null;
	  Scan scan = null;
	  ResultScanner scanner = null;
	  
	  try {
		  
		htable = new HTable(conf, hbaseTable + "_scn");
		scan = new Scan();
		if (hbaseTableCFList.isEmpty()) {
			scan.addFamily(Bytes.toBytes("index"));
		} else {
			for (String sz : hbaseTableCFList) {
				scan.addColumn(Bytes.toBytes("index"),Bytes.toBytes(sz + "_R"));
				scan.addColumn(Bytes.toBytes("index"),Bytes.toBytes(sz + "_C"));
			}
		} 
		scan.setStartRow(Bytes.toBytes(sinceSCN + 1)); //TODO add one to get the kv a little bigger
		scan.setStopRow(Bytes.toBytes(_MAXSCN));
		scanner = htable.getScanner(scan);

		List<Get> getList = new ArrayList<Get>();
		for (Result rs:scanner) {
			long timeStamp = Bytes.toLong(rs.getRow());
			tsWindowEnd = Math.max(timeStamp, tsWindowEnd);
			tsWindowStart = Math.min(timeStamp, tsWindowStart);
			NavigableMap<byte[], byte[]> rsMap = rs.getNoVersionMap().get(Bytes.toBytes("index"));
			byte[] rowKey = null;
			byte[] columnFamily = null;
			byte[] column = null;
			for (byte[] bt : rsMap.keySet()) {
				byte[] value = rsMap.get(bt);
				columnFamily = new byte[bt.length - 2];
				for (int i = 0;i < bt.length - 2;i ++) columnFamily[i] = bt[i];
				if (bt[bt.length - 1] == 'R') {
					rowKey = value;
				} else {
					column = value;
				}
			}
			Get get = new Get(rowKey);
			get.addColumn(columnFamily, column);
			getList.add(get);
		}
		htable.close();
		
		htable = new HTable(conf, hbaseTable);
		Result[] rs = htable.get(getList);
		
		long endTS = System.currentTimeMillis();
		long queryExecTime = endTS - startTS;
		long endOfPeriodSCN = EventReaderSummary.NO_EVENTS_SCN;
		
		for (Result r : rs) {
			if (r.isEmpty() == true) continue; //in case some row has been deleted by user...
			KeyValue kv = r.raw()[0];//make sure the r has only one key value.
			long scn = kv.getTimestamp();
			long timestamp = scn;
			long tsStart = System.currentTimeMillis();
			
			long eventSize = source.getFactory().createAndAppendEvent(scn, 
					timestamp, kv, _eventBuffer, _enableTracing, _relayInboundStatsCollector);
			
			totalEventSerializeTime += System.currentTimeMillis() - tsStart;
			totalEventSize += eventSize;
			endOfPeriodSCN = Math.max(endOfPeriodSCN, scn);
			
			numRowsFetched ++;
		}
		
		endTS = System.currentTimeMillis();
		
		EventReaderSummary summary = new EventReaderSummary(source.getSourceId(), source.getSourceName(),endOfPeriodSCN, numRowsFetched,
                totalEventSize, (endTS - startTS),totalEventSerializeTime,tsWindowStart,tsWindowEnd,queryExecTime);
		
		return summary;
		
	} catch (IOException e) {
		_log.error(e.getStackTrace());
		return null;
	} catch (EventCreationException e) {
		// TODO Auto-generated catch block
		_log.error(e.getStackTrace());
		return null;
	} catch (UnsupportedKeyException e) {
		// TODO Auto-generated catch block
		_log.error(e.getStackTrace());
		return null;
	} finally {
		if (htable != null) {
			try {
				htable.close();
			} catch (IOException e) {
				_log.error(e.getStackTrace());
			}
		}
		if (scanner != null) {
			scanner.close();
		}
	}
  }
  
  /**
   * @param db
   * @return the max scn
   * @throws SQLException
   */
  private long getMaxSCN() //return the max_scn of all hbase table
  {
	//make a hbase table called 'dbus4hbase_maxscn'
	//the max scn record is the only row' row-key
	//the max scn for whole hbase is 1->max_scn:""
	//the max scn for hbase table 'table' is 1->table:"table" 
	//the rowkey is always 1
	 _log.info("Getting the max of all hbase table...");
    long maxScn = EventReaderSummary.NO_EVENTS_SCN;
    String hbaseZkQuorum = _sources.get(0).getZkQuorum();
	String hbaseZkPort = _sources.get(0).getZkPort();
	org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
	conf.set("hbase.zookeeper.quorum", hbaseZkQuorum);
	conf.set("hbase.zookeeper.property.clientPort", hbaseZkPort);
	HTable htable = null;
	try {
		htable = new HTable(conf,MAX_SCN_TABLE);
		Get get = new Get(Bytes.toBytes(MAX_SCN_ROWKEY));
		Result rs = htable.get(get);
		maxScn = Bytes.toLong(rs.getValue(Bytes.toBytes("max_scn"), Bytes.toBytes("")));
		_log.info("Got max_scn for hbase table: " + maxScn);
	} catch (IOException e) {
		// TODO Auto-generated catch block
		_log.error("I can't read max_scn from max_scn_table");
	} finally {
		if (htable != null) {
			try {
				htable.close();
			} catch (IOException e) {
				_log.error(e.getStackTrace());
			}
		}
	}
    return maxScn;
  }
}
