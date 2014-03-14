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
import com.linkedin.databus.monitoring.mbean.EventSourceStatistics;

public class HbaseTriggerMonitoredSourceInfo implements EventSourceStatisticsIface
{
  private final short _sourceId;
  private final EventFactory _factory;
  private final String _sourceName;
  private final String _hbaseZkQuorum;
  private final String _hbaseZkPort;
  private final String _eventTable;
  private final String _eventSchema = "";
  private final EventSourceStatistics _statisticsBean;
  private final boolean _skipInfinityScn;
  public String getZkQuorum() 
  {
	  return _hbaseZkQuorum;
  }
  public String getZkPort() 
  {
	  return _hbaseZkPort;
  }
  public String getEventTable()
  {
    return _eventTable;
  }
  public short getSourceId()
  {
    return _sourceId;
  }
  public EventFactory getFactory()
  {
    return _factory;
  }
  public String getSourceName()
  {
    return _sourceName;
  }
  public String getEventSchema()
  {
    return _eventSchema;
  }
  @Override
  public EventSourceStatistics getStatisticsBean()
  {
    return _statisticsBean;
  }
  public HbaseTriggerMonitoredSourceInfo(short sourceId, String sourceName, String eventTable,
		  String hbaseZkquorum,String hbaseZkPort,EventFactory factory, EventSourceStatistics statisticsBean,
          boolean skipInfinityScn)
  {
	  	_hbaseZkPort = hbaseZkPort;
	  	_hbaseZkQuorum = hbaseZkquorum;
	  	_eventTable = eventTable;
	    _sourceId = sourceId;
	    _factory = factory;
	    _sourceName = sourceName;
	    _skipInfinityScn = skipInfinityScn;
	    if(statisticsBean == null)
	    {
	      statisticsBean = new EventSourceStatistics(sourceName);
	    }
	    _statisticsBean = statisticsBean;
  }
  @Override
  public String toString()
  {
    return _sourceName + " (id=" + _sourceId + ")";
  }
  public boolean isSkipInfinityScn()
  {
    return _skipInfinityScn;
  }
}
