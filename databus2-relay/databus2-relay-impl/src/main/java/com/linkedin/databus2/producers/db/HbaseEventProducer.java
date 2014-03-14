/*
 * $Id: OracleEventProducer.java 272015 2011-05-21 03:03:57Z cbotev $
 */
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


import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import com.linkedin.databus.core.DbusEventBufferAppendable;
import com.linkedin.databus.core.UnsupportedKeyException;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.container.netty.ServerContainer;
import com.linkedin.databus2.core.seq.MaxSCNReaderWriter;
import com.linkedin.databus2.producers.AbstractEventProducer;
import com.linkedin.databus2.producers.EventCreationException;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;

/**
 * Wraps the OracleEventsMonitor in an event producer with a thread that can be started, paused, and
 * stopped.
 */
public class HbaseEventProducer extends AbstractEventProducer
{
  private final SourceDBEventReader _sourceDBEventReader;
  private ArrayList<ObjectName> _registeredMbeans;

  public HbaseEventProducer(
    List<HbaseTriggerMonitoredSourceInfo> sources,
    DbusEventBufferAppendable eventBuffer,
    boolean enableTracing,
    DbusEventsStatisticsCollector dbusEventsStatisticsCollector,
    MaxSCNReaderWriter maxScnReaderWriter,
    PhysicalSourceStaticConfig physicalSourceConfig,
    MBeanServer mbeanServer
    ) throws DatabusException
  {
    super(eventBuffer, maxScnReaderWriter, physicalSourceConfig, mbeanServer);
    _registeredMbeans = new ArrayList<ObjectName>(sources.size());
    _log.info("HbaseEventProducer is starting...");
    for (HbaseTriggerMonitoredSourceInfo source:sources)
    {
    	
    	try
    	{

    		Hashtable<String,String> props = new Hashtable<String,String>();
    		props.put("type", "SourceStatistics");
    		props.put("name", source.getSourceName());
    		ObjectName objectName = new ObjectName(ServerContainer.JMX_DOMAIN, props);

    		if (mbeanServer.isRegistered(objectName))
    		{
    			_log.warn("Unregistering old source statistics mbean: " + objectName);
    			mbeanServer.unregisterMBean(objectName);
    		}

    		mbeanServer.registerMBean(source.getStatisticsBean(), objectName);
    		_log.info("Registered source statistics mbean: " + objectName);
    		_registeredMbeans.add(objectName);
    	}

    	catch(Exception ex)
    	{
    		_log.error("Failed to register the source statistics mbean for source (" + source.getSourceName() + ") due to an exception.", ex);
    		throw new DatabusException("Failed to initialize event statistics mbeans.", ex);
    	}
    }
    _sourceDBEventReader = new HbaseLogEventReader(physicalSourceConfig.getName(), sources,
                                                      eventBuffer, enableTracing,
                                                      dbusEventsStatisticsCollector,
                                                      maxScnReaderWriter,
                                                      physicalSourceConfig.getSlowSourceQueryThreshold(),
                                                      physicalSourceConfig.getChunkingType(),
                                                      physicalSourceConfig.getTxnsPerChunk(),
                                                      physicalSourceConfig.getScnChunkSize(),
                                                      physicalSourceConfig.getChunkedScnThreshold(),
                                                      physicalSourceConfig.getMaxScnDelayMs());

  }

  @Override
  public List<? extends EventSourceStatisticsIface> getSources() {
	  return _sourceDBEventReader.getSources();
  }

  @SuppressWarnings("unchecked")
  public List<HbaseTriggerMonitoredSourceInfo> getMonitoredSourceInfos() {
    return (List<HbaseTriggerMonitoredSourceInfo>)_sourceDBEventReader.getSources();
  }

  @Override //read Events from all sources.
  protected ReadEventCycleSummary readEventsFromAllSources(long sinceSCN)
            throws DatabusException, EventCreationException, UnsupportedKeyException
  {
    return _sourceDBEventReader.readEventsFromAllSources(sinceSCN);
  }

  @Override
  public synchronized void shutdown()
  {
	  for (ObjectName name:_registeredMbeans)
	  {
		  try {
			_mbeanServer.unregisterMBean(name);
			_log.info("Unregistered source mbean: " + name);
		} catch (MBeanRegistrationException e) {
			_log.warn("Exception when unregistering source statistics mbean: " + name + e) ;
		} catch (InstanceNotFoundException e) {
			_log.warn("Exception when unregistering source statistics mbean: " + name + e) ;
		}
	  }
	  super.shutdown();
  }

  public SourceDBEventReader getSourceDBReader()
  {
	  return  _sourceDBEventReader;
  }
}
