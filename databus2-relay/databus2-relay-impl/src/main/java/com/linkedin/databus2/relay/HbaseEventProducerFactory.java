/*
 * $Id: RelayFactory.java 272015 2011-05-21 03:03:57Z cbotev $
 */
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


import java.lang.management.ManagementFactory;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.management.MBeanServer;
import javax.sql.DataSource;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.DbusEventBufferAppendable;
import com.linkedin.databus.core.UnsupportedKeyException;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus.monitoring.mbean.EventSourceStatistics;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.seq.MaxSCNReaderWriter;
import com.linkedin.databus2.producers.ConstantPartitionFunction;
import com.linkedin.databus2.producers.EventCreationException;
import com.linkedin.databus2.producers.EventProducer;
import com.linkedin.databus2.producers.PartitionFunction;
import com.linkedin.databus2.producers.db.EventFactory;
import com.linkedin.databus2.producers.db.HbaseEventFactory;
import com.linkedin.databus2.producers.db.HbaseEventProducer;
import com.linkedin.databus2.producers.db.HbaseTriggerMonitoredSourceInfo;
import com.linkedin.databus2.producers.db.OracleTriggerMonitoredSourceInfo;
import com.linkedin.databus2.producers.db.OracleAvroGenericEventFactory;
import com.linkedin.databus2.producers.db.OracleEventProducer;
import com.linkedin.databus2.relay.config.LogicalSourceStaticConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.schemas.NoSuchSchemaException;
import com.linkedin.databus2.schemas.SchemaRegistryService;

/**
 * @author Jemiah Westerman<jwesterman@linkedin.com>
 * @version $Revision: 272015 $
 */
public class HbaseEventProducerFactory
{
  private final Logger _log = Logger.getLogger(getClass());

  public EventProducer buildEventProducer(PhysicalSourceStaticConfig physicalSourceConfig,
                                 SchemaRegistryService schemaRegistryService,
                                 DbusEventBufferAppendable dbusEventBuffer,
                                 MBeanServer mbeanServer,
                                 DbusEventsStatisticsCollector dbusEventsStatisticsCollector,
                                 MaxSCNReaderWriter _maxScnReaderWriter
                                 )
  throws DatabusException, EventCreationException, UnsupportedKeyException, SQLException, InvalidConfigException
  {

	  _log.info("HbaseEventProducerFactory is starting...");
    // Parse each one of the logical sources
    List<HbaseTriggerMonitoredSourceInfo> sources = new ArrayList<HbaseTriggerMonitoredSourceInfo>();
    for(LogicalSourceStaticConfig sourceConfig : physicalSourceConfig.getSources())
    {
    	HbaseTriggerMonitoredSourceInfo source = buildHbaseMonitoredSourceInfo(sourceConfig, physicalSourceConfig, schemaRegistryService);
      sources.add(source);
    }

    // Create the event producer
    EventProducer eventProducer = new HbaseEventProducer(sources,
                                                          dbusEventBuffer,
                                                          true,
                                                          dbusEventsStatisticsCollector,
                                                          _maxScnReaderWriter,
                                                          physicalSourceConfig,
                                                          ManagementFactory.getPlatformMBeanServer());

    _log.info("Created HbaseEventProducer for config:  " + physicalSourceConfig);
    return eventProducer;
  }

  protected HbaseEventFactory createEventFactory(
		                                                      LogicalSourceStaticConfig sourceConfig, PhysicalSourceStaticConfig pConfig,
          													  String eventSchema, PartitionFunction partitionFunction)
      throws EventCreationException, UnsupportedKeyException
  {
	  return new HbaseEventFactory(sourceConfig.getId(), (short)pConfig.getId(),
              eventSchema, partitionFunction,pConfig.getReplBitSetter());
  }

  public HbaseTriggerMonitoredSourceInfo buildHbaseMonitoredSourceInfo( //build Class HbaseMonitoredSourceInfo for each table...
      LogicalSourceStaticConfig sourceConfig, PhysicalSourceStaticConfig pConfig, SchemaRegistryService schemaRegistryService)
      throws DatabusException, EventCreationException, UnsupportedKeyException,
             InvalidConfigException
  {
    String schema = null;
	try {
		schema = schemaRegistryService.fetchLatestSchemaBySourceName(sourceConfig.getName());
	} catch (NoSuchSchemaException e) {
	      throw new InvalidConfigException("Unable to load the schema for source (" + sourceConfig.getName() + ").");
	}

    if(schema == null)
    {
      throw new InvalidConfigException("Unable to load the schema for source (" + sourceConfig.getName() + ").");
    }

    _log.info("Loading schema for source id " + sourceConfig.getId() + ": " + schema);

    //   -->hbase:172.20.1.1:9000/table/cf1,cf2,cf3,cf4
    
    String uri = sourceConfig.getUri();
    _log.info("Got Hbase Uri:" + uri);
    uri = uri.substring(6,uri.length());
    String eventTable = null;
    String hbaseZkquorum = null;
    String hbaseZkPort = null;
    ArrayList<String> columnFamily = new ArrayList<String>();
    int idxColon = uri.indexOf(':');
    int idxSlashFirst = uri.indexOf('/');
    int idxSlashLast = uri.lastIndexOf('/');
    
    eventTable = uri.substring(idxSlashFirst + 1,idxSlashLast);
    String colunmnFamilyList = uri.substring(idxSlashLast + 1);
    String[] colunmnFamilyArray = colunmnFamilyList.split(",");
    for (String sz : colunmnFamilyArray) {
    	columnFamily.add(sz);
    }
    hbaseZkquorum = uri.substring(0, idxColon);
    hbaseZkPort = uri.substring(idxColon + 1,idxSlashFirst);
    

    PartitionFunction partitionFunction = buildPartitionFunction(sourceConfig);
    EventFactory factory = createEventFactory( sourceConfig, pConfig,
                                              schema, partitionFunction);

    EventSourceStatistics statisticsBean = new EventSourceStatistics(sourceConfig.getName());

    
    //short sourceId, String sourceName, String eventTable,
	//String hbaseZkquorum,String hbaseZkPort,EventFactory factory, EventSourceStatistics statisticsBean,
    //boolean skipInfinityScn)

    HbaseTriggerMonitoredSourceInfo sourceInfo = new HbaseTriggerMonitoredSourceInfo(sourceConfig.getId(),
                                                             sourceConfig.getName(),eventTable,
                                                             hbaseZkquorum, hbaseZkPort ,columnFamily,factory,
                                                             statisticsBean,
                                                             sourceConfig.isSkipInfinityScn());
    return sourceInfo;
  }

  public PartitionFunction buildPartitionFunction(LogicalSourceStaticConfig sourceConfig)
  throws InvalidConfigException
  {
    String partitionFunction = sourceConfig.getPartitionFunction();
    if(partitionFunction.startsWith("constant:"))
    {
      try
      {
        String numberPart = partitionFunction.substring("constant:".length()).trim();
        short constantPartitionNumber = Short.valueOf(numberPart);
        return new ConstantPartitionFunction(constantPartitionNumber);
      }
      catch(Exception ex)
      {
        // Could be a NumberFormatException, IndexOutOfBoundsException or other exception when trying to parse the partition number.
        throw new InvalidConfigException("Invalid partition configuration (" + partitionFunction + "). " +
        		"Could not parse the constant partition number.");
      }
    }
    else
    {
      throw new InvalidConfigException("Invalid partition configuration (" + partitionFunction + ").");
    }
  }


}
