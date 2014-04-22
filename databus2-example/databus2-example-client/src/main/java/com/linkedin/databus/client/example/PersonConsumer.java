package com.linkedin.databus.client.example;
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


import java.nio.ByteBuffer;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.logging.Log;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import sun.awt.image.BytePackedRaster;

import com.linkedin.databus.client.consumer.AbstractDatabusCombinedConsumer;
import com.linkedin.databus.client.pub.ConsumerCallbackResult;
import com.linkedin.databus.client.pub.DbusEventDecoder;
import com.linkedin.databus.core.DbusEvent;
import com.sun.jndi.url.ldaps.ldapsURLContextFactory;

import org.apache.hadoop.hbase.util.Bytes;



public class PersonConsumer extends AbstractDatabusCombinedConsumer
{
  public static final Logger LOG = Logger.getLogger(PersonConsumer.class.getName());

  @Override
  public ConsumerCallbackResult onDataEvent(DbusEvent event,
                                            DbusEventDecoder eventDecoder)
  {
    return processEvent(event, eventDecoder);
  }

  @Override
  public ConsumerCallbackResult onBootstrapEvent(DbusEvent event,
                                                 DbusEventDecoder eventDecoder)
  {
    return processEvent(event, eventDecoder);
  }

  private ConsumerCallbackResult processEvent(DbusEvent event,
                                              DbusEventDecoder eventDecoder)
  {
    GenericRecord decodedEvent = eventDecoder.getGenericRecord(event, null);
    try {
//      Utf8 firstName = (Utf8)decodedEvent.get("firstName");
//      Utf8 lastName = (Utf8)decodedEvent.get("lastName");
//      Long birthDate = (Long)decodedEvent.get("birthDate");
//      Utf8 deleted = (Utf8)decodedEvent.get("deleted");

//      LOG.info("firstName: " + firstName.toString() +
//               ", lastName: " + lastName.toString() +
//               ", birthDate: " + birthDate +
//               ", deleted: " + deleted.toString());
    	
    	ByteBuffer bbk = (ByteBuffer)decodedEvent.get("key");
    	ByteBuffer bbv = (ByteBuffer)decodedEvent.get("value");
    	byte[] bk = bbk.array();
    	byte[] bv = bbv.array();
    	
    	byte[] bytesbuf = new byte[2];
    	bytesbuf[0] = bk[0];
    	bytesbuf[1] = bk[1];
    	short rowkeyLenth = Bytes.toShort(bytesbuf);
    	byte[] rkBytes = new byte[rowkeyLenth];
    	for (int i = 0;i < rowkeyLenth;i ++) rkBytes[i] = bk[2 + i];
    	String roweky = Bytes.toString(rkBytes);
    	String columnFamily = "";
    	String column = "";
    	String type = "";
    	String value = Bytes.toString(bv);
    	bytesbuf[0] = 0;
    	bytesbuf[1] = bk[2 + rowkeyLenth];
    	short cfLenth = Bytes.toShort(bytesbuf);
    	byte[] cfBytes = new byte[cfLenth];
    	for (int i = 0;i < cfLenth;i ++) cfBytes[i] = bk[3 + rowkeyLenth + i];
    	columnFamily = Bytes.toString(cfBytes);
    	if (bk[bk.length - 1] == 0) type = "Minimum";
    	else if (bk[bk.length - 1] == 4) type = "Put";
    	else if (bk[bk.length - 1] == 8) type = "Delete";
    	else if (bk[bk.length - 1] == 10) type = "DeleteFamilyVersion";
    	else if (bk[bk.length - 1] == 12) type = "DeleteColumn";
    	else if (bk[bk.length - 1] == 14) type = "DeleteFamily";
    	else if (bk[bk.length - 1] == 255) type = "Maximum";
    	long ts = 0;
    	byte[] tsBytes = new byte[8];
    	for (int i = 0;i < 8;i ++) tsBytes[i] = bk[bk.length - 9 + i];
    	ts = Bytes.toLong(tsBytes);
    	int cLenth = bk.length - 9 - (3 + rowkeyLenth + cfLenth);
    	byte[] cBytes = new byte[cLenth];
    	for (int i = 0;i < cLenth;i ++) cBytes[i] = bk[3 + rowkeyLenth + cfLenth + i];
    	column = Bytes.toString(cBytes);
    	LOG.info("RowKey: " + roweky);
    	LOG.info("ColumnFamily: " + columnFamily);
    	LOG.info("Column: " + column);
    	LOG.info("Type: " + type);
    	LOG.info("TimeStamp: " + ts);
    	LOG.info("Value: " + value);
    	System.out.println("RowKey: " + roweky);
    	System.out.println("ColumnFamily: " + columnFamily);
    	System.out.println("Column: " + column);
    	System.out.println("Type: " + type);
    	System.out.println("TimeStamp: " + ts);
    	System.out.println("Value: " + value);
    } catch (Exception e) {
      LOG.error("error decoding event ", e);
      return ConsumerCallbackResult.ERROR;
    }

    return ConsumerCallbackResult.SUCCESS;
  }

}
