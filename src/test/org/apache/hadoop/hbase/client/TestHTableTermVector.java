/**
 * Copyright 2009 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.client;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.regionserver.lucene.HBaseneUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.util.OpenBitSet;

public class TestHTableTermVector extends HBaseClusterTestCase implements HConstants {
  private static final HColumnDescriptor column =
    new HColumnDescriptor(CATALOG_FAMILY);

  
  private static final byte [] tableAname = Bytes.toBytes("tableA");
  
  private static final byte [] row = Bytes.toBytes("row");
 
  private static final byte[] FAMILY_TERMVECTOR = Bytes.toBytes("fm.termVector");


  public void testAddTermVector() { 
    HTable table = null;
    try {
      HColumnDescriptor column2 =
        new HColumnDescriptor(FAMILY_TERMVECTOR);
      HBaseAdmin admin = new HBaseAdmin(conf);
      HTableDescriptor testTableADesc =
        new HTableDescriptor(tableAname);
      testTableADesc.addFamily(column);
      testTableADesc.addFamily(column2);
      admin.createTable(testTableADesc);
      
      table = new HTable(conf, tableAname);
      System.out.println("Adding a Document to a table");

      long start = System.nanoTime();
      for (long i = 0; i < 1000000; ++i) {
        table.addDocToTerm(row, i);
      }

      table.flushCommitTermDocs();
      System.out.println("Bulk Added in " + (double)(System.nanoTime() - start)/ (double) 1000000 + "  ms ");
      
      start = System.nanoTime();    
      Get get = null;
      Result result = null;
      
      get = new Get(row);
      get.addFamily(FAMILY_TERMVECTOR);
      System.out.println("Getting row");
      start = System.nanoTime();
      result = table.get(get);
      long stop = System.nanoTime();
      System.out.println("timer " +(stop-start));
      System.out.println("result " +result);

      byte[] value = result.getValue(FAMILY_TERMVECTOR, HBaseneUtil.createTermVectorQualifier(0));
      OpenBitSet bitset = HBaseneUtil.toOpenBitSet(value);
      Assert.assertTrue( bitset.get(1) );
      Assert.assertTrue( bitset.get(20) );
      Assert.assertTrue( bitset.get(100) );
      
      //Assert.assertFalse( bitset.get(2) );
      //Assert.assertFalse( bitset.get(11) );
      //Assert.assertFalse( bitset.get(101) );
    } catch (IOException e) {
      e.printStackTrace();
      fail("Should not have any exception " +
        e.getClass());
    }   
    }

  
  public void testLuceneSize() {
    System.out.println("number of 64 bit words "
        + OpenBitSet.bits2words(45 * 1000000));
  }

}
