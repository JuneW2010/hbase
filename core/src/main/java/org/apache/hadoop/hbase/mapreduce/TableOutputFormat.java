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
package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.conf.Configuration;

/**
 * Convert Map/Reduce output and write it to an HBase table. The KEY is ignored
 * while the output value <u>must</u> be either a {@link Put} or a 
 * {@link Delete} instance. 
 * 
 * @param <KEY>  The type of the key. Ignored in this class.
 */
public class TableOutputFormat<KEY> extends OutputFormat<KEY, Writable> {

  private final Log LOG = LogFactory.getLog(TableOutputFormat.class);
  /** Job parameter that specifies the output table. */
  public static final String OUTPUT_TABLE = "hbase.mapred.outputtable";
  /** Optional job parameter to specify a peer cluster */
  public static final String QUORUM_ADDRESS = "hbase.mapred.output.quorum";
  /** Optional specification of the rs class name of the peer cluster */
  public static final String
      REGION_SERVER_CLASS = "hbase.mapred.output.rs.class";
  /** Optional specification of the rs impl name of the peer cluster */
  public static final String
      REGION_SERVER_IMPL = "hbase.mapred.output.rs.impl";

  /**
   * Writes the reducer output to an HBase table.
   * 
   * @param <KEY>  The type of the key.
   */
  protected static class TableRecordWriter<KEY> 
  extends RecordWriter<KEY, Writable> {
    
    /** The table to write to. */
    private HTable table;

    /**
     * Instantiate a TableRecordWriter with the HBase HClient for writing.
     * 
     * @param table  The table to write to.
     */
    public TableRecordWriter(HTable table) {
      this.table = table;
    }

    /**
     * Closes the writer, in this case flush table commits.
     * 
     * @param context  The context.
     * @throws IOException When closing the writer fails.
     * @see org.apache.hadoop.mapreduce.RecordWriter#close(org.apache.hadoop.mapreduce.TaskAttemptContext)
     */
    @Override
    public void close(TaskAttemptContext context) 
    throws IOException {
      table.flushCommits();
    }

    /**
     * Writes a key/value pair into the table.
     * 
     * @param key  The key.
     * @param value  The value.
     * @throws IOException When writing fails.
     * @see org.apache.hadoop.mapreduce.RecordWriter#write(java.lang.Object, java.lang.Object)
     */
    @Override
    public void write(KEY key, Writable value) 
    throws IOException {
      if (value instanceof Put) this.table.put(new Put((Put)value));
      else if (value instanceof Delete) this.table.delete(new Delete((Delete)value));
      else throw new IOException("Pass a Delete or a Put");
    }
  }
  
  /**
   * Creates a new record writer.
   * 
   * @param context  The current task context.
   * @return The newly created writer instance.
   * @throws IOException When creating the writer fails.
   * @throws InterruptedException When the jobs is cancelled.
   * @see org.apache.hadoop.mapreduce.lib.output.FileOutputFormat#getRecordWriter(org.apache.hadoop.mapreduce.TaskAttemptContext)
   */
  @Override
  public RecordWriter<KEY, Writable> getRecordWriter(
    TaskAttemptContext context) 
  throws IOException, InterruptedException {
    // expecting exactly one path
    Configuration conf = new Configuration(context.getConfiguration());
    String tableName = conf.get(OUTPUT_TABLE);
    String address = conf.get(QUORUM_ADDRESS);
    String serverClass = conf.get(REGION_SERVER_CLASS);
    String serverImpl = conf.get(REGION_SERVER_IMPL);
    HTable table = null;
    try {
      HBaseConfiguration.addHbaseResources(conf);
      if (address != null) {
        // Check is done in TMRU
        String[] parts = address.split(":");
        conf.set(HConstants.ZOOKEEPER_QUORUM, parts[0]);
        conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, parts[1]);
      }
      if (serverClass != null) {
        conf.set(HConstants.REGION_SERVER_CLASS, serverClass);
        conf.set(HConstants.REGION_SERVER_IMPL, serverImpl);
      }
      table = new HTable(conf, tableName);
    } catch(IOException e) {
      LOG.error(e);
      throw e;
    }
    table.setAutoFlush(false);
    return new TableRecordWriter<KEY>(table);
  }

  /**
   * Checks if the output target exists.
   * 
   * @param context  The current context.
   * @throws IOException When the check fails. 
   * @throws InterruptedException When the job is aborted.
   * @see org.apache.hadoop.mapreduce.OutputFormat#checkOutputSpecs(org.apache.hadoop.mapreduce.JobContext)
   */
  @Override
  public void checkOutputSpecs(JobContext context) throws IOException,
      InterruptedException {
    // TODO Check if the table exists?
    
  }

  /**
   * Returns the output committer.
   *  
   * @param context  The current context.
   * @return The committer.
   * @throws IOException When creating the committer fails.
   * @throws InterruptedException When the job is aborted.
   * @see org.apache.hadoop.mapreduce.OutputFormat#getOutputCommitter(org.apache.hadoop.mapreduce.TaskAttemptContext)
   */
  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) 
  throws IOException, InterruptedException {
    return new TableOutputCommitter();
  }
  
}
