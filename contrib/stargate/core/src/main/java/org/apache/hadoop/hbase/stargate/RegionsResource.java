/*
 * Copyright 2010 The Apache Software Foundation
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

package org.apache.hadoop.hbase.stargate;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.CacheControl;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.core.Response.ResponseBuilder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.stargate.User;
import org.apache.hadoop.hbase.stargate.model.TableInfoModel;
import org.apache.hadoop.hbase.stargate.model.TableRegionModel;

public class RegionsResource implements Constants {
  private static final Log LOG = LogFactory.getLog(RegionsResource.class);

  User user;
  String tableName;
  String actualTableName;
  CacheControl cacheControl;
  RESTServlet servlet;

  public RegionsResource(User user, String table) throws IOException {
    if (user != null) {
      this.user = user;
      this.actualTableName = 
        !user.isAdmin() ? (user.getName() + "." + table) : table;
    } else {
      this.actualTableName = table;
    }
    this.tableName = table;
    cacheControl = new CacheControl();
    cacheControl.setNoCache(true);
    cacheControl.setNoTransform(false);
    servlet = RESTServlet.getInstance();
  }

  private Map<HRegionInfo,HServerAddress> getTableRegions()
      throws IOException {
    HTablePool pool = servlet.getTablePool();
    HTableInterface table = pool.getTable(actualTableName);
    try {
      return ((HTable)table).getRegionsInfo();
    } finally {
      pool.putTable(table);
    }
  }

  @GET
  @Produces({MIMETYPE_TEXT, MIMETYPE_XML, MIMETYPE_JSON, MIMETYPE_PROTOBUF})
  public Response get(final @Context UriInfo uriInfo) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("GET " + uriInfo.getAbsolutePath());
    }
    if (!servlet.userRequestLimit(user, 1)) {
      return Response.status(509).build();
    }
    servlet.getMetrics().incrementRequests(1);
    try {
      String name = user.isAdmin() ? actualTableName : tableName;
      TableInfoModel model = new TableInfoModel(name);
      Map<HRegionInfo,HServerAddress> regions = getTableRegions();
      for (Map.Entry<HRegionInfo,HServerAddress> e: regions.entrySet()) {
        HRegionInfo hri = e.getKey();
        if (user.isAdmin()) {
          HServerAddress addr = e.getValue();
          InetSocketAddress sa = addr.getInetSocketAddress();
          model.add(
            new TableRegionModel(name, hri.getRegionId(), hri.getStartKey(),
              hri.getEndKey(),
              sa.getHostName() + ":" + Integer.valueOf(sa.getPort())));
        } else {
          model.add(
            new TableRegionModel(name, hri.getRegionId(), hri.getStartKey(),
              hri.getEndKey()));
        }
      }
      ResponseBuilder response = Response.ok(model);
      response.cacheControl(cacheControl);
      return response.build();
    } catch (TableNotFoundException e) {
      throw new WebApplicationException(Response.Status.NOT_FOUND);
    } catch (IOException e) {
      throw new WebApplicationException(e,
                  Response.Status.SERVICE_UNAVAILABLE);
    }
  }

}
