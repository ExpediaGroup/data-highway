/**
 * Copyright (C) 2016-2019 Expedia Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hotels.road.hive.metastore;

import java.lang.reflect.Proxy;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;

public class MetaStoreClientFactory {
  public IMetaStoreClient newInstance(HiveConf conf) throws MetaStoreException {
    try {
      return synchronizedClient(retryingClient(conf));
    } catch (MetaException e) {
      throw new MetaStoreException(e);
    }
  }

  private IMetaStoreClient retryingClient(HiveConf conf) throws MetaException {
    return RetryingMetaStoreClient.getProxy(conf, tbl -> null, HiveMetaStoreClient.class.getName());
  }

  private IMetaStoreClient synchronizedClient(IMetaStoreClient delegate) {
    return (IMetaStoreClient) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] { IMetaStoreClient.class }, (p, m, a) -> {
          synchronized (delegate) {
            return m.invoke(delegate, a);
          }
        });
  }
}
