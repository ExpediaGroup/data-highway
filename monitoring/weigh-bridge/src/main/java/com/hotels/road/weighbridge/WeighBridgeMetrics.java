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
package com.hotels.road.weighbridge;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.MultiGauge.Row;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;

import com.hotels.road.weighbridge.model.Broker;
import com.hotels.road.weighbridge.model.LogDir;
import com.hotels.road.weighbridge.model.PartitionReplica;
import com.hotels.road.weighbridge.model.Topic;

@Component
public class WeighBridgeMetrics {
  private final MultiGauge diskFree;
  private final MultiGauge diskTotal;
  private final MultiGauge diskUsed;
  private final MultiGauge sizeOnDisk;
  private final MultiGauge logSize;
  private final MultiGauge beginningOffset;
  private final MultiGauge endOffset;
  private final MultiGauge recordCount;

  public WeighBridgeMetrics(MeterRegistry registry) {
    diskFree = MultiGauge.builder("weighbridge_disk_free").register(registry);
    diskTotal = MultiGauge.builder("weighbridge_disk_total").register(registry);
    diskUsed = MultiGauge.builder("weighbridge_disk_used").register(registry);
    sizeOnDisk = MultiGauge.builder("weighbridge_size_on_disk").register(registry);
    logSize = MultiGauge.builder("weighbridge_log_size").register(registry);
    beginningOffset = MultiGauge.builder("weighbridge_beginning_offset").register(registry);
    endOffset = MultiGauge.builder("weighbridge_end_offset").register(registry);
    recordCount = MultiGauge.builder("weighbridge_record_count").register(registry);
  }

  @SuppressWarnings("rawtypes")
  public void update(Broker broker) {
    List<Row> diskFreeRows = new ArrayList<>();
    List<Row> diskTotalRows = new ArrayList<>();
    List<Row> diskUsedRows = new ArrayList<>();
    List<Row> sizeOnDiskRows = new ArrayList<>();
    List<Row> logSizeRows = new ArrayList<>();
    List<Row> beginningOffsetRows = new ArrayList<>();
    List<Row> endOffsetRows = new ArrayList<>();
    List<Row> recordCountRows = new ArrayList<>();

    Tag brokerTag = Tag.of("broker", String.valueOf(broker.getId()));
    Tag rackTag = Tag.of("rack", broker.getRack());

    for (LogDir logDir : broker.getLogDirs()) {
      Tag logDirTag = Tag.of("logdir", logDir.getPath());

      Tags logDirTags = Tags.of(brokerTag, rackTag, logDirTag);
      diskFreeRows.add(Row.of(logDirTags, logDir.getDiskFree()));
      diskTotalRows.add(Row.of(logDirTags, logDir.getDiskTotal()));
      diskUsedRows.add(Row.of(logDirTags, logDir.getDiskTotal() - logDir.getDiskFree()));

      for (Topic topic : logDir.getTopics()) {
        Tag topicTag = Tag.of("topic", topic.getName());

        for (PartitionReplica replica : topic.getPartitionReplicas()) {
          Tag partitionTag = Tag.of("partition", String.valueOf(replica.getPartition()));
          Tag leaderTag = Tag.of("leader", String.valueOf(replica.isLeader()));
          Tag inSyncTag = Tag.of("inSync", String.valueOf(replica.isInSync()));

          Tags replicaTags = Tags.of(brokerTag, rackTag, logDirTag, topicTag, partitionTag, leaderTag, inSyncTag);
          sizeOnDiskRows.add(Row.of(replicaTags, replica.getSizeOnDisk()));
          logSizeRows.add(Row.of(replicaTags, replica.getLogSize()));
          beginningOffsetRows.add(Row.of(replicaTags, replica.getBeginningOffset()));
          endOffsetRows.add(Row.of(replicaTags, replica.getEndOffset()));
          recordCountRows.add(Row.of(replicaTags, replica.getRecordCount()));
        }
      }
    }

    diskFree.register(diskFreeRows, true);
    diskTotal.register(diskTotalRows, true);
    diskUsed.register(diskUsedRows, true);
    sizeOnDisk.register(sizeOnDiskRows, true);
    logSize.register(logSizeRows, true);
    beginningOffset.register(beginningOffsetRows, true);
    endOffset.register(endOffsetRows, true);
    recordCount.register(recordCountRows, true);
  }
}
