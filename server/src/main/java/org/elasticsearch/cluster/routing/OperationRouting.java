/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.ResponseCollectorService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.common.Booleans.parseBoolean;

public class OperationRouting {

    public static final Setting<Boolean> USE_ADAPTIVE_REPLICA_SELECTION_SETTING =
            Setting.boolSetting("cluster.routing.use_adaptive_replica_selection", true,
                    Setting.Property.Dynamic, Setting.Property.NodeScope);

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(OperationRouting.class);
    private static final String IGNORE_AWARENESS_ATTRIBUTES_PROPERTY = "es.search.ignore_awareness_attributes";
    static final String IGNORE_AWARENESS_ATTRIBUTES_DEPRECATION_MESSAGE =
        "searches will not be routed based on awareness attributes starting in version 8.0.0; " +
            "to opt into this behaviour now please set the system property [" + IGNORE_AWARENESS_ATTRIBUTES_PROPERTY + "] to [true]";

    private List<String> awarenessAttributes;
    private boolean useAdaptiveReplicaSelection;

    public OperationRouting(Settings settings, ClusterSettings clusterSettings) {
        // whether to ignore awareness attributes when routing requests
        boolean ignoreAwarenessAttr = parseBoolean(System.getProperty(IGNORE_AWARENESS_ATTRIBUTES_PROPERTY), false);
        if (ignoreAwarenessAttr == false) {
            awarenessAttributes = AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.get(settings);
            if (awarenessAttributes.isEmpty() == false) {
                deprecationLogger.deprecate(DeprecationCategory.SETTINGS, "searches_not_routed_on_awareness_attributes",
                    IGNORE_AWARENESS_ATTRIBUTES_DEPRECATION_MESSAGE);
            }
            clusterSettings.addSettingsUpdateConsumer(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING,
                this::setAwarenessAttributes);
        } else {
            awarenessAttributes = Collections.emptyList();
        }

        this.useAdaptiveReplicaSelection = USE_ADAPTIVE_REPLICA_SELECTION_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(USE_ADAPTIVE_REPLICA_SELECTION_SETTING, this::setUseAdaptiveReplicaSelection);
    }

    void setUseAdaptiveReplicaSelection(boolean useAdaptiveReplicaSelection) {
        this.useAdaptiveReplicaSelection = useAdaptiveReplicaSelection;
    }

    List<String> getAwarenessAttributes() {
        return awarenessAttributes;
    }

    private void setAwarenessAttributes(List<String> awarenessAttributes) {
        boolean ignoreAwarenessAttr = parseBoolean(System.getProperty(IGNORE_AWARENESS_ATTRIBUTES_PROPERTY), false);
        if (ignoreAwarenessAttr == false) {
            if (this.awarenessAttributes.isEmpty() && awarenessAttributes.isEmpty() == false) {
                deprecationLogger.deprecate(DeprecationCategory.SETTINGS, "searches_not_routed_on_awareness_attributes",
                    IGNORE_AWARENESS_ATTRIBUTES_DEPRECATION_MESSAGE);
            }
            this.awarenessAttributes = awarenessAttributes;
        }
    }

    public ShardIterator indexShards(ClusterState clusterState, String index, String id, @Nullable String routing) {
        return shards(clusterState, index, id, routing).shardsIt();
    }

    public ShardIterator getShards(ClusterState clusterState, String index, String id, @Nullable String routing,
                                   @Nullable String preference) {
        return preferenceActiveShardIterator(shards(clusterState, index, id, routing), clusterState.nodes().getLocalNodeId(),
            clusterState.nodes(), preference, null, null);
    }

    public ShardIterator getShards(ClusterState clusterState, String index, int shardId, @Nullable String preference) {
        final IndexShardRoutingTable indexShard = clusterState.getRoutingTable().shardRoutingTable(index, shardId);
        return preferenceActiveShardIterator(indexShard, clusterState.nodes().getLocalNodeId(), clusterState.nodes(),
            preference, null, null);
    }

    public GroupShardsIterator<ShardIterator> searchShards(ClusterState clusterState,
                                                           String[] concreteIndices,
                                                           @Nullable Map<String, Set<String>> routing,
                                                           @Nullable String preference) {
        return searchShards(clusterState, concreteIndices, routing, preference, null, null);
    }


    public GroupShardsIterator<ShardIterator> searchShards(ClusterState clusterState,
                                                           String[] concreteIndices,
                                                           @Nullable Map<String, Set<String>> routing,
                                                           @Nullable String preference,
                                                           @Nullable ResponseCollectorService collectorService,
                                                           @Nullable Map<String, Long> nodeCounts) {
        final Set<IndexShardRoutingTable> shards = computeTargetedShards(clusterState, concreteIndices, routing);
        final Set<ShardIterator> set = new HashSet<>(shards.size());
        // 遍历shard组
        for (IndexShardRoutingTable shard : shards) {
            ShardIterator iterator = preferenceActiveShardIterator(shard,
                    clusterState.nodes().getLocalNodeId(), clusterState.nodes(), preference, collectorService, nodeCounts);
            if (iterator != null) {
                set.add(iterator);
            }
        }
        return GroupShardsIterator.sortAndCreate(new ArrayList<>(set));
    }

    public static ShardIterator getShards(ClusterState clusterState, ShardId shardId) {
        final IndexShardRoutingTable shard = clusterState.routingTable().shardRoutingTable(shardId);
        return shard.activeInitializingShardsRandomIt();
    }

    private static final Map<String, Set<String>> EMPTY_ROUTING = Collections.emptyMap();

    private Set<IndexShardRoutingTable> computeTargetedShards(ClusterState clusterState, String[] concreteIndices,
                                                              @Nullable Map<String, Set<String>> routing) {
        routing = routing == null ? EMPTY_ROUTING : routing; // just use an empty map
        final Set<IndexShardRoutingTable> set = new HashSet<>();
        // we use set here and not list since we might get duplicates
        for (String index : concreteIndices) {// 遍历index
            // index路由表
            final IndexRoutingTable indexRouting = indexRoutingTable(clusterState, index);
            final IndexMetadata indexMetadata = indexMetadata(clusterState, index);
            // 如果指定了
            final Set<String> effectiveRouting = routing.get(index);
            if (effectiveRouting != null) {
                // 遍历指定的shard（通过routing）-》可知其实就是只在对应shard组上执行
                for (String r : effectiveRouting) {
                    final int routingPartitionSize = indexMetadata.getRoutingPartitionSize();
                    for (int partitionOffset = 0; partitionOffset < routingPartitionSize; partitionOffset++) {、
                        // calculateScaledShardId: 计算shardId
                        set.add(RoutingTable.shardRoutingTable(indexRouting, calculateScaledShardId(indexMetadata, r, partitionOffset)));
                    }
                }
            } else {
                // 没指定，默认使用集群中index路由表
                for (IndexShardRoutingTable indexShard : indexRouting) {
                    set.add(indexShard);
                }
            }
        }
        return set;
    }

    private ShardIterator preferenceActiveShardIterator(IndexShardRoutingTable indexShard, String localNodeId,
                                                        DiscoveryNodes nodes, @Nullable String preference,
                                                        @Nullable ResponseCollectorService collectorService,
                                                        @Nullable Map<String, Long> nodeCounts) {
        // 请求的preference参数
        if (preference == null || preference.isEmpty()) {
            return shardRoutings(indexShard, nodes, collectorService, nodeCounts);
        }
        if (preference.charAt(0) == '_') {
            Preference preferenceType = Preference.parse(preference);
            // 优先指定的shard
            if (preferenceType == Preference.SHARDS) {
                // starts with _shards, so execute on specific ones
                int index = preference.indexOf('|');

                String shards;
                if (index == -1) {// 無或者
                    shards = preference.substring(Preference.SHARDS.type().length() + 1);
                } else {
                    shards = preference.substring(Preference.SHARDS.type().length() + 1, index);
                }
                // 拆分
                String[] ids = Strings.splitStringByCommaToArray(shards);
                boolean found = false;
                for (String id : ids) {
                    // 目标id
                    if (Integer.parseInt(id) == indexShard.shardId().id()) {
                        found = true;
                        break;
                    }
                }
                if (found == false) {
                    return null;
                }
                // no more preference
                if (index == -1 || index == preference.length() - 1) {
                    // 最后id才返回？
                    return shardRoutings(indexShard, nodes, collectorService, nodeCounts);
                } else {
                    // update the preference and continue
                    preference = preference.substring(index + 1);// 后一个
                }
            }
            preferenceType = Preference.parse(preference);
            switch (preferenceType) {
                case PREFER_NODES:// 优先指定节点
                    final Set<String> nodesIds =
                            Arrays.stream(
                                    preference.substring(Preference.PREFER_NODES.type().length() + 1).split(",")
                            ).collect(Collectors.toSet());
                    return indexShard.preferNodeActiveInitializingShardsIt(nodesIds);
                case LOCAL:// 优先当前节点
                    return indexShard.preferNodeActiveInitializingShardsIt(Collections.singleton(localNodeId));
                case ONLY_LOCAL:// 只在当前node
                    return indexShard.onlyNodeActiveInitializingShardsIt(localNodeId);
                case ONLY_NODES:// 只在指定的node
                    String nodeAttributes = preference.substring(Preference.ONLY_NODES.type().length() + 1);
                    return indexShard.onlyNodeSelectorActiveInitializingShardsIt(nodeAttributes.split(","), nodes);
                default:
                    throw new IllegalArgumentException("unknown preference [" + preferenceType + "]");
            }
        }

        // 非_前缀
        // if not, then use it as the index
        int routingHash = Murmur3HashFunction.hash(preference);
        if (nodes.getMinNodeVersion().onOrAfter(Version.V_6_0_0_alpha1)) {
            // The AllocationService lists shards in a fixed order based on nodes
            // so earlier versions of this class would have a tendency to
            // select the same node across different shardIds.
            // Better overall balancing can be achieved if each shardId opts
            // for a different element in the list by also incorporating the
            // shard ID into the hash of the user-supplied preference key.
            routingHash = 31 * routingHash + indexShard.shardId.hashCode();
        }
        if (awarenessAttributes.isEmpty()) {
            return indexShard.activeInitializingShardsIt(routingHash);
        } else {
            return indexShard.preferAttributesActiveInitializingShardsIt(awarenessAttributes, nodes, routingHash);
        }
    }

    private ShardIterator shardRoutings(IndexShardRoutingTable indexShard, DiscoveryNodes nodes,
            @Nullable ResponseCollectorService collectorService, @Nullable Map<String, Long> nodeCounts) {
        if (awarenessAttributes.isEmpty()) {
            if (useAdaptiveReplicaSelection) {
                return indexShard.activeInitializingShardsRankedIt(collectorService, nodeCounts);
            } else {
                return indexShard.activeInitializingShardsRandomIt();
            }
        } else {
            return indexShard.preferAttributesActiveInitializingShardsIt(awarenessAttributes, nodes);
        }
    }

    protected IndexRoutingTable indexRoutingTable(ClusterState clusterState, String index) {
        IndexRoutingTable indexRouting = clusterState.routingTable().index(index);
        if (indexRouting == null) {
            throw new IndexNotFoundException(index);
        }
        return indexRouting;
    }

    protected IndexMetadata indexMetadata(ClusterState clusterState, String index) {
        IndexMetadata indexMetadata = clusterState.metadata().index(index);
        if (indexMetadata == null) {
            throw new IndexNotFoundException(index);
        }
        return indexMetadata;
    }

    protected IndexShardRoutingTable shards(ClusterState clusterState, String index, String id, String routing) {
        // 1。路由具体index的（逻辑primary）shard id
        int shardId = generateShardId(indexMetadata(clusterState, index), id, routing);
        // 2。拿到对应大逻辑shard 的所有具体shard实例配置
        return clusterState.getRoutingTable().shardRoutingTable(index, shardId);
    }

    public ShardId shardId(ClusterState clusterState, String index, String id, @Nullable String routing) {
        IndexMetadata indexMetadata = indexMetadata(clusterState, index);
        // 生成shard id
        /**
         * 默认未调整各种因子下：
         * shard id = routing的murmurhash % 索引分片数
         *
         * 如果都调整
         *  (routing的murmurhash + 请求id的hash % (index.routing_partition_size))%  (索引分片数+ RoutingFactor)
         */
        return new ShardId(indexMetadata.getIndex(), generateShardId(indexMetadata, id, routing));
    }

    public static int generateShardId(IndexMetadata indexMetadata, @Nullable String id, @Nullable String routing) {
        final String effectiveRouting;
        final int partitionOffset;

        // 没传入routing，就是id-》task Id
        if (routing == null) {
            assert(indexMetadata.isRoutingPartitionedIndex() == false) : "A routing value is required for gets from a partitioned index";
            effectiveRouting = id;
        } else {
            effectiveRouting = routing;
        }

        // 是否调整index.routing_partition_size为非1
        if (indexMetadata.isRoutingPartitionedIndex()) {
            // 非1
            // 1。先对请求id进行murmurhash
            // 2。用hash值对index.routing_partition_size进行取模
            partitionOffset = Math.floorMod(Murmur3HashFunction.hash(id), indexMetadata.getRoutingPartitionSize());
        } else {// 默认
            // we would have still got 0 above but this check just saves us an unnecessary hash calculation
            partitionOffset = 0;
        }

        return calculateScaledShardId(indexMetadata, effectiveRouting, partitionOffset);
    }

    private static int calculateScaledShardId(IndexMetadata indexMetadata, String effectiveRouting, int partitionOffset) {
        // 1。对routing（准确index）名进行murmurhash
        final int hash = Murmur3HashFunction.hash(effectiveRouting) + partitionOffset;

        // we don't use IMD#getNumberOfShards since the index might have been shrunk such that we need to use the size
        // of original index to hash documents
        // 2。索引的shard数(默认index.number_of_shards) / RoutingFactor（默认1）
        //3。最后路由得到的shard id= routing的hash % (索引的shard数/ RoutingFactor)
        //  -》 默认就是 routing的hash/索引shard数量
        return Math.floorMod(hash, indexMetadata.getRoutingNumShards()) / indexMetadata.getRoutingFactor();
    }

}
