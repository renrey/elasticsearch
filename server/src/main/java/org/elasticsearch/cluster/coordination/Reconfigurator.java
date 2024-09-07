/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;

import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Computes the optimal configuration of voting nodes in the cluster.
 */
public class Reconfigurator {

    private static final Logger logger = LogManager.getLogger(Reconfigurator.class);

    /**
     * The cluster usually requires a vote from at least half of the master nodes in order to commit a cluster state update, and to achieve
     * the best resilience it makes automatic adjustments to the voting configuration as master nodes join or leave the cluster. Adjustments
     * that fix or increase the size of the voting configuration are always a good idea, but the wisdom of reducing the voting configuration
     * size is less clear. For instance, automatically reducing the voting configuration down to a single node means the cluster requires
     * this node to operate, which is not resilient: if it broke we could restore every other master-eligible node in the cluster to health
     * and still the cluster would be unavailable. However not reducing the voting configuration size can also hamper resilience: in a
     * five-node cluster we could lose two nodes and by reducing the voting configuration to the remaining three nodes we could tolerate the
     * loss of a further node before failing.
     *
     * We offer two options: either we auto-shrink the voting configuration as long as it contains more than three nodes, or we don't and we
     * require the user to control the voting configuration manually using the retirement API. The former, default, option, guarantees that
     * as long as there have been at least three master-eligible nodes in the cluster and no more than one of them is currently unavailable,
     * then the cluster will still operate, which is what almost everyone wants. Manual control is for users who want different guarantees.
     */
    public static final Setting<Boolean> CLUSTER_AUTO_SHRINK_VOTING_CONFIGURATION =
        Setting.boolSetting("cluster.auto_shrink_voting_configuration", true, Property.NodeScope, Property.Dynamic);

    private volatile boolean autoShrinkVotingConfiguration;

    public Reconfigurator(Settings settings, ClusterSettings clusterSettings) {
        // 是否自动扩大投票集合
        autoShrinkVotingConfiguration = CLUSTER_AUTO_SHRINK_VOTING_CONFIGURATION.get(settings);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_AUTO_SHRINK_VOTING_CONFIGURATION, this::setAutoShrinkVotingConfiguration);
    }

    public void setAutoShrinkVotingConfiguration(boolean autoShrinkVotingConfiguration) {
        this.autoShrinkVotingConfiguration = autoShrinkVotingConfiguration;
    }

    private static int roundDownToOdd(int size) {
        return size - (size % 2 == 0 ? 1 : 0);
    }

    @Override
    public String toString() {
        return "Reconfigurator{" +
            "autoShrinkVotingConfiguration=" + autoShrinkVotingConfiguration +
            '}';
    }

    /**
     * Compute an optimal configuration for the cluster.
     *
     * @param liveNodes      The live nodes in the cluster. The optimal configuration prefers live nodes over non-live nodes as far as
     *                       possible.
     * @param retiredNodeIds Nodes that are leaving the cluster and which should not appear in the configuration if possible. Nodes that are
     *                       retired and not in the current configuration will never appear in the resulting configuration; this is useful
     *                       for shifting the vote in a 2-node cluster so one of the nodes can be restarted without harming availability.
     * @param currentMaster  The current master. Unless retired, we prefer to keep the current master in the config.
     * @param currentConfig  The current configuration. As far as possible, we prefer to keep the current config as-is.
     * @return An optimal configuration, or leave the current configuration unchanged if the optimal configuration has no live quorum.
     */
    public VotingConfiguration reconfigure(Set<DiscoveryNode> liveNodes, Set<String> retiredNodeIds, DiscoveryNode currentMaster,
                                           VotingConfiguration currentConfig) {
        assert liveNodes.contains(currentMaster) : "liveNodes = " + liveNodes + " master = " + currentMaster;
        logger.trace("{} reconfiguring {} based on liveNodes={}, retiredNodeIds={}, currentMaster={}",
            this, currentConfig, liveNodes, retiredNodeIds, currentMaster);

        // 认为是可用的master节点
        final Set<String> liveNodeIds = liveNodes.stream()
            .filter(DiscoveryNode::isMasterNode).map(DiscoveryNode::getId).collect(Collectors.toSet());
        // 目前的投票节点集合
        final Set<String> currentConfigNodeIds = currentConfig.getNodeIds();

        final Set<VotingConfigNode> orderedCandidateNodes = new TreeSet<>();
        // 目前认为可作为投票节点（对当前节点发送join的master节点），加入orderedCandidateNodes
        liveNodes.stream()
            .filter(DiscoveryNode::isMasterNode)
            .filter(n -> retiredNodeIds.contains(n.getId()) == false)// 万一被exclude了
            .forEach(n -> orderedCandidateNodes.add(new VotingConfigNode(n.getId(), true,
                n.getId().equals(currentMaster.getId()), currentConfigNodeIds.contains(n.getId()))));
        // 也保留那些没被主动无效的，master node-》但是没join到这里
        currentConfigNodeIds.stream()
            .filter(nid -> liveNodeIds.contains(nid) == false)
            .filter(nid -> retiredNodeIds.contains(nid) == false)
            .forEach(nid -> orderedCandidateNodes.add(new VotingConfigNode(nid, false, false, true)));

        /*
         * Now we work out how many nodes should be in the configuration:
         */
        // 原有本次不删除的master 节点数
        final int nonRetiredConfigSize = Math.toIntExact(orderedCandidateNodes.stream().filter(n -> n.inCurrentConfig).count());
        // 自动扩容的话（包证可用）——》就是强行指定1or3
        final int minimumConfigEnforcedSize = autoShrinkVotingConfiguration ? (nonRetiredConfigSize < 3 ? 1 : 3) : nonRetiredConfigSize;
        // join到本地的数量
        final int nonRetiredLiveNodeCount = Math.toIntExact(orderedCandidateNodes.stream().filter(n -> n.live).count());

        // 取最大-- join到本地的数量 | （默认autoShrinkVotingConfiguration） 1|3    | 关闭--原有节点数，所以偏向是新增节点才会变化
        // 默认autoShrinkVotingConfiguration：一般跟着join数量动态变化，1、3用来兜底的
        // 关闭：只使用出现过的最大数量-》无法缩容
        final int targetSize = Math.max(roundDownToOdd(nonRetiredLiveNodeCount), minimumConfigEnforcedSize);

        // 按顺序的，如果需要截取优先删掉没join的
        final VotingConfiguration newConfig = new VotingConfiguration(
            orderedCandidateNodes.stream()
                .limit(targetSize)// 动态数量
                .map(n -> n.id)
                .collect(Collectors.toSet()));

        // 验证 新集合在当前 join节点下是否可用
        // new configuration should have a quorum
        if (newConfig.hasQuorum(liveNodeIds)) {// 可用的话，targetSize=nonRetiredLiveNodeCount肯定没问题，
            return newConfig;
        } else {
            // 不可用：
            // 1. 关闭autoShrinkVotingConfiguration，当前join没达到 原有节点数
            // If there are not enough live nodes to form a quorum in the newly-proposed configuration, it's better to do nothing.
            return currentConfig;
        }
    }

    static class VotingConfigNode implements Comparable<VotingConfigNode> {
        final String id;
        final boolean live;
        final boolean currentMaster;
        final boolean inCurrentConfig;

        VotingConfigNode(String id, boolean live, boolean currentMaster, boolean inCurrentConfig) {
            this.id = id;
            this.live = live;
            this.currentMaster = currentMaster;
            this.inCurrentConfig = inCurrentConfig;
        }

        @Override
        public int compareTo(VotingConfigNode other) {
            // prefer current master
            final int currentMasterComp = Boolean.compare(other.currentMaster, currentMaster);
            if (currentMasterComp != 0) {
                return currentMasterComp;
            }
            // prefer nodes that are live
            final int liveComp = Boolean.compare(other.live, live);
            if (liveComp != 0) {
                return liveComp;
            }
            // prefer nodes that are in current config for stability
            final int inCurrentConfigComp = Boolean.compare(other.inCurrentConfig, inCurrentConfig);
            if (inCurrentConfigComp != 0) {
                return inCurrentConfigComp;
            }
            // tiebreak by node id to have stable ordering
            return id.compareTo(other.id);
        }

        @Override
        public String toString() {
            return "VotingConfigNode{" +
                "id='" + id + '\'' +
                ", live=" + live +
                ", currentMaster=" + currentMaster +
                ", inCurrentConfig=" + inCurrentConfig +
                '}';
        }
    }
}
