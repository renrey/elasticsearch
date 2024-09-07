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
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationState.VoteCollection;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.monitor.NodeHealthService;
import org.elasticsearch.monitor.StatusInfo;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongConsumer;
import java.util.stream.StreamSupport;

import static org.elasticsearch.monitor.StatusInfo.Status.UNHEALTHY;

import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;

public class PreVoteCollector {

    private static final Logger logger = LogManager.getLogger(PreVoteCollector.class);

    public static final String REQUEST_PRE_VOTE_ACTION_NAME = "internal:cluster/request_pre_vote";

    private final TransportService transportService;
    private final Runnable startElection;
    private final LongConsumer updateMaxTermSeen;
    private final ElectionStrategy electionStrategy;
    private NodeHealthService nodeHealthService;

    // Tuple for simple atomic updates. null until the first call to `update()`.
    private volatile Tuple<DiscoveryNode, PreVoteResponse> state; // DiscoveryNode component is null if there is currently no known leader.

    PreVoteCollector(final TransportService transportService, final Runnable startElection, final LongConsumer updateMaxTermSeen,
                     final ElectionStrategy electionStrategy, NodeHealthService nodeHealthService) {
        this.transportService = transportService;
        this.startElection = startElection;
        this.updateMaxTermSeen = updateMaxTermSeen;
        this.electionStrategy = electionStrategy;
        this.nodeHealthService = nodeHealthService;

        // 注册request_pre_vote
        transportService.registerRequestHandler(REQUEST_PRE_VOTE_ACTION_NAME, Names.GENERIC, false, false,
            PreVoteRequest::new,
            (request, channel, task) -> channel.sendResponse(handlePreVoteRequest(request)));
    }

    /**
     * Start a new pre-voting round.
     *
     * @param clusterState   the last-accepted cluster state
     * @param broadcastNodes the nodes from whom to request pre-votes
     * @return the pre-voting round, which can be closed to end the round early.
     */
    public Releasable start(final ClusterState clusterState, final Iterable<DiscoveryNode> broadcastNodes) {
        PreVotingRound preVotingRound = new PreVotingRound(clusterState, state.v2().getCurrentTerm());
        // 开始
        preVotingRound.start(broadcastNodes);
        return preVotingRound;
    }

    // only for testing
    PreVoteResponse getPreVoteResponse() {
        return state.v2();
    }

    // only for testing
    @Nullable
    DiscoveryNode getLeader() {
        return state.v1();
    }

    public void update(final PreVoteResponse preVoteResponse, @Nullable final DiscoveryNode leader) {
        logger.trace("updating with preVoteResponse={}, leader={}", preVoteResponse, leader);
        state = new Tuple<>(leader, preVoteResponse);
    }

    private PreVoteResponse handlePreVoteRequest(final PreVoteRequest request) {
        // 更新见过最大term -》不是本地选的term
        updateMaxTermSeen.accept(request.getCurrentTerm());

        Tuple<DiscoveryNode, PreVoteResponse> state = this.state;
        assert state != null : "received pre-vote request before fully initialised";

        final DiscoveryNode leader = state.v1();// leader、follower 当前的leader
        final PreVoteResponse response = state.v2();

        final StatusInfo statusInfo = nodeHealthService.getHealth();
        if (statusInfo.getStatus() == UNHEALTHY) {
            String message = "rejecting " + request + " on unhealthy node: [" + statusInfo.getInfo() + "]";
            logger.debug(message);
            throw new NodeHealthCheckFailureException(message);
        }

        // 当前节点是Candidate，寻找集群，返回 自己的term
        if (leader == null) {
            return response;
        }

        if (leader.equals(request.getSourceNode())) {
            // This is a _rare_ case where our leader has detected a failure and stepped down, but we are still a follower. It's possible
            // that the leader lost its quorum, but while we're still a follower we will not offer joins to any other node so there is no
            // major drawback in offering a join to our old leader. The advantage of this is that it makes it slightly more likely that the
            // leader won't change, and also that its re-election will happen more quickly than if it had to wait for a quorum of followers
            // to also detect its failure.
            return response;
        }

        throw new CoordinationStateRejectedException("rejecting " + request + " as there is already a leader");
    }

    @Override
    public String toString() {
        return "PreVoteCollector{" +
            "state=" + state +
            '}';
    }

    private class PreVotingRound implements Releasable {
        private final Map<DiscoveryNode, PreVoteResponse> preVotesReceived = newConcurrentMap();
        private final AtomicBoolean electionStarted = new AtomicBoolean();
        private final PreVoteRequest preVoteRequest;
        private final ClusterState clusterState;
        private final AtomicBoolean isClosed = new AtomicBoolean();

        PreVotingRound(final ClusterState clusterState, final long currentTerm) {
            this.clusterState = clusterState;
            preVoteRequest = new PreVoteRequest(transportService.getLocalNode(), currentTerm);
        }

        void start(final Iterable<DiscoveryNode> broadcastNodes) {
            assert StreamSupport.stream(broadcastNodes.spliterator(), false).noneMatch(Coordinator::isZen1Node) : broadcastNodes;
            logger.debug("{} requesting pre-votes from {}", this, broadcastNodes);
            // 每个master都发送 request_pre_vote请求
            broadcastNodes.forEach(n -> transportService.sendRequest(n, REQUEST_PRE_VOTE_ACTION_NAME, preVoteRequest,
                new TransportResponseHandler<PreVoteResponse>() {
                    @Override
                    public PreVoteResponse read(StreamInput in) throws IOException {
                        return new PreVoteResponse(in);
                    }

                    @Override
                    public void handleResponse(PreVoteResponse response) {
                        // 处理响应
                        handlePreVoteResponse(response, n);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        logger.debug(new ParameterizedMessage("{} failed", this), exp);
                    }

                    @Override
                    public String executor() {
                        return Names.GENERIC;
                    }

                    @Override
                    public String toString() {
                        return "TransportResponseHandler{" + PreVoteCollector.this + ", node=" + n + '}';
                    }
                }));
        }

        private void handlePreVoteResponse(final PreVoteResponse response, final DiscoveryNode sender) {
            if (isClosed.get()) {
                logger.debug("{} is closed, ignoring {} from {}", this, response, sender);
                return;
            }

            /**
             * @see Coordinator#updateMaxTermSeen(long)
             */
            updateMaxTermSeen.accept(response.getCurrentTerm());// 主要更新本地收到的最大term

            //  对方的版本 比本地新 ，直接跳过 -》选择自己的必须比自己老的？
            if (response.getLastAcceptedTerm() > clusterState.term()
                || (response.getLastAcceptedTerm() == clusterState.term()
                && response.getLastAcceptedVersion() > clusterState.getVersionOrMetadataVersion())) {
                logger.debug("{} ignoring {} from {} as it is fresher", this, response, sender);
                return;
            }
            // 到这里，代表对方的term、元数据版本最多跟本地的相同
            // 保存对象
            preVotesReceived.put(sender, response);

            // create a fake VoteCollection based on the pre-votes and check if there is an election quorum
            final VoteCollection voteCollection = new VoteCollection();
            final DiscoveryNode localNode = clusterState.nodes().getLocalNode();
            final PreVoteResponse localPreVoteResponse = getPreVoteResponse();

            // 基于这些不比本地新的 创建假的 选票集合、
            // 选择自己的条件
            preVotesReceived.forEach((node, preVoteResponse) -> voteCollection.addJoinVote(
                new Join(node, localNode, preVoteResponse.getCurrentTerm(),
                preVoteResponse.getLastAcceptedTerm(), preVoteResponse.getLastAcceptedVersion())));

            // 当前对选自己的选票数量 超过目标一半 -> 选举有意义
            if (electionStrategy.isElectionQuorum(clusterState.nodes().getLocalNode(), localPreVoteResponse.getCurrentTerm(),
                localPreVoteResponse.getLastAcceptedTerm(), localPreVoteResponse.getLastAcceptedVersion(),
                clusterState.getLastCommittedConfiguration(), clusterState.getLastAcceptedConfiguration(), voteCollection) == false) {
                logger.debug("{} added {} from {}, no quorum yet", this, response, sender);
                return;
            }

            if (electionStarted.compareAndSet(false, true) == false) {
                logger.debug("{} added {} from {} but election has already started", this, response, sender);
                return;
            }

            // 选自己的超过一半？
            // 开始选举？
            logger.debug("{} added {} from {}, starting election", this, response, sender);
            /**
             * @see Coordinator#startElection()
             */
            startElection.run();
        }

        @Override
        public String toString() {
            return "PreVotingRound{" +
                "preVotesReceived=" + preVotesReceived +
                ", electionStarted=" + electionStarted +
                ", preVoteRequest=" + preVoteRequest +
                ", isClosed=" + isClosed +
                '}';
        }

        @Override
        public void close() {
            final boolean isNotAlreadyClosed = isClosed.compareAndSet(false, true);
            assert isNotAlreadyClosed;
        }
    }
}
