/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.node;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.bulk.TransportSingleItemBulkWriteAction;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.replication.ReplicatedWriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskListener;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterService;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Client that executes actions on the local node.
 *  在当前节点（本地）执行action的逻辑
 *  实际的作用是把action转成TransportAction, 交给集群内部执行！！！
 *  继承AbstractClient的作用：响应的回调都是提交线程池执行，就是action执行完成后，后置的响应回调处理都是异步的
 */
public class NodeClient extends AbstractClient {

    /**
     * 注册的方法
     * @see org.elasticsearch.action.ActionModule#setupActions(java.util.List)
     */
    private Map<ActionType, TransportAction> actions;
    /**
     * The id of the local {@link DiscoveryNode}. Useful for generating task ids from tasks returned by
     * {@link #executeLocally(ActionType, ActionRequest, TaskListener)}.
     */
    private Supplier<String> localNodeId;
    private RemoteClusterService remoteClusterService;
    private NamedWriteableRegistry namedWriteableRegistry;

    public NodeClient(Settings settings, ThreadPool threadPool) {
        super(settings, threadPool);
    }

    public void initialize(Map<ActionType, TransportAction> actions, Supplier<String> localNodeId,
                           RemoteClusterService remoteClusterService, NamedWriteableRegistry namedWriteableRegistry) {
        this.actions = actions;
        this.localNodeId = localNodeId;
        this.remoteClusterService = remoteClusterService;
        this.namedWriteableRegistry = namedWriteableRegistry;
    }

    @Override
    public void close() {
        // nothing really to do
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse>
    void doExecute(ActionType<Response> action, Request request, ActionListener<Response> listener) {
        // Discard the task because the Client interface doesn't use it.
        try {
            executeLocally(action, request, listener);
        } catch (TaskCancelledException | IllegalArgumentException | IllegalStateException e) {
            // #executeLocally returns the task and throws TaskCancelledException if it fails to register the task because the parent
            // task has been cancelled, IllegalStateException if the client was not in a state to execute the request because it was not
            // yet properly initialized or IllegalArgumentException if header validation fails we forward them to listener since this API
            // does not concern itself with the specifics of the task handling
            listener.onFailure(e);
        }
    }

    /**
     * Execute an {@link ActionType} locally, returning that {@link Task} used to track it, and linking an {@link ActionListener}.
     * Prefer this method if you don't need access to the task when listening for the response. This is the method used to implement
     * the {@link Client} interface.
     * 这个会返回Task对象，用于追踪执行情况
     * @throws TaskCancelledException if the request's parent task has been cancelled already
     */
    public <    Request extends ActionRequest,
                Response extends ActionResponse
            > Task executeLocally(ActionType<Response> action, Request request, ActionListener<Response> listener) {
        // 获取当前action(ActionType)的绑定通信操作(TransportAction),并传入request执行发送请求
         // 根据ActionType找到对应的transportAction，然后execute执行
        /**
         * 例如IndexAction使用TransportIndexAction
         * @see TransportIndexAction
         * 但是实际现在底层用TransportBulkAction，旧的过期类是调用这个类
         * @see TransportBulkAction
         *
         * execute
         * 1。 先执行transportAction父类的execute
         * 2、执行transportAction子类的doExecute
         *
         * 上面indexaction
         * 1、先转成bulk请求
         * @see TransportSingleItemBulkWriteAction#doExecute(Task, ReplicatedWriteRequest, ActionListener)
         * 2. 执行bulk请求
         * @see org.elasticsearch.action.bulk.TransportBulkAction#doExecute(org.elasticsearch.tasks.Task, org.elasticsearch.action.bulk.BulkRequest, org.elasticsearch.action.ActionListener)
         */
        return transportAction(action).execute(request, listener);
    }

    /**
     * Execute an {@link ActionType} locally, returning that {@link Task} used to track it, and linking an {@link TaskListener}.
     * Prefer this method if you need access to the task when listening for the response.
     *
     * @throws TaskCancelledException if the request's parent task has been cancelled already
     */
    public <    Request extends ActionRequest,
                Response extends ActionResponse
            > Task executeLocally(ActionType<Response> action, Request request, TaskListener<Response> listener) {
        // 根据ActionType找到对应的transportAction，然后execute执行
        return transportAction(action).execute(request, listener);
    }

    /**
     * The id of the local {@link DiscoveryNode}. Useful for generating task ids from tasks returned by
     * {@link #executeLocally(ActionType, ActionRequest, TaskListener)}.
     */
    public String getLocalNodeId() {
        return localNodeId.get();
    }

    /**
     * Get the {@link TransportAction} for an {@link ActionType}, throwing exceptions if the action isn't available.
     */
    @SuppressWarnings("unchecked")
    private <    Request extends ActionRequest,
                Response extends ActionResponse
            > TransportAction<Request, Response> transportAction(ActionType<Response> action) {
        if (actions == null) {
            throw new IllegalStateException("NodeClient has not been initialized");
        }
        // ActionType类都有对应绑定的TransportAction
        TransportAction<Request, Response> transportAction = actions.get(action);
        if (transportAction == null) {
            throw new IllegalStateException("failed to find action [" + action + "] to execute");
        }
        return transportAction;
    }

    @Override
    public Client getRemoteClusterClient(String clusterAlias) {
        return remoteClusterService.getRemoteClusterClient(threadPool(), clusterAlias);
    }


    public NamedWriteableRegistry getNamedWriteableRegistry() {
        return namedWriteableRegistry;
    }
}
