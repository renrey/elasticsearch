/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.FilterClient;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskAction.TASKS_ORIGIN;

/**
 * A {@linkplain Client} that cancels tasks executed locally when the provided {@link HttpChannel}
 * is closed before completion.
 *
 * 作用：当HttpChannel被关闭时，如果本地task未完成，可以被取消
 */
public class RestCancellableNodeClient extends FilterClient {
    private static final Map<HttpChannel, CloseListener> httpChannels = new ConcurrentHashMap<>();

    private final NodeClient client;
    private final HttpChannel httpChannel;

    public RestCancellableNodeClient(NodeClient client, HttpChannel httpChannel) {
        super(client);
        this.client = client;
        this.httpChannel = httpChannel;
    }

    /**
     * Returns the number of channels tracked globally.
     */
    public static int getNumChannels() {
        return httpChannels.size();
    }

    /**
     * Returns the number of tasks tracked globally.
     */
    static int getNumTasks() {
        return httpChannels.values().stream()
            .mapToInt(CloseListener::getNumTasks)
            .sum();
    }

    /**
     * Returns the number of tasks tracked by the provided {@link HttpChannel}.
     */
    static int getNumTasks(HttpChannel channel) {
        CloseListener listener = httpChannels.get(channel);
        return listener == null ? 0 : listener.getNumTasks();
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
        ActionType<Response> action, Request request, ActionListener<Response> listener) {

        // 通过下面的可知，这层主要作用就是可取消请求
        // 可取消指定的整个http请求：
        // 1. 保存http channel、等http连接相关
        // 2. 生成可取消的task 标志

        /**
         * 1. 保存当前连接的httpChannel，并创建1个CloseListener（用于后续关闭使用，就是保存一些现在有的属性，例如当前连接有什么请求）
         */
        CloseListener closeListener = httpChannels.computeIfAbsent(httpChannel, channel -> new CloseListener());
        TaskHolder taskHolder = new TaskHolder();
        /**
         * 执行任务核心
         * 2.  执行NodeClient的executeLocally : 即把action转成TransportAction, 交给集群执行
         * 得到这次执行的task对象,为了拿到taskId
         * @see NodeClient#executeLocally(ActionType, ActionRequest, ActionListener)
         *
         */
        Task task = client.executeLocally(action, request,
            // 可以看到这里的回调作用就是把当前请求的task从 （对应连接）closeListener的tasks中去除
            new ActionListener<Response>() {
                @Override
                public void onResponse(Response response) {
                    try {
                        // 完成就是不保存task
                        closeListener.unregisterTask(taskHolder);
                    } finally {
                        listener.onResponse(response);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    try {
                        closeListener.unregisterTask(taskHolder);
                    } finally {
                        listener.onFailure(e);
                    }
                }
            });
        assert task instanceof CancellableTask : action.name() + " is not cancellable";
        // 生成task的唯一标识（!!!!） -> 可用来对task进行取消
        final TaskId taskId = new TaskId(client.getLocalNodeId(), task.getId());
        /**
         * 3。 把task注册到closeListener，方便后续调用取消
         * 作用：保存taskId
         */
        closeListener.registerTask(taskHolder, taskId);
        /**
         * 4. 把closeListener(实际就是RestCancellableNodeClient自身)注册到httpChannel的关闭回调函数中
         * 作用：当httpChannel执行关闭时，调用这个RestCancellableNodeClient的onResponse、onFailure，进行当前task的清理取消（实际往集群发送取消task的请求）
         */
        closeListener.maybeRegisterChannel(httpChannel);
    }

    private void cancelTask(TaskId taskId) {
        /**
         *1。  生成1个CancelTasksRequest请求
         */
        CancelTasksRequest req = new CancelTasksRequest()
            .setTaskId(taskId)
            .setReason("channel closed");
        // force the origin to execute the cancellation as a system user
        /**
         * 2. 发送取消请求到集群
         */
        new OriginSettingClient(client, TASKS_ORIGIN).admin().cluster().cancelTasks(req, ActionListener.wrap(() -> {}));
    }

    private class CloseListener implements ActionListener<Void> {
        private final AtomicReference<HttpChannel> channel = new AtomicReference<>();
        private final Set<TaskId> tasks = new HashSet<>();

        CloseListener() {
        }

        synchronized int getNumTasks() {
            return tasks.size();
        }

        void maybeRegisterChannel(HttpChannel httpChannel) {
            if (channel.compareAndSet(null, httpChannel)) {
                //In case the channel is already closed when we register the listener, the listener will be immediately executed which will
                //remove the channel from the map straight-away. That is why we first create the CloseListener and later we associate it
                //with the channel. This guarantees that the close listener is already in the map when it gets registered to its
                //corresponding channel, hence it is always found in the map when it gets invoked if the channel gets closed.
                httpChannel.addCloseListener(this);
            }
        }

        synchronized void registerTask(TaskHolder taskHolder, TaskId taskId) {
            taskHolder.taskId = taskId;
            if (taskHolder.completed == false) {
                this.tasks.add(taskId);
            }
        }

        synchronized void unregisterTask(TaskHolder taskHolder) {
            if (taskHolder.taskId != null) {
                this.tasks.remove(taskHolder.taskId);
            }
            taskHolder.completed = true;
        }

        @Override
        public void onResponse(Void aVoid) {
            final HttpChannel httpChannel = channel.get();
            assert httpChannel != null : "channel not registered";
            // when the channel gets closed it won't be reused: we can remove it from the map and forget about it.
            CloseListener closeListener = httpChannels.remove(httpChannel);
            assert closeListener != null : "channel not found in the map of tracked channels";
            final List<TaskId> toCancel;
            synchronized (this) {
                toCancel = new ArrayList<>(tasks);
                tasks.clear();
            }
            // 每个taskId都取消
            for (TaskId taskId : toCancel) {
                cancelTask(taskId);
            }
        }

        @Override
        public void onFailure(Exception e) {
            onResponse(null);
        }
    }

    private static class TaskHolder {
        private TaskId taskId;
        private boolean completed = false;
    }
}
