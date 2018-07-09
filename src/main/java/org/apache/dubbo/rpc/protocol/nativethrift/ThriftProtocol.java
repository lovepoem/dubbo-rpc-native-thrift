/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.rpc.protocol.nativethrift;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.protocol.AbstractProxyProtocol;

import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.lang.reflect.Constructor;

/**
 * native thrift protocol
 */
public class ThriftProtocol extends AbstractProxyProtocol {

    public static final int DEFAULT_PORT = 40880;

    public static final String NAME = "thrift";
    public static final String THRIFT_IFACE = "$Iface";
    public static final String THRIFT_PROCESSOR = "$Processor";
    public static final String THRIFT_CLIENT = "$Client";

    @Override
    public int getDefaultPort() {
        return DEFAULT_PORT;
    }

    public ThriftProtocol() {
        super(TException.class, RpcException.class);
    }

    @Override
    protected <T> Runnable doExport(T impl, Class<T> type, URL url) throws RpcException {
        return exportThreadedSelectorServer(impl, type, url);
    }

    @Override
    protected <T> T doRefer(Class<T> type, URL url) throws RpcException {
        return doReferFrameAndCompact(type, url);
    }

    public ThriftProtocol(Class<?>... exceptions) {
        super(exceptions);
    }

    private <T> Runnable exportThreadedSelectorServer(T impl, Class<T> type, URL url) throws RpcException {
        TProcessor tprocessor;
        TThreadedSelectorServer.Args tArgs = null;
        String typeName = type.getName();
        TNonblockingServerSocket transport;
        if (typeName.endsWith(THRIFT_IFACE)) {
            String processorClsName = typeName.substring(0, typeName.indexOf(THRIFT_IFACE)) + THRIFT_PROCESSOR;
            try {
                Class<?> clazz = Class.forName(processorClsName);
                Constructor constructor = clazz.getConstructor(type);
                try {
                    tprocessor = (TProcessor) constructor.newInstance(impl);

                    /**Solve the problem of only 50 of the default number of concurrent connections*/
                    TNonblockingServerSocket.NonblockingAbstractServerSocketArgs args = new TNonblockingServerSocket.NonblockingAbstractServerSocketArgs();
                    /**1000 connections*/
                    args.backlog(1000);
                    args.port(url.getPort());
                    /**timeout: 10s */
                    args.clientTimeout(10000);

                    transport = new TNonblockingServerSocket(args);

                    tArgs = new TThreadedSelectorServer.Args(transport);
                    tArgs.workerThreads(200);
                    tArgs.selectorThreads(4);
                    tArgs.acceptQueueSizePerThread(256);
                    tArgs.processor(tprocessor);
                    tArgs.transportFactory(new TFramedTransport.Factory());
                    tArgs.protocolFactory(new TCompactProtocol.Factory());
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    throw new RpcException("Fail to create thrift server(" + url + ") : " + e.getMessage(), e);
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                throw new RpcException("Fail to create thrift server(" + url + ") : " + e.getMessage(), e);
            }
        }

        if (tArgs == null) {
            logger.error("Fail to create thrift server(" + url + ") due to null args");
            throw new RpcException("Fail to create thrift server(" + url + ") due to null args");
        }
        final TServer thriftServer = new TThreadedSelectorServer(tArgs);

        new Thread(new Runnable() {

            @Override
            public void run() {
                logger.info("Start Thrift ThreadedSelectorServer");
                thriftServer.serve();
                logger.info("Thrift ThreadedSelectorServer started.");
            }
        }).start();

        return new Runnable() {
            @Override
            public void run() {
                try {
                    logger.info("Close Thrift NonblockingServer");
                    thriftServer.stop();
                } catch (Throwable e) {
                    logger.warn(e.getMessage(), e);
                }
            }
        };
    }

    private <T> T doReferFrameAndCompact(Class<T> type, URL url) throws RpcException {

        try {
            TSocket tSocket;
            TTransport transport;
            TProtocol protocol;
            T thriftClient = null;
            String typeName = type.getName();
            if (typeName.endsWith(THRIFT_IFACE)) {
                String clientClsName = typeName.substring(0, typeName.indexOf(THRIFT_IFACE)) + THRIFT_CLIENT;
                Class<?> clazz = Class.forName(clientClsName);
                Constructor constructor = clazz.getConstructor(TProtocol.class);
                try {
                    tSocket = new TSocket(url.getHost(), url.getPort());
                    transport = new TFramedTransport(tSocket);
                    protocol = new TCompactProtocol(transport);
                    thriftClient = (T) constructor.newInstance(protocol);
                    transport.open();
                    logger.info("thrift client opened for service(" + url + ")");
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    throw new RpcException("Fail to create remote client:" + e.getMessage(), e);
                }
            }
            return thriftClient;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RpcException("Fail to create remote client for service(" + url + "): " + e.getMessage(), e);
        }
    }

}
