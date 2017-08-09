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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.tongweb.samples.filewatch;

import javax.naming.InitialContext;
import javax.resource.ResourceException;
import javax.resource.spi.*;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.transaction.xa.XAResource;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.System.out;
import static java.nio.file.StandardWatchEventKinds.*;

/**
 * @author Robert Panzer (robert.panzer@me.com)
 * @author Bartosz Majsak (bartosz.majsak@gmail.com)
 */
@Connector
public class FileSystemWatcherResourceAdapter implements ResourceAdapter {

    private BootstrapContext bootstrapContext;
    
    private FileSystem fileSystem;
    private WatchService watchService;
    private Map<WatchKey, MessageEndpointFactory> listeners = new ConcurrentHashMap<>();
    private Map<MessageEndpointFactory, Class<?>> endpointFactoryToBeanClass = new ConcurrentHashMap<>();
    
    @Override
    public void start(BootstrapContext bootstrapContext) throws ResourceAdapterInternalException {
        
        out.println("----[RA] "+this.getClass().getSimpleName() + " resource adapater started");
        
        this.bootstrapContext = bootstrapContext;

        try {
            fileSystem = FileSystems.getDefault();
            watchService = fileSystem.newWatchService();
        } catch (IOException e) {
            throw new ResourceAdapterInternalException(e);
        }

        new WatchingThread(watchService, this).start();
    }

    @Override
    public void endpointActivation(MessageEndpointFactory endpointFactory, ActivationSpec activationSpec) throws ResourceException {
        lookupEnv();
        out.println("----[RA] "+this.getClass().getSimpleName() + " resource adapater endpoint activated for " + endpointFactory.getEndpointClass());
        out.println("----[RA]  resource adapater endpoint activated with activation name : " + endpointFactory.getActivationName());
        FileSystemWatcherActivationSpec fsWatcherAS = (FileSystemWatcherActivationSpec) activationSpec;

        try {
            WatchKey watchKey = 
                fileSystem.getPath(fsWatcherAS.getDir())
                          .register(watchService, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);

            listeners.put(watchKey, endpointFactory);

            endpointFactoryToBeanClass.put(endpointFactory, endpointFactory.getEndpointClass());
        } catch (IOException e) {
            throw new ResourceException(e);
        }
    }

    @Override
    public void endpointDeactivation(MessageEndpointFactory endpointFactory, ActivationSpec activationSpec) {
        lookupEnv();
        out.println("----[RA] "+this.getClass().getSimpleName() + " resource adapater endpoint deactivated for " + endpointFactory.getEndpointClass());
        
        for (WatchKey watchKey : listeners.keySet()) {
            if (listeners.get(watchKey) == endpointFactory) {
                listeners.remove(watchKey);
                break;
            }
        }
        endpointFactoryToBeanClass.remove(endpointFactory);
    }

    @Override
    public XAResource[] getXAResources(ActivationSpec[] arg0) throws ResourceException {
        return new XAResource[0];
    }

    @Override
    public void stop() {
        
        out.println("----[RA] "+this.getClass().getSimpleName() + " resource adapater stopping");
        
        try {
            watchService.close();
        } catch (IOException e) {
            throw new RuntimeException("Failed stopping file watcher.", e);
        }
    }

    public MessageEndpointFactory getListener(WatchKey watchKey) {
        return listeners.get(watchKey);
    }

    public BootstrapContext getBootstrapContext() {
        return bootstrapContext;
    }

    public Class<?> getBeanClass(MessageEndpointFactory endpointFactory) {
        return endpointFactoryToBeanClass.get(endpointFactory);
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    private void lookupEnv() {

        try {
            InitialContext ctx=new InitialContext();
            String sqType =(String) ctx.lookup("java:comp/env/sql_type");
            out.println("----[RA] lookup component env: " +sqType);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
