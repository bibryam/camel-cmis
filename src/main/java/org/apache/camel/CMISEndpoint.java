/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel;

import org.apache.camel.impl.DefaultEndpoint;
import org.apache.chemistry.opencmis.client.api.Session;
import org.apache.chemistry.opencmis.client.api.SessionFactory;
import org.apache.chemistry.opencmis.client.runtime.SessionFactoryImpl;
import org.apache.chemistry.opencmis.commons.SessionParameter;
import org.apache.chemistry.opencmis.commons.enums.BindingType;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents a CMIS endpoint.
 */
public class CMISEndpoint extends DefaultEndpoint {
    private String userName;
    private String password;
    private String repositoryId;
    private String url;

    public CMISEndpoint() {
    }

    public CMISEndpoint(String uri, CMISComponent component) {
        super(uri, component);
    }

    public CMISEndpoint(String endpointUri) {
        super(endpointUri);
    }

    public CMISEndpoint(String uri, String remaining, CMISComponent cmisComponent) {
        this(uri, cmisComponent);
        this.url = remaining;
    }

    public Producer createProducer() throws Exception {
        return new CMISProducer(this);
    }

    public Consumer createConsumer(Processor processor) throws Exception {
        throw new UnsupportedOperationException("Consumer not supported for CMIS endpoint");
    }

    public boolean isSingleton() {
        return true;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getRepositoryId() {
        return repositoryId;
    }

    public void setRepositoryId(String repositoryId) {
        this.repositoryId = repositoryId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }


    public Session getSession() {
        SessionFactory sessionFactory = SessionFactoryImpl.newInstance();
        Map<String, String> parameter = new HashMap<String, String>(); //parameter.put(SessionParameter.ATOMPUB_URL, " http://repo.opencmis.org/inmemory/atom/");
        parameter.put(SessionParameter.ATOMPUB_URL, getUrl());
        parameter.put(SessionParameter.USER, getUserName());
        parameter.put(SessionParameter.PASSWORD, getPassword());
        parameter.put(SessionParameter.BINDING_TYPE, BindingType.ATOMPUB.value());

        if (getRepositoryId() != null) {
            parameter.put(SessionParameter.REPOSITORY_ID, getRepositoryId());
            return SessionFactoryImpl.newInstance().createSession(parameter);
        } else {
            return SessionFactoryImpl.newInstance().getRepositories(parameter).get(0).createSession();
        }
    }
}
