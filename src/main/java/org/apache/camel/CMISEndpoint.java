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
import org.apache.chemistry.opencmis.client.runtime.SessionFactoryImpl;
import org.apache.chemistry.opencmis.commons.SessionParameter;
import org.apache.chemistry.opencmis.commons.enums.BindingType;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents a CMIS endpoint.
 */
public class CMISEndpoint extends DefaultEndpoint {
    private String username;
    private String password;
    private String repositoryId;
    private String url;
    private boolean query;
    private Session session;

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
        CMISSessionFacade cmisSessionFacade = new CMISSessionFacade(this.session);
        if (this.query) {
            return new CMISQueryProducer(this, cmisSessionFacade);
        }
        return new CMISProducer(this, cmisSessionFacade);
    }

    public Consumer createConsumer(Processor processor) throws Exception {
        throw new UnsupportedOperationException("Consumer not supported for CMIS endpoint");
    }

    public boolean isSingleton() {
        return true;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setRepositoryId(String repositoryId) {
        this.repositoryId = repositoryId;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setQuery(boolean query) {
        this.query = query;
    }

    void initSession() {
        Map<String, String> parameter = new HashMap<String, String>();
        parameter.put(SessionParameter.BINDING_TYPE, BindingType.ATOMPUB.value());
        parameter.put(SessionParameter.ATOMPUB_URL, this.url);
        parameter.put(SessionParameter.USER, this.username);
        parameter.put(SessionParameter.PASSWORD, this.password);
        if (this.repositoryId != null) {
            parameter.put(SessionParameter.REPOSITORY_ID, this.repositoryId);
            this.session = SessionFactoryImpl.newInstance().createSession(parameter);
        } else {
            this.session = SessionFactoryImpl.newInstance().getRepositories(parameter).get(0).createSession();
        }
    }
}
