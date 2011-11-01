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

import org.apache.camel.impl.DefaultProducer;
import org.apache.camel.util.ExchangeHelper;
import org.apache.camel.util.MessageHelper;
import org.apache.chemistry.opencmis.client.api.Document;
import org.apache.chemistry.opencmis.client.api.Folder;
import org.apache.chemistry.opencmis.client.api.Session;
import org.apache.chemistry.opencmis.commons.PropertyIds;
import org.apache.chemistry.opencmis.commons.data.ContentStream;
import org.apache.chemistry.opencmis.commons.enums.VersioningState;
import org.apache.chemistry.opencmis.commons.exceptions.CmisObjectNotFoundException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * The CMIS producer.
 */
public class CMISProducer extends DefaultProducer {
    private static final transient Log LOG = LogFactory.getLog(CMISProducer.class);
    private CMISEndpoint endpoint;

    public CMISProducer(CMISEndpoint endpoint) {
        super(endpoint);
        this.endpoint = endpoint;
    }

    public void process(Exchange exchange) throws Exception {
        String nodeId = createNode(exchange);
        exchange.getOut().setBody(nodeId);
    }

    private String createNode(Exchange exchange) throws Exception {
        validateRequiredHeader(exchange, PropertyIds.NAME);

        Session session = this.endpoint.getSession();
        Folder parentFolder = getFolderFromPathOrRoot(exchange, session);
        Map<String, Object> cmisProperties = filterCMISProperties(exchange.getIn().getHeaders());

        if (isDocumentCreation(exchange)) {
            cmisProperties.put(PropertyIds.OBJECT_TYPE_ID, "cmis:document");
            ContentStream contentStream = createContentStream(exchange.getIn(), session);
            Document document = parentFolder.createDocument(cmisProperties, contentStream, VersioningState.NONE);
            return document.getId();
        } else {
            cmisProperties.put(PropertyIds.OBJECT_TYPE_ID, "cmis:folder");
            Folder newFolder = parentFolder.createFolder(cmisProperties);
            return newFolder.getId();
        }
    }

    private void validateRequiredHeader(Exchange exchange, String name) throws NoSuchHeaderException {
        ExchangeHelper.getMandatoryHeader(exchange, name, String.class);
    }

    private boolean isDocumentCreation(Exchange exchange) {
        String objectType = exchange.getIn().getHeader(PropertyIds.OBJECT_TYPE_ID, String.class);
        if (objectType != null) {
            return objectType.equals("cmis:document");
        }
        return exchange.getIn().getBody() != null;
    }

    private ContentStream createContentStream(Message message, Session session) throws Exception {
        if (message.getBody() == null) {
            return null;
        }

        String fileName = message.getHeader(PropertyIds.NAME, String.class);
        String mimeType = getMimeType(message);
        byte[] buf = getBodyData(message);
        return session.getObjectFactory().createContentStream(fileName, buf.length, mimeType, new ByteArrayInputStream(buf));
    }

    private byte[] getBodyData(Message message) {
        return message.getBody(new byte [0].getClass());
    }

    private String getMimeType(Message message) throws NoSuchHeaderException {
        String mimeType = message.getHeader(PropertyIds.CONTENT_STREAM_MIME_TYPE, String.class);
        if (mimeType == null) {
            mimeType = MessageHelper.getContentType(message);
        }
        return mimeType;
    }

    private Folder getFolderFromPathOrRoot(Exchange exchange, Session session) {
        String path = exchange.getIn().getHeader(CMISParams.CMIS_FOLDER_PATH, "/", String.class);
        try {
            return (Folder) session.getObjectByPath(path);
        } catch (CmisObjectNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, Object> filterCMISProperties(Map<String, Object> properties) {
        Map<String, Object> result = new HashMap<String, Object>(properties.size());
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            if (entry.getKey().startsWith("cmis:")) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }
}
