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

import org.apache.camel.builder.RouteBuilder;
import org.apache.chemistry.opencmis.client.api.*;
import org.apache.chemistry.opencmis.client.runtime.SessionFactoryImpl;
import org.apache.chemistry.opencmis.commons.PropertyIds;
import org.apache.chemistry.opencmis.commons.SessionParameter;
import org.apache.chemistry.opencmis.commons.data.ContentStream;
import org.apache.chemistry.opencmis.commons.enums.BindingType;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class CMISProducerTest extends CMISTestSupport {

    @Test
    public void storeMessageBodyAsTextDocument() throws Exception {
        String content = "Some content to be store";
        Exchange exchange = createExchangeWithInBody(content);
        exchange.getIn().getHeaders().put(PropertyIds.CONTENT_STREAM_MIME_TYPE, "text/plain; charset=UTF-8");
        exchange.getIn().getHeaders().put(PropertyIds.NAME, "test.file");

        template.send(exchange);
        String newNodeId = exchange.getOut().getBody(String.class);
        assertNotNull(newNodeId);

        String newNodeContent = getDocumentContentAsString(newNodeId);
        assertEquals(content, newNodeContent);
    }

    @Test
    public void getDocumentMimeTypeFromMessageContentType() throws Exception {
        Exchange exchange = createExchangeWithInBody("Some content to be store");
        exchange.getIn().getHeaders().put(Exchange.CONTENT_TYPE, "text/plain");
        exchange.getIn().getHeaders().put(PropertyIds.NAME, "test.file");

        template.send(exchange);
        String newNodeId = exchange.getOut().getBody(String.class);

        CmisObject cmisObject = retrieveCMISObjectByIdFromServer(newNodeId);
        Document doc = (Document) cmisObject;
        assertEquals("text/plain", doc.getPropertyValue(PropertyIds.CONTENT_STREAM_MIME_TYPE));
    }

    @Test
    public void namePropertyIsAlwaysRequired() {
        Exchange exchange = createExchangeWithInBody("Some content that will fail to be stored");
        exchange.getIn().getHeaders().put(PropertyIds.CONTENT_STREAM_MIME_TYPE, "text/plain; charset=UTF-8");

        template.send(exchange);
        Exception exception = exchange.getException();
        Object body = exchange.getOut().getBody();

        assertNull(body);
        assertTrue(exception instanceof NoSuchHeaderException);
    }

    @Test
    public void createDocumentWithoutContentByExplicitlySpecifyingObjectTypeHeader() throws Exception {
        Exchange exchange = createExchangeWithInBody(null);
        exchange.getIn().getHeaders().put(PropertyIds.CONTENT_STREAM_MIME_TYPE, "text/plain; charset=UTF-8");
        exchange.getIn().getHeaders().put(PropertyIds.OBJECT_TYPE_ID, "cmis:document");
        exchange.getIn().getHeaders().put(PropertyIds.NAME, "test.file");

        template.send(exchange);
        String newNodeId = exchange.getOut().getBody(String.class);
        assertNotNull(newNodeId);

        CmisObject cmisObject = retrieveCMISObjectByIdFromServer(newNodeId);
        Document doc = (Document) cmisObject;
        assertEquals("cmis:document", doc.getPropertyValue(PropertyIds.OBJECT_TYPE_ID));
    }

    @Test
    public void emptyBodyAndMissingObjectTypeHeaderCreatesFolderNode() throws Exception {
        Exchange exchange = createExchangeWithInBody(null);
        exchange.getIn().getHeaders().put(PropertyIds.NAME, "testFolder");

        template.send(exchange);
        String newNodeId = exchange.getOut().getBody(String.class);
        assertNotNull(newNodeId);

        CmisObject newNode = retrieveCMISObjectByIdFromServer(newNodeId);
        assertEquals("cmis:folder", newNode.getType().getId());
        assertTrue(newNode instanceof Folder);
    }

    @Test
    public void cmisPropertiesAreStored() throws Exception {
        Exchange exchange = createExchangeWithInBody("Some content to be store");
        exchange.getIn().getHeaders().put(PropertyIds.CONTENT_STREAM_MIME_TYPE, "text/plain; charset=UTF-8");
        exchange.getIn().getHeaders().put(PropertyIds.NAME, "test.txt");

        template.send(exchange);
        String newNodeId = exchange.getOut().getBody(String.class);
        CmisObject newNode = retrieveCMISObjectByIdFromServer(newNodeId);

        assertEquals("test.txt", newNode.getPropertyValue(PropertyIds.NAME));
        assertEquals("text/plain; charset=UTF-8", newNode.getPropertyValue(PropertyIds.CONTENT_STREAM_MIME_TYPE));
    }

    @Test(expected = ResolveEndpointFailedException.class)
    public void failConnectingToNonExistingRepository() throws Exception {
        Endpoint endpoint = context.getEndpoint("cmis://" + CMIS_ENDPOINT_TEST_SERVER + "?username=admin&password=admin&repositoryId=NON_EXISTING_ID");
        Producer producer = endpoint.createProducer();

        Exchange exchange = createExchangeWithInBody("Some content to be store");
        exchange.getIn().getHeaders().put(CamelCMISParams.CMIS_MIME_TYPE, "text/plain; charset=UTF-8");
        exchange.getIn().getHeaders().put(PropertyIds.NAME, "test.txt");
        producer.process(exchange);
    }

    @Test
    public void createDocumentAtSpecificPath() throws Exception {
        String existingFolderStructure = "/My_Folder-0-0/My_Folder-1-0/My_Folder-2-0";

        Exchange exchange = createExchangeWithInBody("Some content to be stored");
        exchange.getIn().getHeaders().put(PropertyIds.CONTENT_STREAM_MIME_TYPE, "text/plain; charset=UTF-8");
        exchange.getIn().getHeaders().put(PropertyIds.NAME, "test.file");
        exchange.getIn().getHeaders().put(CamelCMISParams.CMIS_FOLDER_PATH, existingFolderStructure);

        template.send(exchange);
        String newNodeId = exchange.getOut().getBody(String.class);

        Document document = (Document) retrieveCMISObjectByIdFromServer(newNodeId);
        String documentFullPath = document.getPaths().get(0);
        assertEquals(existingFolderStructure + "/test.file", documentFullPath);
    }

    @Test
    public void failCreatingFolderAtNonExistingPath() throws Exception {
        String existingFolderStructure = "/No/Path/Here";

        Exchange exchange = createExchangeWithInBody(null);
        exchange.getIn().getHeaders().put(PropertyIds.NAME, "folder1");
        exchange.getIn().getHeaders().put(PropertyIds.OBJECT_TYPE_ID, "cmis:folder");
        exchange.getIn().getHeaders().put(CamelCMISParams.CMIS_FOLDER_PATH, existingFolderStructure);

        template.send(exchange);
        assertTrue(exchange.getException() instanceof RuntimeExchangeException);
    }

    private CmisObject retrieveCMISObjectByIdFromServer(String nodeId) throws Exception {
        SessionFactory sessionFactory = SessionFactoryImpl.newInstance();
        Map<String, String> parameter = new HashMap<String, String>();
        parameter.put(SessionParameter.ATOMPUB_URL, CMIS_ENDPOINT_TEST_SERVER);
        parameter.put(SessionParameter.BINDING_TYPE, BindingType.ATOMPUB.value());

        Repository repository = sessionFactory.getRepositories(parameter).get(0);
        Session session = repository.createSession();
        return session.getObject(nodeId);
    }

    private String getDocumentContentAsString(String nodeId) throws Exception {
        CmisObject cmisObject = retrieveCMISObjectByIdFromServer(nodeId);
        Document doc = (Document) cmisObject;
        return getContentAsString(doc.getContentStream());
    }

    private static String getContentAsString(ContentStream stream) throws IOException {
        InputStream inputStream = stream.getStream();
        StringBuffer stringBuffer = new StringBuffer(inputStream.available());
        int count;
        byte[] buf2 = new byte[100];
        while ((count = inputStream.read(buf2)) != -1) {
            for (int i = 0; i < count; i++) {
                stringBuffer.append((char) buf2[i]);
            }
        }
        inputStream.close();
        return stringBuffer.toString();
    }

    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        return new RouteBuilder() {
            public void configure() {
                from("direct:start")
                        .to("cmis://" + CMIS_ENDPOINT_TEST_SERVER)
                        .to("mock:result");
            }
        };
    }

}
