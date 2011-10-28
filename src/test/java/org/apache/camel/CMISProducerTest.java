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
import org.apache.camel.impl.DefaultExchange;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.apache.chemistry.opencmis.client.api.*;
import org.apache.chemistry.opencmis.client.runtime.SessionFactoryImpl;
import org.apache.chemistry.opencmis.commons.PropertyIds;
import org.apache.chemistry.opencmis.commons.SessionParameter;
import org.apache.chemistry.opencmis.commons.data.ContentStream;
import org.apache.chemistry.opencmis.commons.enums.BindingType;
import org.apache.chemistry.opencmis.commons.exceptions.CmisInvalidArgumentException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.webapp.WebAppContext;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class CMISProducerTest extends CamelTestSupport {
    private static final String CMIS_ENDPOINT_TEST_SERVER = "http://localhost:9090/chemistry-opencmis-server-inmemory/atom";
    private static final String openCmisServerWarPath = "target/dependency/chemistry-opencmis-server-inmemory-war-0.5.0.war";

    private Server cmisServer;

    @Produce(uri = "direct:start")
    protected ProducerTemplate template;

    @Test
    public void storeMessageBodyAsTextDocument() throws Exception {
        String content = "Some content to be store";
        Exchange exchange = createExchangeWithOptionalInBody(content);
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
        Exchange exchange = createExchangeWithOptionalInBody("Some content to be store");
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
        Exchange exchange = createExchangeWithOptionalInBody("Some content that will fail to be stored");
        exchange.getIn().getHeaders().put(PropertyIds.CONTENT_STREAM_MIME_TYPE, "text/plain; charset=UTF-8");

        template.send(exchange);
        Exception exception = exchange.getException();
        Object body = exchange.getOut().getBody();

        assertNull(body);
        assertTrue(exception instanceof NoSuchHeaderException);
    }

    @Test
    public void createDocumentWithNoContentByExplicitlySpecifyingObjectTypeHeader() throws Exception {
        Exchange exchange = createExchangeWithOptionalInBody(null);
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
        Exchange exchange = createExchangeWithOptionalInBody(null);
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
        Exchange exchange = createExchangeWithOptionalInBody("Some content to be store");
        exchange.getIn().getHeaders().put(PropertyIds.CONTENT_STREAM_MIME_TYPE, "text/plain; charset=UTF-8");
        exchange.getIn().getHeaders().put(PropertyIds.NAME, "test.txt");

        template.send(exchange);
        String newNodeId = exchange.getOut().getBody(String.class);
        CmisObject newNode = retrieveCMISObjectByIdFromServer(newNodeId);

        assertEquals("test.txt", newNode.getPropertyValue(PropertyIds.NAME));
        assertEquals("text/plain; charset=UTF-8", newNode.getPropertyValue(PropertyIds.CONTENT_STREAM_MIME_TYPE));
    }

    @Test(expected = CmisInvalidArgumentException.class)
    public void failConnectingToNonExistingRepository() throws Exception {
        Endpoint endpoint = context.getEndpoint("cmis://" + CMIS_ENDPOINT_TEST_SERVER + "?userName=admin&password=admin&repositoryId=NON_EXISTING_ID");
        Producer producer = endpoint.createProducer();

        Exchange exchange = createExchangeWithOptionalInBody("Some content to be store");
        exchange.getIn().getHeaders().put(CMISParams.CMIS_MIME_TYPE, "text/plain; charset=UTF-8");
        exchange.getIn().getHeaders().put(PropertyIds.NAME, "test.txt");
        producer.process(exchange);
    }

    @Test
    public void createDocumentAtSpecificPath() throws Exception {
        String existingFolderStructure = "/My_Folder-0-0/My_Folder-1-0/My_Folder-2-0";

        Exchange exchange = createExchangeWithOptionalInBody("Some content to be stored");
        exchange.getIn().getHeaders().put(PropertyIds.CONTENT_STREAM_MIME_TYPE, "text/plain; charset=UTF-8");
        exchange.getIn().getHeaders().put(PropertyIds.NAME, "test.file");
        exchange.getIn().getHeaders().put(CMISParams.CMIS_FOLDER_PATH, existingFolderStructure);

        template.send(exchange);
        String newNodeId = exchange.getOut().getBody(String.class);

        Document document = (Document) retrieveCMISObjectByIdFromServer(newNodeId);
        String documentFullPath = document.getPaths().get(0);
        assertEquals(existingFolderStructure + "/test.file", documentFullPath);
    }

    @Test
    public void queryServer() throws Exception {


         SessionFactory sessionFactory = SessionFactoryImpl.newInstance();
        Map<String, String> parameter = new HashMap<String, String>();
        parameter.put(SessionParameter.ATOMPUB_URL, CMIS_ENDPOINT_TEST_SERVER);
        parameter.put(SessionParameter.BINDING_TYPE, BindingType.ATOMPUB.value());

        Repository repository = sessionFactory.getRepositories(parameter).get(0);
        Session session = repository.createSession();

                String query = "SELECT * FROM cmis:document";
        ItemIterable<QueryResult> q = session.query(query, false);

        // Did it work?
        System.out.println("***results from query " + query);

        int i = 1;
        for (QueryResult qr : q) {
            System.out.println("--------------------------------------------\n" + i + " , "
                    + qr.getPropertyByQueryName("cmis:objectTypeId").getFirstValue() + " , "
                    + qr.getPropertyByQueryName("cmis:name").getFirstValue() + " , "
                    + qr.getPropertyByQueryName("cmis:createdBy").getFirstValue() + " , "
                    + qr.getPropertyByQueryName("cmis:objectId").getFirstValue() + " , "
                    + qr.getPropertyByQueryName("cmis:contentStreamFileName").getFirstValue() + " , "
                    + qr.getPropertyByQueryName("cmis:contentStreamMimeType").getFirstValue() + " , "
                    + qr.getPropertyByQueryName("cmis:contentStreamLength").getFirstValue());
            i++;
        }



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

    private Exchange createExchangeWithOptionalInBody(String body) {
        DefaultExchange exchange = new DefaultExchange(context);
        if (body != null) {
            exchange.getIn().setBody(body);
        }
        return exchange;
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

    @Override
    @Before
    public void setUp() throws Exception {
        cmisServer = new Server(9090);
        WebAppContext openCmisServerApi = new WebAppContext(openCmisServerWarPath, "/chemistry-opencmis-server-inmemory");
        cmisServer.addHandler(openCmisServerApi);
        cmisServer.start();
        super.setUp();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        cmisServer.stop();
        super.tearDown();
    }
}
