package org.apache.camel;

import org.apache.chemistry.opencmis.client.api.Folder;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

public class CMISQueryProducerTest extends CMISTestSupport {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        populateServerWithContent();
    }

    @Test
    public void queryServerForDocumentWithSpecificName() throws Exception {
        Endpoint endpoint = context.getEndpoint("cmis://" + CMIS_ENDPOINT_TEST_SERVER + "?queryMode=true");
        Producer producer = endpoint.createProducer();

        Exchange exchange = createExchangeWithInBody("SELECT * FROM cmis:document WHERE cmis:name = 'test1.txt'");
        producer.process(exchange);

        List<Map<String, Object>> documents = exchange.getOut().getBody(List.class);
        assertEquals(1, documents.size());
        assertEquals("test1.txt", documents.get(0).get("cmis:name"));
    }

    @Test
    public void getResultCountFromHeader() throws Exception {
        Endpoint endpoint = context.getEndpoint("cmis://" + CMIS_ENDPOINT_TEST_SERVER + "?queryMode=true");
        Producer producer = endpoint.createProducer();

        Exchange exchange = createExchangeWithInBody("SELECT * FROM cmis:document WHERE CONTAINS('Camel test content.')");
        producer.process(exchange);

        List<Map<String, Object>> documents = exchange.getOut().getBody(List.class);
        assertEquals(2, documents.size());
        assertEquals(2, exchange.getOut().getHeader("CamelCMISResultCount"));
    }

    @Test
    public void limitNumberOfResultsWithReadSizeHeader() throws Exception {
        Endpoint endpoint = context.getEndpoint("cmis://" + CMIS_ENDPOINT_TEST_SERVER + "?queryMode=true");
        Producer producer = endpoint.createProducer();

        Exchange exchange = createExchangeWithInBody("SELECT * FROM cmis:document WHERE CONTAINS('Camel test content.')");
        exchange.getIn().getHeaders().put("CamelCMISReadSize", 1);

        producer.process(exchange);

        List<Map<String, Object>> documents = exchange.getOut().getBody(List.class);
        assertEquals(1, documents.size());
    }

    @Test
    public void retrieveAlsoDocumentContent() throws Exception {
        Endpoint endpoint = context.getEndpoint("cmis://" + CMIS_ENDPOINT_TEST_SERVER + "?queryMode=true");
        Producer producer = endpoint.createProducer();

        Exchange exchange = createExchangeWithInBody("SELECT * FROM cmis:document WHERE cmis:name='test1.txt'");
        exchange.getIn().getHeaders().put("CamelCMISRetrieveContent", true);

        producer.process(exchange);

        List<Map<String, Object>> documents = exchange.getOut().getBody(List.class);
        InputStream content = (InputStream) documents.get(0).get("CamelCMISContent");
        assertEquals("This is the first Camel test content.", readFromStream(content));
    }

    private void populateServerWithContent() throws UnsupportedEncodingException {
        Folder newFolder = createFolderWithName("CamelCmisTestFolder");
        createTextDocument(newFolder, "This is the first Camel test content.", "test1.txt");
        createTextDocument(newFolder, "This is the second Camel test content.", "test2.txt");
    }

}
