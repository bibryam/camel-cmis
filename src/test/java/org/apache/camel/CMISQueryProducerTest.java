package org.apache.camel;

import org.apache.chemistry.opencmis.client.api.Folder;
import org.apache.chemistry.opencmis.client.api.Session;
import org.apache.chemistry.opencmis.client.runtime.SessionFactoryImpl;
import org.apache.chemistry.opencmis.commons.PropertyIds;
import org.apache.chemistry.opencmis.commons.SessionParameter;
import org.apache.chemistry.opencmis.commons.data.ContentStream;
import org.apache.chemistry.opencmis.commons.enums.BindingType;
import org.apache.chemistry.opencmis.commons.enums.VersioningState;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.HashMap;
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
        Endpoint endpoint = context.getEndpoint("cmis://" + CMIS_ENDPOINT_TEST_SERVER + "?query=true");
        Producer producer = endpoint.createProducer();

        Exchange exchange = createExchangeWithInBody("SELECT * FROM cmis:document WHERE cmis:name = 'test1.txt'");
        producer.process(exchange);

        List<Map<String, Object>> documents = exchange.getOut().getBody(List.class);
        assertEquals(1, documents.size());
        assertEquals("test1.txt", documents.get(0).get("cmis:name"));
    }

    @Test
    public void getResultCountFromHeader() throws Exception {
        Endpoint endpoint = context.getEndpoint("cmis://" + CMIS_ENDPOINT_TEST_SERVER + "?query=true");
        Producer producer = endpoint.createProducer();

        Exchange exchange = createExchangeWithInBody("SELECT * FROM cmis:document WHERE CONTAINS('Camel test content.')");
        producer.process(exchange);

        List<Map<String, Object>> documents = exchange.getOut().getBody(List.class);
        assertEquals(2, documents.size());
        assertEquals(2, exchange.getOut().getHeader("CamelCMISResultCount"));
    }

    @Test
    public void limitNumberOfResultsWithReadSizeHeader() throws Exception {
        Endpoint endpoint = context.getEndpoint("cmis://" + CMIS_ENDPOINT_TEST_SERVER + "?query=true");
        Producer producer = endpoint.createProducer();

        Exchange exchange = createExchangeWithInBody("SELECT * FROM cmis:document WHERE CONTAINS('Camel test content.')");
        exchange.getIn().getHeaders().put("CamelCMISReadSize", 1);

        producer.process(exchange);

        List<Map<String, Object>> documents = exchange.getOut().getBody(List.class);
        assertEquals(1, documents.size());
    }

    @Test
    public void retrieveAlsoDocumentContent() throws Exception {
        Endpoint endpoint = context.getEndpoint("cmis://" + CMIS_ENDPOINT_TEST_SERVER + "?query=true");
        Producer producer = endpoint.createProducer();

        Exchange exchange = createExchangeWithInBody("SELECT * FROM cmis:document WHERE cmis:name='test1.txt'");
        exchange.getIn().getHeaders().put("CamelCMISRetrieveContent", true);

        producer.process(exchange);

        List<Map<String, Object>> documents = exchange.getOut().getBody(List.class);
        InputStream content = (InputStream) documents.get(0).get("CamelCMISContent");
        assertEquals("This is the first Camel test content.", readFromStream(content));
    }

    private String readFromStream(InputStream in) throws Exception {
        StringBuilder result = new StringBuilder();
        BufferedReader br = new BufferedReader(new InputStreamReader(in));

        String strLine;
        while ((strLine = br.readLine()) != null) {
            result.append(strLine);
        }
        in.close();
        return result.toString();
    }

    private void populateServerWithContent() throws UnsupportedEncodingException {
        Session session = getSession();
        Folder newFolder = createFolderWithName(session, "CamelCmisTestFolder");
        createTextDocument(session, newFolder, "This is the first Camel test content.", "test1.txt");
        createTextDocument(session, newFolder, "This is the second Camel test content.", "test2.txt");
    }

    private void createTextDocument(Session session, Folder newFolder, String content, String fileName) throws UnsupportedEncodingException {
        byte[] buf = content.getBytes("UTF-8");
        ByteArrayInputStream input = new ByteArrayInputStream(buf);
        ContentStream contentStream = session.getObjectFactory().createContentStream(fileName, buf.length, "text/plain; charset=UTF-8", input);

        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put(PropertyIds.OBJECT_TYPE_ID, "cmis:document");
        properties.put(PropertyIds.NAME, fileName);
        newFolder.createDocument(properties, contentStream, VersioningState.NONE);
    }

    private Folder createFolderWithName(Session session, String folderName) {
        Map<String, String> newFolderProps = new HashMap<String, String>();
        newFolderProps.put(PropertyIds.OBJECT_TYPE_ID, "cmis:folder");
        newFolderProps.put(PropertyIds.NAME, folderName);
        return session.getRootFolder().createFolder(newFolderProps);
    }

    private Session getSession() {
        Map<String, String> parameter = new HashMap<String, String>();
        parameter.put(SessionParameter.ATOMPUB_URL, CMIS_ENDPOINT_TEST_SERVER);
        parameter.put(SessionParameter.BINDING_TYPE, BindingType.ATOMPUB.value());
        return SessionFactoryImpl.newInstance().getRepositories(parameter).get(0).createSession();
    }
}
