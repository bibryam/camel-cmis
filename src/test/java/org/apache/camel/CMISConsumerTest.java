package org.apache.camel;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.chemistry.opencmis.client.api.Folder;
import org.junit.Before;
import org.junit.Test;

import java.io.UnsupportedEncodingException;

public class CMISConsumerTest extends CMISTestSupport {

    @EndpointInject(uri = "mock:result")
    protected MockEndpoint resultEndpoint;

    @Test
    public void queryServerForDocumentWithSpecificName() throws Exception {
        resultEndpoint.expectedMessageCount(6);
        resultEndpoint.assertIsSatisfied();
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        deleteAllContent();
        populateServerWithContent();
    }

    private void populateServerWithContent() throws UnsupportedEncodingException {
        Folder folder1 = createFolderWithName("Folder 1");
        createTextDocument(folder1, "Doc 1.1", "1.txt");
        Folder folder2 = createChildFolderWithName(folder1, "Folder 1.2");
        createTextDocument(folder2, "Document2.1", "2.1.txt");
        createTextDocument(folder2, "Document2.2", "2.2.txt");
    }

    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        return new RouteBuilder() {
            public void configure() {
                from("cmis://" + CMIS_ENDPOINT_TEST_SERVER).to("mock:result");
            }
        };
    }
}
