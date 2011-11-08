package org.apache.camel;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.chemistry.opencmis.client.api.Folder;
import org.junit.Before;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class CMISConsumerTest extends CMISTestSupport {

    @EndpointInject(uri = "mock:result")
    protected MockEndpoint resultEndpoint;

    @Test
    public void getAllContentFromServerOrderedFromRootToLeaves() throws Exception {
        resultEndpoint.expectedMessageCount(5);
        resultEndpoint.assertIsSatisfied();

        List<Exchange> exchanges = resultEndpoint.getExchanges();
        assertTrue(getNodeNameForIndex(exchanges, 0).equals("RootFolder"));
        assertTrue(getNodeNameForIndex(exchanges, 1).equals("Folder1"));
        assertTrue(getNodeNameForIndex(exchanges, 2).equals("Folder2"));
        assertTrue(getNodeNameForIndex(exchanges, 3).contains(".txt") );
        assertTrue(getNodeNameForIndex(exchanges, 4).contains(".txt"));
    }

    private String getNodeNameForIndex(List<Exchange> exchanges, int index) {
        return exchanges.get(index).getIn().getHeader("cmis:name", String.class);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        deleteAllContent();
        populateServerWithContent();
    }

    private void populateServerWithContent() throws UnsupportedEncodingException {
        Folder folder1 = createFolderWithName("Folder1");
        Folder folder2 = createChildFolderWithName(folder1, "Folder2");
        createTextDocument(folder2, "Document2.1", "2.1.txt");
        createTextDocument(folder2, "Document2.2", "2.2.txt");
        //L0              ROOT
        //                |
        //L1            Folder1
        //L2              |_____Folder2
        //                        ||
        //L3            Doc2.1___||___Doc2.2
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
