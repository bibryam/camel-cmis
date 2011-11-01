package org.apache.camel;

import org.apache.camel.impl.DefaultExchange;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.After;
import org.junit.Before;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.webapp.WebAppContext;

public class CMISTestSupport extends CamelTestSupport {
    protected static final String CMIS_ENDPOINT_TEST_SERVER = "http://localhost:9090/chemistry-opencmis-server-inmemory/atom";
    protected static final String openCmisServerWarPath = "target/dependency/chemistry-opencmis-server-inmemory-war-0.5.0.war";

    protected Server cmisServer;

    @Produce(uri = "direct:start")
    protected ProducerTemplate template;

    protected Exchange createExchangeWithInBody(String body) {
        DefaultExchange exchange = new DefaultExchange(context);
        if (body != null) {
            exchange.getIn().setBody(body);
        }
        return exchange;
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
