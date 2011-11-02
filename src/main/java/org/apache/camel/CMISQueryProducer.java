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
import org.apache.chemistry.opencmis.client.api.ItemIterable;
import org.apache.chemistry.opencmis.client.api.QueryResult;
import org.apache.chemistry.opencmis.commons.data.PropertyData;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The CMIS Query producer.
 */
public class CMISQueryProducer extends DefaultProducer {
    private static final transient Log LOG = LogFactory.getLog(CMISQueryProducer.class);
    private final CMISSessionFacade cmisSessionFacade;

    public CMISQueryProducer(CMISEndpoint endpoint, CMISSessionFacade cmisSessionFacade) {
        super(endpoint);
        this.cmisSessionFacade = cmisSessionFacade;
    }

    public void process(Exchange exchange) throws Exception {
        List<Map<String, Object>> nodes = executeQuery(exchange);
        exchange.getOut().setBody(nodes);
        exchange.getOut().setHeader(CamelCMISParams.CAMEL_CMIS_RESULT_COUNT, nodes.size());
    }

    private List<Map<String, Object>> executeQuery(Exchange exchange) throws Exception {
        String query = ExchangeHelper.getMandatoryInBody(exchange, String.class);
        boolean retrieveContent = exchange.getIn().getHeader(CamelCMISParams.CAMEL_CMIS_RETRIEVE_CONTENT, false, Boolean.class);
        int readSize = exchange.getIn().getHeader(CamelCMISParams.CAMEL_CMIS_READ_SIZE, 0, Integer.class);

        ItemIterable<QueryResult> itemIterable = cmisSessionFacade.executeQuery(query);
        List<Map<String, Object>> nodes = retriveResult(retrieveContent, readSize, itemIterable);
        return nodes;
    }

    private List<Map<String, Object>> retriveResult(boolean retrieveContent, int readSize, ItemIterable<QueryResult> itemIterable) {
        List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
        int count = 0;
        int pageNumber = 0;
        boolean finished = false;
        while (!finished) {
            ItemIterable<QueryResult> currentPage = itemIterable.skipTo(count).getPage();
            LOG.debug("Processing page " + pageNumber);
            for (QueryResult item : currentPage) {
                Map<String, Object> properties = propertiesToMap(item.getProperties());
                if (retrieveContent) {
                    InputStream inputStream = this.cmisSessionFacade.getContentStreamFor(item);
                    properties.put(CamelCMISParams.CAMEL_CMIS_CONTENT_STREAM, inputStream);
                }

                result.add(properties);
                count++;
                if (count == readSize) {
                    finished = true;
                    break;
                }
            }
            pageNumber++;
            if (!currentPage.getHasMoreItems()) {
                finished = true;
            }
        }
        return result;
    }

    private Map<String, Object> propertiesToMap(List<PropertyData<?>> properties) {
        Map result = new HashMap<String, Object>();
        for (PropertyData propertyData : properties) {
            result.put(propertyData.getId(), propertyData.getFirstValue());
        }
        return result;
    }
}
