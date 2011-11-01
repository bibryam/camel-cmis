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
import org.apache.chemistry.opencmis.client.api.*;
import org.apache.chemistry.opencmis.client.runtime.ObjectIdImpl;
import org.apache.chemistry.opencmis.client.runtime.OperationContextImpl;
import org.apache.chemistry.opencmis.commons.data.PropertyData;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The CMIS producer.
 */
public class CMISQueryProducer extends DefaultProducer {
    private CMISEndpoint endpoint;

    public CMISQueryProducer(CMISEndpoint endpoint) {
        super(endpoint);
        this.endpoint = endpoint;
    }

    public void process(Exchange exchange) throws Exception {
        List<Map<String, Object>> items = executeQuery(exchange);
        exchange.getOut().setBody(items);
        exchange.getOut().setHeader("CamelCMISResultCount", items.size());
    }

    private List<Map<String, Object>> executeQuery(Exchange exchange) throws Exception {
        String body = ExchangeHelper.getMandatoryInBody(exchange, String.class);
        boolean retrieveContent = exchange.getIn().getHeader("CamelCMISRetrieveContent", false, Boolean.class);
        Integer readSize = exchange.getIn().getHeader("CamelCMISReadSize", 0, Integer.class);

        ItemIterable<QueryResult> itemIterable = query(body);
        List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();

        long count = 0;
        boolean finished = false;
        while (!finished) {
            ItemIterable<QueryResult> currentPage = itemIterable.skipTo(count).getPage();
            for (QueryResult item : currentPage) {
                Map<String, Object> properties = propertiesToMap(item.getProperties());
                if (retrieveContent) {
                    putContentStream(item, properties);
                }
                result.add(properties);
                count++;
                if (count == readSize) {
                    finished = true;
                    break;
                }
            }
            if (!currentPage.getHasMoreItems()) {
                finished = true;
            }
        }
        return result;
    }

    private void putContentStream(QueryResult item, Map<String, Object> properties) {
        Document document = getDocument(item);
        if (document.getContentStream() != null) {
            InputStream stream = document.getContentStream().getStream();
            properties.put("CamelCMISContent", stream);
        }
    }

    private Map<String, Object> propertiesToMap(List<PropertyData<?>> properties) {
        Map result = new HashMap<String, Object>();
        for (PropertyData propertyData : properties) {
            result.put(propertyData.getId(), propertyData.getFirstValue());
        }
        return result;
    }

    private ItemIterable<QueryResult> query(String query) {
        OperationContext operationContext = new OperationContextImpl();
        operationContext.setMaxItemsPerPage(10);
        Session session = this.endpoint.getSession();
        return session.query(query, false, operationContext);
    }

    private Document getDocument(QueryResult queryResult) {
        Session session = this.endpoint.getSession();
        String objectId = (String) queryResult.getPropertyById("cmis:objectId").getFirstValue();
        ObjectIdImpl objectIdImpl = new ObjectIdImpl(objectId);
        return (org.apache.chemistry.opencmis.client.api.Document) session.getObject(objectIdImpl);
    }
}
