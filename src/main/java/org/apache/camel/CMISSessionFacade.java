package org.apache.camel;

import org.apache.chemistry.opencmis.client.api.*;
import org.apache.chemistry.opencmis.client.runtime.ObjectIdImpl;
import org.apache.chemistry.opencmis.client.runtime.OperationContextImpl;
import org.apache.chemistry.opencmis.commons.PropertyIds;
import org.apache.chemistry.opencmis.commons.data.ContentStream;
import org.apache.chemistry.opencmis.commons.exceptions.CmisObjectNotFoundException;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class CMISSessionFacade {
    private static final int MAX_ITEMS_PER_PAGE = 10;
    private final Session session;

    public CMISSessionFacade(Session session) {
        this.session = session;
    }

    public ItemIterable<QueryResult> executeQuery(String query) {
        OperationContext operationContext = new OperationContextImpl();
        operationContext.setMaxItemsPerPage(MAX_ITEMS_PER_PAGE);
        return session.query(query, false, operationContext);
    }

    public Document getDocument(QueryResult queryResult) {
        if (CamelCMISParams.CMIS_DOCUMENT.equals(queryResult.getPropertyValueById(PropertyIds.OBJECT_TYPE_ID))) {
            String objectId = (String) queryResult.getPropertyById(PropertyIds.OBJECT_ID).getFirstValue();
            ObjectIdImpl objectIdImpl = new ObjectIdImpl(objectId);
            return (org.apache.chemistry.opencmis.client.api.Document) session.getObject(objectIdImpl);
        }
        return null;
    }

    public InputStream getContentStreamFor(QueryResult item) {
        Document document = getDocument(item);
        if (document != null && document.getContentStream() != null) {
            return document.getContentStream().getStream();
        }
        return null;
    }

    public Folder getFolderFromPathOrRoot(Exchange exchange, String path) {
        try {
            return (Folder) session.getObjectByPath(path);
        } catch (CmisObjectNotFoundException e) {
            throw new RuntimeExchangeException("Path not found " + path, exchange, e);
        }
    }

    public ContentStream createContentStream(String fileName, byte[] buf, String mimeType) throws Exception {
        if (buf != null) {
            return session.getObjectFactory().createContentStream(fileName, buf.length, mimeType, new ByteArrayInputStream(buf));
        }
        return null;
    }
}
