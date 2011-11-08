package org.apache.camel;

import org.apache.chemistry.opencmis.client.api.*;
import org.apache.chemistry.opencmis.client.runtime.ObjectIdImpl;
import org.apache.chemistry.opencmis.client.runtime.OperationContextImpl;
import org.apache.chemistry.opencmis.client.runtime.SessionFactoryImpl;
import org.apache.chemistry.opencmis.commons.PropertyIds;
import org.apache.chemistry.opencmis.commons.SessionParameter;
import org.apache.chemistry.opencmis.commons.data.ContentStream;
import org.apache.chemistry.opencmis.commons.enums.BindingType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class CMISSessionFacade {
    private static final transient Log LOG = LogFactory.getLog(CMISSessionFacade.class);
    private final String url;
    private int pageSize = 100;
    private int readCount;
    private boolean readContent;
    private String username;
    private String password;
    private String repositoryId;
    private String query;
    private Session session;

    public CMISSessionFacade(String url) {
        this.url = url;
    }

    void initSession() {
        Map<String, String> parameter = new HashMap<String, String>();
        parameter.put(SessionParameter.BINDING_TYPE, BindingType.ATOMPUB.value());
        parameter.put(SessionParameter.ATOMPUB_URL, this.url);
        parameter.put(SessionParameter.USER, this.username);
        parameter.put(SessionParameter.PASSWORD, this.password);
        if (this.repositoryId != null) {
            parameter.put(SessionParameter.REPOSITORY_ID, this.repositoryId);
            this.session = SessionFactoryImpl.newInstance().createSession(parameter);
        } else {
            this.session = SessionFactoryImpl.newInstance().getRepositories(parameter).get(0).createSession();
        }


    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setRepositoryId(String repositoryId) {
        this.repositoryId = repositoryId;
    }

    public void setReadContent(boolean readContent) {
        this.readContent = readContent;
    }

    public void setReadCount(int readCount) {
        this.readCount = readCount;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public int poll(CMISConsumer cmisConsumer) throws Exception {
        if (query != null) {
            return pollWithQuery(cmisConsumer);
        }
        return pollTree(cmisConsumer);
    }

    private int pollTree(CMISConsumer cmisConsumer) throws Exception {
        Folder rootFolder = session.getRootFolder();
        return processFolderRecursively(rootFolder, 0, cmisConsumer);
    }

    private int processFolderRecursively(Folder folder, int totalCount, CMISConsumer cmisConsumer) throws Exception {
        totalCount += cmisConsumer.sendExchangeWithPropsAndBody(CMISHelper.objectProperties(folder), null);

        OperationContext operationContext = new OperationContextImpl();
        operationContext.setMaxItemsPerPage(pageSize);

        int count = 0;
        int pageNumber = 0;
        boolean finished = false;
        ItemIterable<CmisObject> itemIterable = folder.getChildren(operationContext);
        while (!finished) {
            ItemIterable<CmisObject> currentPage = itemIterable.skipTo(count).getPage();
            LOG.debug("Processing page " + pageNumber);
            for (CmisObject child : currentPage) {
                if (isFolder(child)) {
                    Folder childFolder = (Folder) child;
                    totalCount = processFolderRecursively(childFolder, totalCount, cmisConsumer);
                } else {
                    totalCount += processNonFolderNode(child, folder, cmisConsumer);
                }

                count++;
                if (totalCount == readCount) {
                    finished = true;
                    break;
                }
            }
            pageNumber++;
            if (!currentPage.getHasMoreItems()) {
                finished = true;
            }
        }

        return totalCount;
    }

    private boolean isFolder(CmisObject cmisObject) {
        return CamelCMISConstants.CMIS_FOLDER.equals(getObjectTypeId(cmisObject));
    }

    private boolean isDocument(CmisObject cmisObject) {
        return CamelCMISConstants.CMIS_DOCUMENT.equals(getObjectTypeId(cmisObject));
    }

    private Object getObjectTypeId(CmisObject child) {
        return child.getPropertyValue(PropertyIds.OBJECT_TYPE_ID);//BASE_TYPE_ID?
    }

    private int processNonFolderNode(CmisObject cmisObject, Folder parentFolder, CMISConsumer cmisConsumer) throws Exception {
        InputStream inputStream = null;
        Map<String, Object> properties = CMISHelper.objectProperties(cmisObject);
        properties.put("CamelCMISParentFolderPath", parentFolder.getPath());
        if (isDocument(cmisObject) && readContent) {
            ContentStream contentStream = ((Document) cmisObject).getContentStream();
            if (contentStream != null) {
                inputStream = contentStream.getStream();
            }
        }
        return cmisConsumer.sendExchangeWithPropsAndBody(properties, inputStream);
    }

    private int pollWithQuery(CMISConsumer cmisConsumer) throws Exception {
        int sendCount = 0;
        int readCount = 100;
        int count = 0;
        int pageNumber = 0;
        boolean finished = false;
        ItemIterable<QueryResult> itemIterable = executeQuery(query);
        while (!finished) {
            ItemIterable<QueryResult> currentPage = itemIterable.skipTo(count).getPage();
            LOG.debug("Processing page " + pageNumber);
            for (QueryResult item : currentPage) {
                Map<String, Object> properties = CMISHelper.propertyDataToMap(item.getProperties());
                Object objectTypeId = item.getPropertyValueById(PropertyIds.OBJECT_TYPE_ID);
                InputStream inputStream = null;
                if (readContent && CamelCMISConstants.CMIS_DOCUMENT.equals(objectTypeId)) {
                    inputStream = getContentStreamFor(item);
                }

                System.out.println("processinbg " + count + "   " + properties);
                sendCount += cmisConsumer.sendExchangeWithPropsAndBody(properties, inputStream);
                count++;
                if (count == readCount) {
                    finished = true;
                    break;
                }
            }
            pageNumber++;
            if (!currentPage.getHasMoreItems()) {
                finished = true;
            }
        }
        return sendCount;
    }

    public ItemIterable<QueryResult> executeQuery(String query) {
        OperationContext operationContext = new OperationContextImpl();
        operationContext.setMaxItemsPerPage(pageSize);
        return session.query(query, false, operationContext);
    }

    public Document getDocument(QueryResult queryResult) {
        if (CamelCMISConstants.CMIS_DOCUMENT.equals(queryResult.getPropertyValueById(PropertyIds.OBJECT_TYPE_ID))) {
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

    public CmisObject getObjectByPath(String path) {
        return session.getObjectByPath(path);
    }

    public boolean isObjectTypeVersionable(String objectType) {
        return ((DocumentType)session.getTypeDefinition(objectType)).isVersionable();
    }

    public ContentStream createContentStream(String fileName, byte[] buf, String mimeType) throws Exception {
        if (buf != null) {
            return session.getObjectFactory().createContentStream(fileName, buf.length, mimeType, new ByteArrayInputStream(buf));
        }
        return null;
    }
}
