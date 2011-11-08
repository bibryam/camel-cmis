package org.apache.camel;

import org.apache.chemistry.opencmis.client.api.*;
import org.apache.chemistry.opencmis.client.runtime.OperationContextImpl;
import org.apache.chemistry.opencmis.commons.data.ContentStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.InputStream;
import java.util.Map;

public class RecursiveTreeWalker {
    private static final transient Log LOG = LogFactory.getLog(RecursiveTreeWalker.class);

    private final CMISConsumer cmisConsumer;
    private final boolean readContent;
    private final int readCount;
    private final int pageSize;
    private int totalPolled;

    public RecursiveTreeWalker(CMISConsumer cmisConsumer, boolean readContent, int readCount, int pageSize) {
        this.cmisConsumer = cmisConsumer;
        this.readContent = readContent;
        this.readCount = readCount;
        this.pageSize = pageSize;
    }

    int processFolderRecursively(Folder folder) throws Exception {
        processFolderNode(folder);

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
                if (CMISHelper.isFolder(child)) {
                    Folder childFolder = (Folder) child;
                    processFolderRecursively(childFolder);
                } else {
                    processNonFolderNode(child, folder);
                }

                count++;
                if (totalPolled == readCount) {
                    finished = true;
                    break;
                }
            }
            pageNumber++;
            if (!currentPage.getHasMoreItems()) {
                finished = true;
            }
        }

        return totalPolled;
    }

    private void processNonFolderNode(CmisObject cmisObject, Folder parentFolder) throws Exception {
        InputStream inputStream = null;
        Map<String, Object> properties = CMISHelper.objectProperties(cmisObject);
        properties.put(CamelCMISConstants.CMIS_FOLDER_PATH, parentFolder.getPath());
        if (CMISHelper.isDocument(cmisObject) && readContent) {
            ContentStream contentStream = ((Document) cmisObject).getContentStream();
            if (contentStream != null) {
                inputStream = contentStream.getStream();
            }
        }
        sendNode(properties, inputStream);
    }

    private void processFolderNode(Folder folder) throws Exception {
        sendNode(CMISHelper.objectProperties(folder), null);
    }

    private void sendNode(Map<String, Object> properties, InputStream inputStream) throws Exception {
        totalPolled += cmisConsumer.sendExchangeWithPropsAndBody(properties, inputStream);
    }
}
