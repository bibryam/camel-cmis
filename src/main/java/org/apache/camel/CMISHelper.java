package org.apache.camel;

import org.apache.chemistry.opencmis.client.api.*;
import org.apache.chemistry.opencmis.client.api.Property;
import org.apache.chemistry.opencmis.commons.PropertyIds;
import org.apache.chemistry.opencmis.commons.data.PropertyData;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class CMISHelper {
    private CMISHelper() {
    }

    public static Map<String, Object> filterCMISProperties(Map<String, Object> properties) {
        Map<String, Object> result = new HashMap<String, Object>(properties.size());
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            if (entry.getKey().startsWith("cmis:")) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    public static Map<String, Object> objectProperties(CmisObject cmisObject) {
        List<Property<?>> propertyList = cmisObject.getProperties();
        return propertyDataToMap(propertyList);
    }

    public static Map<String, Object> propertyDataToMap(List<? extends PropertyData<?>> properties) {
        Map<String, Object> result = new HashMap<String, Object>();
        for (PropertyData propertyData : properties) {
            result.put(propertyData.getId(), propertyData.getFirstValue());
        }
        return result;
    }

    public static boolean isFolder(CmisObject cmisObject) {
        return CamelCMISConstants.CMIS_FOLDER.equals(getObjectTypeId(cmisObject));
    }

    public static boolean isDocument(CmisObject cmisObject) {
        return CamelCMISConstants.CMIS_DOCUMENT.equals(getObjectTypeId(cmisObject));
    }

    public static Object getObjectTypeId(CmisObject child) {
        return child.getPropertyValue(PropertyIds.OBJECT_TYPE_ID);//BASE_TYPE_ID?
    }

}