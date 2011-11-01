Camel CMIS Component Project
====================
This Project is a CMIS Camel component.


from("direct:a")
    //.setProperty(JcrConstants.QUERY, constant("SELECT * FROM cmis:document WHERE cmis:name LIKE 'test%'"))
    //.setProperty("my.contents.property", body())
    .to("cmis://http://cmis.alfresco.com/cmisatom?username=admin&password=admin&repositoryId=371554cd-ac06-40ba-98b8-e6b60275cca7");



    producer:

 - insert node, reading properties -- jcr
 -



Working with CMIS Queries: SELECT * FROM cmis:document WHERE cmis:name LIKE 'test%'

TODO: update delete existing nodes.

Allows creation of documents and folder in CMIS repository.

Creates a CMIS document from message body and properties from headers. For document with content, mimetype or content type is required by CMIS specification.

Creates folder if there is no message body or folder type is specified.
Name header is required for both document and folder creation.
Optional path header can specify in which location at create the new node. If not specified, the new node is created under the root folder.