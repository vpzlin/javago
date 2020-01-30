package org.vpzlin.javago.utils.elastic.elasticsearch;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.vpzlin.javago.utils.Result;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class DocumentUtil {
    /**
     * default parameters
     */
    // the connection timeout minutes of ElasticSearch master node
    private int timeoutMinutesMasterNode = 1;
    // the connection timeout minutes of ElasticSearch all nodes
    private int timeoutMinutesAllNodes  = 2;
    // the number of documents to commit add each time
    private int numberOfDocumentToCommitAddEachTime = 10000;
    // retry times when failed
    private int retryTimes = 3;

    /* the ElasticSearch connection client */
    private RestHighLevelClient client;

    /**
     * transform array to String without bracket
     * @param array array of String
     * @return String
     */
    private static String transformArrayToStringWithoutBracket(String[] array){
        if(array == null){
            return null;
        }

        return Arrays.toString(array).replace("[", "").replace("]", "").replace(" ", "");
    }

    /**
     * set the number of documents to commit add each time
     * @param num the number
     */
    public void setNumberOfDocumentToCommitAddEachTime(int num){
        if(num > 0){
            this.numberOfDocumentToCommitAddEachTime = num;
        }
    }

    /**
     * get the number of documents to commit add each time
     * @return the number
     */
    public int getNumberOfDocumentToCommitAddEachTime(){
        return this.numberOfDocumentToCommitAddEachTime;
    }

    /**
     * get retry times
     * @return the retry times, default value is [3]
     */
    public int getRetryTimes() {
        return retryTimes;
    }

    /**
     * set retry times, the default value is [3]
     * @param retryTimes
     */
    public void setRetryTimes(int retryTimes) {
        if(retryTimes >= 0){
            this.retryTimes = retryTimes;
        }
    }

    /**
     * init class witch connection to ElasticSearch server
     * @param client
     * @throws Exception
     */
    public DocumentUtil(RestHighLevelClient client) throws Exception{
        if(client == null){
            throw new Exception("Failed to init class [DocumentUtil], the input parameter [RestHighLevelClient client] can't be null.");
        }

        this.client = client;
    }

    /**
     * init class witch connection to ElasticSearch server
     * @param serversIP the servers' IP to connect
     * @param serverPort the servers' port to connect, the default value is [9200]
     * @param connectProtocol the servers' connection protocol to connect, the value supports [http] or [https], the default is [http]
     * @param connectTimeout connection timeout seconds, the default value is [1800]
     * @param socketTimeout socket timeout seconds, the default value is [7200]
     * @throws Exception
     */
    public DocumentUtil(String[] serversIP, String serverPort, String connectProtocol, int connectTimeout, int socketTimeout) throws Exception{
        Result result = ClientUtil.getClient(serversIP, serverPort, connectProtocol, connectTimeout, socketTimeout);
        if(result.isSuccess()){
            this.client = (RestHighLevelClient)result.getData();
        }
        else {
            throw new Exception(String.format("Failed to init class [DocumentUtil]. %s", result.getMessage()));
        }
    }

    /**
     * init class witch connection to ElasticSearch server
     * @param serversIP the servers' IP to connect
     * @param serverPort the servers' port to connect, the default value is [9200]
     * @throws Exception
     */
    public DocumentUtil(String[] serversIP, String serverPort) throws Exception{
        Result result = ClientUtil.getClient(serversIP, serverPort);
        if(result.isSuccess()){
            this.client = (RestHighLevelClient)result.getData();
        }
        else {
            throw new Exception(String.format("Failed to init class [DocumentUtil]. %s", result.getMessage()));
        }
    }

    /**
     * init class witch connection to ElasticSearch server
     * @param serversIP the servers' IP to connect
     * @throws Exception
     */
    public DocumentUtil(String[] serversIP) throws Exception{
        Result result = ClientUtil.getClient(serversIP);
        if(result.isSuccess()){
            this.client = (RestHighLevelClient)result.getData();
        }
        else {
            throw new Exception(String.format("Failed to init class [DocumentUtil]. %s", result.getMessage()));
        }
    }

    /**
     * init class witch connection to ElasticSearch server
     * @param serverIP the server's IP to connect
     * @param serverPort the servers' port to connect, the default value is [9200]
     * @param connectProtocol the servers' connection protocol to connect, the value supports [http] or [https], the default is [http]
     * @param connectTimeout connection timeout seconds, the default value is [1800]
     * @param socketTimeout socket timeout seconds, the default value is [7200]
     * @throws Exception
     */
    public DocumentUtil(String serverIP, String serverPort, String connectProtocol, int connectTimeout, int socketTimeout) throws Exception{
        Result result = ClientUtil.getClient(serverIP, serverPort, connectProtocol, connectTimeout, socketTimeout);
        if(result.isSuccess()){
            this.client = (RestHighLevelClient)result.getData();
        }
        else {
            throw new Exception(String.format("Failed to init class [DocumentUtil]. %s", result.getMessage()));
        }
    }

    /**
     * init class witch connection to ElasticSearch server
     * @param serverIP the server's IP to connect
     * @param serverPort the servers' port to connect, the default value is [9200]
     * @throws Exception
     */
    public DocumentUtil(String serverIP, String serverPort) throws Exception{
        Result result = ClientUtil.getClient(serverIP, serverPort);
        if(result.isSuccess()){
            this.client = (RestHighLevelClient)result.getData();
        }
        else {
            throw new Exception(String.format("Failed to init class [DocumentUtil]. %s", result.getMessage()));
        }
    }

    /**
     * init class witch connection to ElasticSearch server
     * @param serverIP the server's IP to connect
     * @throws Exception
     */
    public DocumentUtil(String serverIP) throws Exception{
        Result result = ClientUtil.getClient(serverIP);
        if(result.isSuccess()){
            this.client = (RestHighLevelClient)result.getData();
        }
        else {
            throw new Exception(String.format("Failed to init class [DocumentUtil]. %s", result.getMessage()));
        }
    }

    /**
     * set connection client
     * @param client
     */
    public void setClient(RestHighLevelClient client){
        this.client = client;
    }

    /**
     * get connection client
     * @return connection client
     */
    public RestHighLevelClient getClient(){
        return this.client;
    }

    /**
     * close ElasticSearch connection client
     * @param client the ElasticSearch connection client
     * @return
     */
    public Result closeClient(RestHighLevelClient client){
        return ClientUtil.closeClient(client);
    }

    /**
     * check if document ID exists in index
     * @param indexName index name
     * @param documentID document ID
     * @return the type of Result.data is [boolean]
     */
    public Result existDocument(String indexName, String documentID){
        if(indexName == null || indexName.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to check if document ID exists, the index name can't be null or empty."));
        }

        if(documentID == null || documentID.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to check if document ID exists in index [%s], the document ID can't be null or empty.", documentID));
        }

        /**
         * do check
         */
        GetRequest getRequest = new GetRequest(indexName, documentID);
        // do not get fields data, just get document ID
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");
        try {
            boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
            if(exists == true){
                return Result.getResult(true, true, String.format("Document ID [%s] exists in index [%s].", documentID, indexName));
            }
            else {
                return Result.getResult(true, false, String.format("Document ID [%s] doesn't exist in index [%s].", documentID, indexName));
            }
        }
        catch (Exception e){
            return Result.getResult(false, null, String.format("Failed to check document ID [%s] exists in index [%s], more info = [s].", documentID, indexName, e.getMessage()));
        }
    }

    /**
     * add document to index
     * @param indexName index name
     * @param data map data, the key is field name, the value is field value
     * @return
     */
    public Result addDocument(String indexName, Map<String, Object> data){
        if(indexName == null || indexName.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to add document to index, the index name can't be null or empty."));
        }

        if(data == null || data.size() == 0){
            return Result.getResult(false, null, String.format("Failed to add document to index [%s], the data map of field name and field value can't be null or empty.", indexName));
        }

        /* add document */
        IndexRequest request = new IndexRequest(indexName).source(data);
        request.timeout(TimeValue.timeValueMinutes(this.timeoutMinutesAllNodes));
        try{
            this.client.index(request, RequestOptions.DEFAULT);
            return Result.getResult(true, null, String.format("Added document to index [%s].", indexName));
        }
        catch (Exception e){
            return Result.getResult(false, null, String.format("Failed to add document to index [%s], more info = [%s].", indexName, e.getMessage()));
        }
    }

    /**
     * add document to index
     * @param indexName index name
     * @param documentID document ID
     * @param data map data, the key is field name, the value is field value
     * @return
     */
    public Result addDocument(String indexName, String documentID, Map<String, Object> data){
        if(indexName == null || indexName.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to add document to index, the index name can't be null or empty."));
        }

        if(documentID == null || documentID.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to add document to index [%s], the document ID can't be null or empty.", indexName));
        }

        if(data == null || data.size() == 0){
            return Result.getResult(false, null, String.format("Failed to add document to index [%s], the data map of field name and field value can't be null or empty.", indexName));
        }

        /* add document */
        IndexRequest request = new IndexRequest(indexName).id(documentID).source(data);
        request.timeout(TimeValue.timeValueMinutes(this.timeoutMinutesAllNodes));
        try{
            this.client.index(request, RequestOptions.DEFAULT);
            return Result.getResult(true, null, String.format("Added document with document ID [%s] to index [%s].", documentID, indexName));
        }
        catch (Exception e){
            return Result.getResult(false, null, String.format("Failed to add document with document ID [%s] to index [%s], more info = [%s].", documentID, indexName, e.getMessage()));
        }
    }

    /**
     * add documents
     * @param indexName index name
     * @param data the data map of documents </br>
     *             the key of data map is document ID, the value of data map is a map of fields' names and fields' value.
     * @return
     */
    public Result addDocuments(String indexName, Map<String, Object> data){
        if(indexName == null || indexName.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to add documents to index, the index name can't be null or empty."));
        }

        if(data == null || data.size() == 0){
            return Result.getResult(false, null, String.format("Failed to add documents to index [%s], the data map of documents can't be null or empty, the key of data map is document ID, the value of data map is a map of fields' names and fields' value.", indexName));
        }

        /* add documents */
        int recordsCounter = 0;
        BulkRequest bulkRequest = new BulkRequest();
        for(Map.Entry<String, Object> entry: data.entrySet()){
            bulkRequest.add(new IndexRequest(indexName).id(entry.getKey()).source((Map<String, Object>)entry.getValue()));
            recordsCounter++;
            if (recordsCounter % numberOfDocumentToCommitAddEachTime == 0 || recordsCounter == data.size()) {
                try {
                    this.client.bulk(bulkRequest, RequestOptions.DEFAULT);
                }
                catch (Exception e){
                    return Result.getResult(false, null, String.format("Failed to add documents to index [%s], more info = [%s]", indexName, e.getMessage()));
                }
                bulkRequest = new BulkRequest();
            }
        }

        return Result.getResult(true, null, String.format("Add [%s] documents to index [%s].", recordsCounter, indexName));
    }

    /**
     * add documents
     * @param indexName index name
     * @param data the data list of documents, each list node is a map object. </br>
     *             the key of map is field name, the value of map is field value.
     * @return
     */
    public Result addDocuments(String indexName, List<Map<String, Object>> data){
        if(indexName == null || indexName.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to add documents to index, the index name can't be null or empty."));
        }

        if(data == null || data.size() == 0){
            return Result.getResult(false, null, String.format("Failed to add documents to index [%s], the data list of documents can't be null or empty..", indexName));
        }

        int recordsCounter = 0;
        BulkRequest bulkRequest = new BulkRequest();
        for(Map<String, Object> document: data){
            bulkRequest.add(new IndexRequest(indexName).source(document));
            // 每隔numberOfInsertDocuments条数据插入一次
            recordsCounter++;
            if (recordsCounter % numberOfDocumentToCommitAddEachTime == 0 || recordsCounter == data.size()) {
                try {
                    this.client.bulk(bulkRequest, RequestOptions.DEFAULT);
                } catch (Exception e) {
                    return Result.getResult(false, null, String.format("Failed to add documents to index [%s], more info = [%s]", indexName, e.getMessage()));
                }
                bulkRequest = new BulkRequest();
            }
        }

        return Result.getResult(true, null, String.format("Add [%s] documents to index [%s].", recordsCounter, indexName));
    }

    /**
     * update document of index
     * @param indexName index name
     * @param documentID document ID
     * @param data the data map of fields' names and value. </br>
     *             the key is fields' name, the value is fields' value.
     * @return
     */
    public Result updateDocument(String indexName, String documentID, Map<String, Object> data){
        if(indexName == null || indexName.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to update document of index, the index name can't be null or empty."));
        }

        if(documentID == null || documentID.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to update document of index [%s], the document ID can't be null or empty.", indexName));
        }

        if(data == null || data.size() == 0){
            return Result.getResult(false, null, String.format("Failed to update document ID [%s] of index [%s], the document ID can't be null or empty.", documentID, indexName));
        }

        Result existResult = existDocument(indexName, documentID);
        if(existResult.isSuccess() == false){
            return Result.getResult(false, null, String.format("Failed to update document ID [%s] of index [%s]. %s", documentID, indexName, existResult.getMessage()));
        }
        if((boolean)existResult.getData() == false){
            return Result.getResult(false, null, String.format("Failed to update document ID [%s] of index [%s], the document ID doesn't exist.", documentID, indexName));
        }

        /**
         * update document
         */
        UpdateRequest request = new UpdateRequest(indexName, documentID).doc(data);
        request.timeout(TimeValue.timeValueMinutes(this.timeoutMinutesAllNodes));
        // set retry time when failed
        request.retryOnConflict(this.retryTimes);
        try{
            this.client.update(request, RequestOptions.DEFAULT);
            return Result.getResult(true, null, String.format("Updated document ID [%s] of index [%s].", documentID, indexName));
        }
        catch (Exception e){
            return Result.getResult(false, null, String.format("Failed to update document ID [%s] of index [%s].", documentID, indexName));
        }
    }

    /**
     * delete document
     * @param indexName index name
     * @param documentID document ID
     * @return
     */
    public Result deleteDocument(String indexName, String documentID){
        if(indexName == null || indexName.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to delete document of index, the index name can't be null or empty."));
        }

        if(documentID == null || documentID.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to delete document of index [%s], the document ID can't be null or empty.", indexName));
        }

        Result existResult = existDocument(indexName, documentID);
        if(existResult.isSuccess() == false){
            return Result.getResult(false, null, String.format("Failed to delete document ID [%s] of index [%s]. %s", documentID, indexName, existResult.getMessage()));
        }
        if((boolean)existResult.getData() == false){
            return Result.getResult(false, null, String.format("Failed to delete document ID [%s] of index [%s], the document ID doesn't exist.", documentID, indexName));
        }

        /**
         * delete document
         */
        DeleteRequest request = new DeleteRequest(indexName, documentID);
        request.timeout(TimeValue.timeValueMinutes(this.timeoutMinutesAllNodes));
        try {
            this.client.delete(request, RequestOptions.DEFAULT);
            return Result.getResult(true, null, String.format("Deleted document ID [%s] of index [%s].", documentID, indexName));
        }
        catch (Exception e){
            return Result.getResult(false, null, String.format("Failed to delete document ID [%s] of index [%s].", documentID, indexName));
        }
    }

    /**
     *
     * @param indicesNames indices' names
     * @param fieldName field name to query
     * @param fieldValue field value to query
     * @return the type of Result.data is [long], it means the number of documents deleted
     */
    public Result deleteDocument(String[] indicesNames, String fieldName, String fieldValue){
        if(indicesNames == null || indicesNames.length == 0){
            return Result.getResult(false, null, String.format("Failed to delete documents of indices, the indices' names can't be null or empty."));
        }

        if(fieldName == null || fieldName.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to delete documents of indices [%s], the field name to query can't be null or empty.", transformArrayToStringWithoutBracket(indicesNames)));
        }

        if(fieldValue == null || fieldValue.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to delete documents of indices [%s], the field value to query can't be null or empty.", transformArrayToStringWithoutBracket(indicesNames)));
        }

        /**
         * begin to delete
         */
        DeleteByQueryRequest request = new DeleteByQueryRequest(indicesNames);
        request.setTimeout(TimeValue.timeValueMinutes(this.timeoutMinutesAllNodes));
        request.setQuery(new TermQueryBuilder(fieldName, fieldValue));
        // refresh indices to after finish deleting documents
        request.setRefresh(true);
        try{
            BulkByScrollResponse bulkByScrollResponse = this.client.deleteByQuery(request, RequestOptions.DEFAULT);
            long deletedDocs = bulkByScrollResponse.getDeleted();
            return Result.getResult(true, deletedDocs, String.format("Finished deleting [%s] documents, the field name queried = [%s], the field value queried = [%s].", deletedDocs, fieldName, fieldValue));
        }
        catch (Exception e){
            return Result.getResult(false, null, String.format("Failed to delete documents, the field name queried = [%s], the field value queried = [%s]， more info = [%s].", fieldName, fieldValue, e.getMessage()));
        }
    }

    /**
     *
     * @param indexName index name
     * @param fieldName field name to query
     * @param fieldValue field value to query
     * @return the type of Result.data is [long], it means the number of documents deleted
     */
    public Result deleteDocument(String indexName, String fieldName, String fieldValue){
        if(indexName == null || indexName.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to delete documents of index, the index name can't be null or empty."));
        }

        String[] indicesNames = {indexName};
        return deleteDocument(indicesNames, fieldName, fieldValue);
    }

    /**
     * get document
     * @param indexName index name
     * @param documentID document ID
     * @param fieldsNames fields' names assigned to returned, if not assigns(set this parameter to null), it will return all fields
     * @return the type of Result.data is [Map<String, Object>]
     */
    public Result getDocument(String indexName, String documentID, String[] fieldsNames){
        if(indexName == null || indexName.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to delete document of index, the index name can't be null or empty."));
        }

        if(documentID == null || documentID.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to delete document of index [%s], the document ID can't be null or empty.", indexName));
        }

        Result existResult = existDocument(indexName, documentID);
        if(existResult.isSuccess() == false){
            return Result.getResult(false, null, String.format("Failed to delete document ID [%s] of index [%s]. %s", documentID, indexName, existResult.getMessage()));
        }
        if((boolean)existResult.getData() == false){
            return Result.getResult(false, null, String.format("Failed to delete document ID [%s] of index [%s], the document ID doesn't exist.", documentID, indexName));
        }

        /**
         * get document
         */
        GetRequest request = new GetRequest(indexName, documentID);
        // assign some fields to get, if doesn't assign, it will return all fields
        if(fieldsNames != null && fieldsNames.length > 0){
            request.fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE);
            FetchSourceContext fetchSourceContext = new FetchSourceContext(true, fieldsNames, null);
            request.fetchSourceContext(fetchSourceContext);
        }
        try {
            GetResponse getResponse = this.client.get(request, RequestOptions.DEFAULT);
            Map<String, Object> fieldsValueMap = getResponse.getSourceAsMap();
            return Result.getResult(true, fieldsValueMap, String.format("Got document data with document ID [%s] of index [%s].", documentID, indexName));
        }
        catch (Exception e){
            return Result.getResult(true, null, String.format("Failed to get document data with document ID [%s] of index [%s], more info = [%s].", documentID, indexName, e.getMessage()));
        }
    }

    /**
     * get document
     * @param indexName index name
     * @param documentID document ID
     * @return the type of Result.data is [Map<String, Object>]
     */
    public Result getDocument(String indexName, String documentID){
        return getDocument(indexName, documentID, null);
    }

    /**
     * reindex
     * @param fromIndices indices from
     * @param toIndex index to
     * @return the type of Result.data is [long], it means total documents number
     */
    public Result reindex(String[] fromIndices, String toIndex){
        if(fromIndices == null || fromIndices.length == 0){
            return Result.getResult(false, null, String.format("Failed to reindex, indices from can't be null or empty."));
        }
        if(toIndex == null || toIndex.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to reindex, index to can't be null or empty."));
        }

        /**
         * begin to reindex
         */
        ReindexRequest request = new ReindexRequest();
        request.setTimeout(TimeValue.timeValueMinutes(this.timeoutMinutesAllNodes));
        request.setSourceIndices(fromIndices);
        request.setDestIndex(toIndex);
        // refresh index to after finish reindexing
        request.setRefresh(true);
        try{
            BulkByScrollResponse bulkByScrollResponse = this.client.reindex(request, RequestOptions.DEFAULT);
            long totalDocs = bulkByScrollResponse.getTotal();
            long updatedDocs = bulkByScrollResponse.getUpdated();
            long createdDocs = bulkByScrollResponse.getCreated();
            long deletedDocs = bulkByScrollResponse.getDeleted();
            String info = String.format("Total documents = [%s], updated documents = [], created documents = [%s], deleted documents = [%s].", totalDocs, updatedDocs, createdDocs, deletedDocs);
            return Result.getResult(true, totalDocs, String.format("Finished reindexing [%s] to index [%s].", transformArrayToStringWithoutBracket(fromIndices), toIndex));
        }
        catch (Exception e){
            return Result.getResult(false, null, String.format("Failed to reindex [%s] to index [%s].", transformArrayToStringWithoutBracket(fromIndices), toIndex));
        }
    }

    /**
     * reindex
     * @param fromIndex index from
     * @param toIndex index to
     * @return the type of Result.data is [long], it means total documents number
     */
    public Result reindex(String fromIndex, String toIndex){
        String[] fromIndices = {fromIndex};
        return reindex(fromIndices, toIndex);
    }
}
