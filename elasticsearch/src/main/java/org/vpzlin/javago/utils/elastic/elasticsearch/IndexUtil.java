package org.vpzlin.javago.utils.elastic.elasticsearch;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.vpzlin.javago.utils.Result;

import java.util.*;

public class IndexUtil {
    /**
     * default parameters
     */
    // the default replicas number of index
    private int replicasNumDefault = 2;
    // the default shard number of index
    private int shardsNumDefault = 5;
    // the connection timeout minutes of ElasticSearch master node
    private int timeoutMinutesMasterNode = 1;
    // the connection timeout minutes of ElasticSearch all nodes
    private int timeoutMinutesAllNodes  = 2;

    // the ElasticSearch connection client
    private RestHighLevelClient client;

    /**
     * init class witch connection to ElasticSearch server
     * @param client
     * @throws Exception
     */
    public IndexUtil(RestHighLevelClient client) throws Exception{
        if(client == null){
            throw new Exception("Failed to init class [IndexUtil], the input parameter [RestHighLevelClient client] can't be null.");
        }
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
    public IndexUtil(String[] serversIP, String serverPort, String connectProtocol, int connectTimeout, int socketTimeout) throws Exception{
        Result result = ClientUtil.getClient(serversIP, serverPort, connectProtocol, connectTimeout, socketTimeout);
        if(result.isSuccess()){
            this.client = (RestHighLevelClient)result.getData();
        }
        else {
            throw new Exception(String.format("Failed to init class [IndexUtil]. [%s]", result.getMessage()));
        }
    }

    /**
     * init class witch connection to ElasticSearch server
     * @param serversIP the servers' IP to connect
     * @param serverPort the servers' port to connect, the default value is [9200]
     * @throws Exception
     */
    public IndexUtil(String[] serversIP, String serverPort) throws Exception{
        Result result = ClientUtil.getClient(serversIP, serverPort);
        if(result.isSuccess()){
            this.client = (RestHighLevelClient)result.getData();
        }
        else {
            throw new Exception(String.format("Failed to init class [IndexUtil]. [%s]", result.getMessage()));
        }
    }

    /**
     * init class witch connection to ElasticSearch server
     * @param serversIP the servers' IP to connect
     * @throws Exception
     */
    public IndexUtil(String[] serversIP) throws Exception{
        Result result = ClientUtil.getClient(serversIP);
        if(result.isSuccess()){
            this.client = (RestHighLevelClient)result.getData();
        }
        else {
            throw new Exception(String.format("Failed to init class [IndexUtil]. [%s]", result.getMessage()));
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
    public IndexUtil(String serverIP, String serverPort, String connectProtocol, int connectTimeout, int socketTimeout) throws Exception{
        Result result = ClientUtil.getClient(serverIP, serverPort, connectProtocol, connectTimeout, socketTimeout);
        if(result.isSuccess()){
            this.client = (RestHighLevelClient)result.getData();
        }
        else {
            throw new Exception(String.format("Failed to init class [IndexUtil]. [%s]", result.getMessage()));
        }
    }

    /**
     * init class witch connection to ElasticSearch server
     * @param serverIP the server's IP to connect
     * @param serverPort the servers' port to connect, the default value is [9200]
     * @throws Exception
     */
    public IndexUtil(String serverIP, String serverPort) throws Exception{
        Result result = ClientUtil.getClient(serverIP, serverPort);
        if(result.isSuccess()){
            this.client = (RestHighLevelClient)result.getData();
        }
        else {
            throw new Exception(String.format("Failed to init class [IndexUtil]. [%s]", result.getMessage()));
        }
    }

    /**
     * init class witch connection to ElasticSearch server
     * @param serverIP the server's IP to connect
     * @throws Exception
     */
    public IndexUtil(String serverIP) throws Exception{
        Result result = ClientUtil.getClient(serverIP);
        if(result.isSuccess()){
            this.client = (RestHighLevelClient)result.getData();
        }
        else {
            throw new Exception(String.format("Failed to init class [IndexUtil]. [%s]", result.getMessage()));
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
     * check if the index exists
     * @param indexName index name
     * @return the type of Result.data is [boolean]
     */
    public Result existIndex(String indexName){
        GetIndexRequest getRequest = new GetIndexRequest(indexName);
        try {
            boolean exists = this.client.indices().exists(getRequest, RequestOptions.DEFAULT);
            if(exists){
                return Result.getResult(true, true, String.format("ElasticSearch index [%s] exists.", indexName));
            }
            else {
                return Result.getResult(true, false, String.format("ElasticSearch index [%s] doesn't exist.", indexName));
            }
        }
        catch (Exception e){
            return Result.getResult(false, null, String.format("Failed to check if index [%s] exists, more info = [%s].", indexName, e.getMessage()));
        }
    }

    /**
     * create index
     * @param indexName index name
     * @param fieldsTypesMap the fields' mapping, the key likes [student], the value like [text]
     * @param shardsNum the shards number of index
     * @param replicasNum the replicas number of index
     * @param indexAlias the alias name of index, this parameter can be null or empty String
     * @param textFieldAnalyzer the analyzer for text field, this parameter can be null or empty String
     * @param fieldNameOfCopyTo the copy field which include some fields to be searched, this parameter can be null or empty String
     * @param disableSource set if internal field [_source] is to be disabled, the parameter's default value is [false]
     * @return
     */
    public Result createIndex(String indexName, Map<String, Object> fieldsTypesMap,
                              int shardsNum, int replicasNum,
                              String indexAlias,
                              String textFieldAnalyzer,
                              String fieldNameOfCopyTo, boolean disableSource){
        /**
         * check the parameters inputted
         */
        // check if the index exists
        Result indexExistsResult = this.existIndex(indexName);
        if(indexExistsResult.isSuccess() && ((boolean)indexExistsResult.getData()) == true){
            return Result.getResult(false, null, String.format("Failed to create index [%s], it already exists.", indexName));
        }
        // check if the fields' types is empty
        if(fieldsTypesMap == null || fieldsTypesMap.size() == 0){
        }
        // check the index shards number
        if(shardsNum < 1){
            return Result.getResult(false, null, String.format("Failed to create index [%s], the shards number [%s] should be bigger than [0].", indexName, shardsNum));
        }
        // check the index replicas number
        if(replicasNum < 0){
            return Result.getResult(false, null, String.format("Failed to create index [%s], the replicas number [%s] should be bigger than or equal to [0].", indexName, replicasNum));
        }

        /**
         * set index's property
         */
        CreateIndexRequest request = new CreateIndexRequest(indexName);
        // set index's replicas's number and shards' number
        request.settings(Settings.builder()
                .put("index.number_of_replicas", replicasNum)
                .put("index.number_of_shards", shardsNum));
        // set index alias
        if(indexAlias != null && indexAlias.trim().length() > 0){
            request.alias(new Alias(indexAlias));
        }
        // set timeout
        request.setMasterTimeout(TimeValue.timeValueMinutes(this.timeoutMinutesMasterNode));
        request.setTimeout(TimeValue.timeValueMinutes(this.timeoutMinutesAllNodes));

        /**
         * set index's mapping
         */
        Map<String, Object> esJsonMap = new HashMap<>(16);
        Map<String, Object> esFieldsMap = new HashMap<>(16);
        for(Map.Entry<String, Object> entry: fieldsTypesMap.entrySet()){
            String fieldName = entry.getKey();
            String fieldType = entry.getValue().toString();
            // set fields' types
            Map<String, Object> fieldProperty = new HashMap<>(16);
            fieldProperty.put("type", fieldType);
            // set copy field to fields
            if(fieldNameOfCopyTo != null && fieldNameOfCopyTo.trim().length() > 0){
                fieldProperty.put("copy_to", fieldNameOfCopyTo);
            }
            // set analyzer to text fields
            if(fieldType.toLowerCase() == "text" && textFieldAnalyzer != null && textFieldAnalyzer.trim().length() > 0){
                fieldProperty.put("analyzer", textFieldAnalyzer);
            }
            esFieldsMap.put(fieldName, fieldProperty);
        }
        // disable index internal field "_source"
        if(disableSource == true){
            Map<String, Object> enableStatus = new HashMap<>(1);
            enableStatus.put("enabled", false);
            esJsonMap.put("_source", enableStatus);
        }
        // put fields mapping
        esJsonMap.put("properties", esFieldsMap);
        request.mapping(esJsonMap);

        /**
         * create index
          */
        try {
            this.client.indices().create(request, RequestOptions.DEFAULT);
            if(indexAlias != null && indexAlias.trim().length() > 0){
                return Result.getResult(true, null, String.format("Created index [%s], it's alias is [%s].", indexName, indexAlias));
            }
            else {
                return Result.getResult(true, null, String.format("Created index [%s].", indexName));
            }
        }
        catch (Exception e){
            e.printStackTrace();
            return Result.getResult(false, null, String.format("Failed to create index [%s], more info = [%s].", indexName, e.getMessage()));
        }
    }

    /**
     * create index
     * @param indexName index name
     * @param fieldsTypesMap the fields' mapping, the key likes [student], the value like [text]
     * @param shardsNum the shards number of index
     * @param replicasNum the replicas number of index
     * @param indexAlias the alias name of index, this parameter can be null or empty String
     * @return
     */
    public Result createIndex(String indexName, Map<String, Object> fieldsTypesMap,
                              int shardsNum, int replicasNum,
                              String indexAlias){
        return createIndex(indexName, fieldsTypesMap, shardsNum, replicasNum, indexAlias, null, null, false);
    }

    /**
     * create index
     * @param indexName index name
     * @param fieldsTypesMap the fields' mapping, the key likes [student], the value like [text]
     * @param indexAlias the alias name of index, this parameter can be null or empty String
     * @return
     */
    public Result createIndex(String indexName, Map<String, Object> fieldsTypesMap,
                              String indexAlias){
        return createIndex(indexName, fieldsTypesMap, shardsNumDefault, replicasNumDefault, indexAlias, null, null, false);
    }

    /**
     * delete index
     * @param indexName index name
     * @return
     */
    public Result deleteIndex(String indexName){
        Result indexExistResult = this.existIndex(indexName);
        if(indexExistResult.isSuccess() == false){
            return Result.getResult(false, null, String.format("Failed to delete index [%s]. %s", indexName, indexExistResult.getMessage()));
        }
        if((boolean)indexExistResult.getData() == false){
            return Result.getResult(false, null, String.format("Failed to delete index [%s], the index doesn't exists.", indexName));
        }

        DeleteIndexRequest request = new DeleteIndexRequest(indexName);
        /**
         * set timeout
         */
        request.masterNodeTimeout(TimeValue.timeValueMinutes(this.timeoutMinutesMasterNode));
        request.timeout(TimeValue.timeValueMinutes(this.timeoutMinutesAllNodes));

        // begin to delete index
        try{
            this.client.indices().delete(request, RequestOptions.DEFAULT);
            return Result.getResult(true, null, String.format("Deleted index [%s].", indexName));
        }
        catch (Exception e){
            return Result.getResult(false, null, String.format("Failed to delete index [%s], more info = [%s].", indexName, e.getMessage()));
        }
    }

    public Result addIndexAlias(String indexName, String indexAlias){
        return null;
    }

    /**
     * add fields to index
     * @param indexName index name
     * @param fieldsMapping fields mapping, the key likes [student], the value like [text]
     * @return
     */
    public Result addFields(String indexName, Map<String, String> fieldsMapping){
        /**
         * check the inputted parameters
         */
        if(indexName == null || indexName.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to add fields to index, the index name can't be null or empty String."));
        }
        Result indexExistsResult = this.existIndex(indexName);
        if(indexExistsResult.isSuccess() == false){
            return Result.getResult(false, null, String.format("Failed to add fields to index [%s]. %s.", indexName, indexExistsResult.getMessage()));
        }
        if(fieldsMapping == null || fieldsMapping.isEmpty()){
            return Result.getResult(false, null, String.format("Failed to add fields to index [%s], the fields mapping can't be null or empty.", indexName));
        }

        /**
         * add fields
         */
        PutMappingRequest request = new PutMappingRequest(indexName);
        Map<String, Object> propertiesMap = new HashMap<>(8);
        Map<String, Object> fieldsMap = new HashMap<>(8);
        for(Map.Entry<String, String> entry: fieldsMapping.entrySet()){
            Map<String, Object> fieldsTypeMap = new HashMap<>(8);
            fieldsTypeMap.put("type", entry.getValue());
            fieldsMap.put(entry.getKey(), fieldsTypeMap);
        }
        propertiesMap.put("properties", fieldsMap);
        request.source(propertiesMap);
        try {
            this.client.indices().putMapping(request, RequestOptions.DEFAULT);
            return Result.getResult(true, null, String.format("Added fields to index [%s].", indexName));
        }
        catch (Exception e){
            return Result.getResult(false, null, String.format("Failed to add fields to index [%s], more info = [%s].", indexName, e.getMessage()));
        }
    }

    /**
     * add field to index
     * @param indexName index name
     * @param fieldName field name
     * @param fieldType field type
     * @return
     */
    public Result addFields(String indexName, String fieldName, String fieldType){
        Map<String, String> fieldMap = new HashMap<>(1);
        fieldMap.put(fieldName, fieldType);
        return this.addFields(fieldName, fieldMap);
    }

    /**
     * reindex new index from source index
     * @param sourceIndexName source index name
     * @param newIndexName new index name
     * @return
     */
    public Result reindex(String sourceIndexName, String newIndexName){
        if(sourceIndexName == null || sourceIndexName.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to reindex source index [%s] to target index [%s], the source index name can't be null or empty.", sourceIndexName, newIndexName));
        }
        if(newIndexName == null || newIndexName.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to reindex source index [%s] to target index [%s], the target index name can't be null or empty.", sourceIndexName, newIndexName));
        }
        sourceIndexName = sourceIndexName.trim().toLowerCase();

        /**
         * begin to reindex
         */
        ReindexRequest request = new ReindexRequest();
        request.setSourceIndices(sourceIndexName);
        request.setDestIndex(newIndexName);
        // refresh index after finishing reindex
        request.setRefresh(true);
        try{
            this.client.reindex(request, RequestOptions.DEFAULT);
            return Result.getResult(true, null, String.format("Reindexed [%s] from index [%s].", newIndexName, sourceIndexName));
        }
        catch (Exception e){
            return Result.getResult(false, null, String.format("Failed to reindex [%s] from index [%s], more info = [%s].", newIndexName, sourceIndexName, e.getMessage()));
        }
    }

    /**
     * get fields' mapping of indices
     * @param indicesNames indices' names
     * @param fieldsNames fields' names, this parameter can be null
     * @return the type of Result.data is [Map<String, Object>]. <br/>
     *         the key is index name, the value is fields' mapping of type [Map<String, String>]. <br/>
     *         if a index has no fields, it will not exist in keys of [Map<String, Object>].
     */
    public Result getFieldsMapping(String[] indicesNames, String[] fieldsNames){
        if(indicesNames == null && indicesNames.length == 0){
            return Result.getResult(false, null, String.format("Failed to get fields mapping of index [%s], the indices' names inputted can't be null or empty.", Arrays.toString(indicesNames).replace("[", "").replace("]", "")));
        }

        GetFieldMappingsRequest request = new GetFieldMappingsRequest();
        // choose indices
        request.indices(indicesNames);
        // choose fields' names
        if(fieldsNames != null && fieldsNames.length > 0){
            request.fields(fieldsNames);
        }
        else {
            request.fields("*");
        }

        // begin to get fields' mapping
        try {
            GetFieldMappingsResponse response = this.client.indices().getFieldMapping(request, RequestOptions.DEFAULT);
            Map<String, Object> data = new LinkedHashMap<>(8);
            for(Map.Entry<String, Map<String, GetFieldMappingsResponse.FieldMappingMetaData>> indexEntry: response.mappings().entrySet()){
                String indexName = indexEntry.getKey();
                Map<String, String> fieldsNamesAndTypes = new LinkedHashMap<>(8);
                for(Map.Entry<String, GetFieldMappingsResponse.FieldMappingMetaData> fieldEntry: indexEntry.getValue().entrySet()){
                    if(fieldEntry.getValue().sourceAsMap().size() > 0) {
                        fieldsNamesAndTypes.put(fieldEntry.getKey(), ((Map<String, String>) fieldEntry.getValue().sourceAsMap().get(fieldEntry.getKey())).get("type"));
                    }
                }

                data.put(indexName, fieldsNamesAndTypes);
            }

            return Result.getResult(true, data, String.format("Got fields' mapping of index [%s].", Arrays.toString(indicesNames).replace("[", "").replace("]", "")));
        }
        catch (Exception e){
            return Result.getResult(false, null, String.format("Failed to fields' mapping of index [%s], more info = [%s].", Arrays.toString(indicesNames).replace("[", "").replace("]", ""), e.getMessage()));
        }
    }

    /**
     * get fields' mapping of indices
     * @param indicesNames indices' names
     * @return the type of Result.data is [Map<String, Object>]. <br/>
     *         the key is index name, the value is fields' mapping of type [Map<String, String>]. <br/>
     *         if a index has no fields, it will not exist in keys of [Map<String, Object>].
     */
    public Result getFieldsMapping(String[] indicesNames){
        return getFieldsMapping(indicesNames, null);
    }

    /**
     * get fields' mapping of indices
     * @param indexName index's name
     * @param fieldsNames fields' names, this parameter can be null
     * @return the type of Result.data is [Map<String, String>], the key is field name, the value is field
     */
    public Result getFieldsMapping(String indexName, String[] fieldsNames){
        String[] indicesNames = {indexName};
        Result indexFieldsMappingResult = getFieldsMapping(indicesNames, fieldsNames);
        if(indexFieldsMappingResult.isSuccess() == false){
            return indexFieldsMappingResult;
        }

        Map<String, String> fieldsMapping = (Map<String, String>) ((Map<String, Object>) indexFieldsMappingResult.getData()).get(indexName);
        return Result.getResult(true, fieldsMapping, String.format("Got fields mapping of index [%s].", indexName));
    }

    /**
     * get fields' mapping of indices
     * @param indexName index's name
     * @return the type of Result.data is [Map<String, String>], the key is field name, the value is field
     */
    public Result getFieldsMapping(String indexName){
        return getFieldsMapping(indexName, null);
    }

    /**
     * get fields' names of index
     * @param indexName index name
     * @return the type of Result.data is [List<String>]
     */
    public Result getIndexFieldsNames(String indexName){
        Result fieldsMappingResult = getFieldsMapping(indexName);
        if(fieldsMappingResult.isSuccess() == false){
            return Result.getResult(false, null, String.format("Failed to get fields' names of index [%s]. %s", indexName, fieldsMappingResult.getMessage()));
        }

        List<String> fieldsNames = new LinkedList<>();
        for(Map.Entry<String, String> fieldMapping: ((Map<String, String>)fieldsMappingResult.getData()).entrySet()){
            fieldsNames.add(fieldMapping.getKey());
        }

        return Result.getResult(true, fieldsNames, String.format("Got fields' names of index [%s].", indexName));
    }
}
