package org.vpzlin.javago.utils.elastic.elasticsearch;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.flush.SyncedFlushRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.SyncedFlushResponse;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.cluster.metadata.AliasMetaData;
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

    /**
     * check if index aliases are existed
     * @param aliasesNames index aliases names
     * @param indicesNames index names, this parameter can be null or empty
     * @return the type of Result.data is [boolean]
     */
    public Result existAliases(String[] aliasesNames, String[] indicesNames){
        if(aliasesNames != null && aliasesNames.length == 0){
            return Result.getResult(false, null, String.format("Failed to check if index aliases names are existed, the aliases names can't be null or empty."));
        }

        GetAliasesRequest request = new GetAliasesRequest(aliasesNames);
        if(indicesNames != null){
            request.indices(indicesNames);
        }
        try{
            if(this.client.indices().existsAlias(request, RequestOptions.DEFAULT) == true){
                if(indicesNames != null && indicesNames.length > 0){
                    return Result.getResult(true, true, String.format("Index alias [%s] are existed in indices [%s].", transformArrayToStringWithoutBracket(aliasesNames), transformArrayToStringWithoutBracket(indicesNames)));
                }
                else {
                    return Result.getResult(true, true, String.format("Index alias [%s] are existed.", transformArrayToStringWithoutBracket(aliasesNames)));
                }
            }
            else {
                if(indicesNames != null && indicesNames.length > 0){
                    return Result.getResult(true, false, String.format("Index alias [%s] are not existed in indices [%s].", transformArrayToStringWithoutBracket(aliasesNames), transformArrayToStringWithoutBracket(indicesNames)));
                }
                else {
                    return Result.getResult(true, false, String.format("Index alias [%s] are not existed.", transformArrayToStringWithoutBracket(aliasesNames)));
                }
            }
        }
        catch (Exception e){
            return Result.getResult(false, null, String.format("Failed to check if index aliases names are existed, more info = [%s].", e.getMessage()));
        }
    }

    /**
     * check if index aliases are existed
     * @param aliasesNames index aliases names
     * @return the type of Result.data is [boolean]
     */
    public Result existAliases(String[] aliasesNames){
        return existAliases(aliasesNames, null);
    }

    /**
     * check if index aliases are existed
     * @param aliasName index alias names
     * @param indexName index names, this parameter can be null or empty
     * @return the type of Result.data is [boolean]
     */
    public Result existAliases(String aliasName, String indexName){
        String[] aliasesNames = {aliasName};
        String[] indicesNames = {indexName};
        return existAliases(aliasesNames, indicesNames);
    }

    /**
     * check if index aliases are existed
     * @param aliasName index alias names
     * @return the type of Result.data is [boolean]
     */
    public Result existAliases(String aliasName){
        return existAliases(aliasName, null);
    }

    /**
     * create index alias
     * @param indexName index name
     * @param indexAlias index alias
     * @return the type of Result.data is [boolean]
     */
    public Result createAlias(String indexName, String indexAlias){
        // check if index exists
        Result indexExistResult = this.existIndex(indexName);
        if(indexExistResult.isSuccess() == false){
            return Result.getResult(false,null, String.format("Failed to create alias [%s] to index [%s]. %s", indexAlias, indexName, indexExistResult.getMessage()));
        }
        if((boolean)indexExistResult.getData() == false){
            return Result.getResult(false,null, String.format("Failed to create alias [%s] to index [%s], the index doesn't exist.", indexAlias, indexName));
        }

        // check if alias exists
        Result aliasExistResult = this.existAliases(indexAlias);
        if(aliasExistResult.isSuccess() == false){
            return Result.getResult(false,null, String.format("Failed to create alias [%s] to index [%s]. %s", indexAlias, indexName, indexExistResult.getMessage()));
        }
        if((boolean)aliasExistResult.getData() == true){
            return Result.getResult(false,null, String.format("Failed to create alias [%s] to index [%s], the index alias already exist.", indexAlias, indexName));
        }

        /**
         * begin to create alias
         */
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        IndicesAliasesRequest.AliasActions aliasActions = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
                .index(indexName).alias(indexAlias);
        // set timeout
        request.masterNodeTimeout(TimeValue.timeValueMinutes(this.timeoutMinutesMasterNode));
        request.timeout(TimeValue.timeValueMinutes(this.timeoutMinutesAllNodes));
        // create alias
        request.addAliasAction(aliasActions);
        try{
            AcknowledgedResponse indicesAliasesResponse = this.client.indices().updateAliases(request, RequestOptions.DEFAULT);
            if(indicesAliasesResponse.isAcknowledged() == true){
                return Result.getResult(true, null, String.format("Created alias [%s] to index [%s].", indexAlias, indexName));
            }
            else {
                return Result.getResult(false, null, String.format("Failed to create alias [%s] to index [%s].", indexAlias, indexName));
            }
        }
        catch (Exception e){
            return Result.getResult(false, null, String.format("Failed to create alias [%s] to index [%s], more info = [%s].", indexAlias, indexName, e.getMessage()));
        }
    }

    /**
     * delete index alias
     * @param indexName index name
     * @param indexAlias index alias
     * @return the type of Result.data is [boolean]
     */
    public Result deleteAlias(String indexName, String indexAlias){
        // check if index exists
        Result indexExistResult = this.existIndex(indexName);
        if(indexExistResult.isSuccess() == false){
            return Result.getResult(false,null, String.format("Failed to delete alias [%s] to index [%s]. %s", indexAlias, indexName, indexExistResult.getMessage()));
        }
        if((boolean)indexExistResult.getData() == false){
            return Result.getResult(false,null, String.format("Failed to delete alias [%s] to index [%s], the index doesn't exist.", indexAlias, indexName));
        }

        // check if alias exists
        Result aliasExistResult = this.existAliases(indexAlias);
        if(aliasExistResult.isSuccess() == false){
            return Result.getResult(false,null, String.format("Failed to delete alias [%s] to index [%s]. %s", indexAlias, indexName, indexExistResult.getMessage()));
        }
        if((boolean)aliasExistResult.getData() == false){
            return Result.getResult(false,null, String.format("Failed to delete alias [%s] to index [%s], the index alias doesn't exist.", indexAlias, indexName));
        }

        /**
         * begin to delete alias
         */
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        IndicesAliasesRequest.AliasActions aliasActions = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.REMOVE).index(indexName).alias(indexAlias);
        // set timeout
        request.masterNodeTimeout(TimeValue.timeValueMinutes(this.timeoutMinutesMasterNode));
        request.timeout(TimeValue.timeValueMinutes(this.timeoutMinutesAllNodes));
        // set timeout
        request.masterNodeTimeout(TimeValue.timeValueMinutes(this.timeoutMinutesMasterNode));
        request.timeout(TimeValue.timeValueMinutes(this.timeoutMinutesAllNodes));
        // create alias
        request.addAliasAction(aliasActions);
        try{
            AcknowledgedResponse indicesAliasesResponse = this.client.indices().updateAliases(request, RequestOptions.DEFAULT);
            if(indicesAliasesResponse.isAcknowledged() == true){
                return Result.getResult(true, null, String.format("Deleted alias [%s] to index [%s].", indexAlias, indexName));
            }
            else {
                return Result.getResult(false, null, String.format("Failed to delete alias [%s] of index [%s].", indexAlias, indexName));
            }
        }
        catch (Exception e){
            return Result.getResult(false, null, String.format("Failed to delete alias [%s] of index [%s], more info = [%s].", indexAlias, indexName, e.getMessage()));
        }
    }

    /**
     * add fields to index
     * @param indexName index name
     * @param fieldsMapping fields mapping, the key likes [student], the value like [text] or [int]
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
    public Result addField(String indexName, String fieldName, String fieldType){
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
            return Result.getResult(false, null, String.format("Failed to get fields mapping of index [%s], the indices' names inputted can't be null or empty.", transformArrayToStringWithoutBracket(indicesNames)));
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

            return Result.getResult(true, data, String.format("Got fields' mapping of index [%s].", transformArrayToStringWithoutBracket(indicesNames)));
        }
        catch (Exception e){
            return Result.getResult(false, null, String.format("Failed to fields' mapping of index [%s], more info = [%s].", transformArrayToStringWithoutBracket(indicesNames), e.getMessage()));
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

    /**
     * clear indices cache
     * @param indicesNames indices names <br/>
     *                     if indices names is null, it will clear all indices' cache, but if do so, the performance of ES cluster may be very poor
     * @param clearQueryCache clear cache of query result
     * @param clearFieldDataCache clear cache of fields' data
     * @param clearRequestCache clear cache of request
     * @return
     */
    public Result clearIndicesCache(String[] indicesNames,
                                    boolean clearQueryCache,
                                    boolean clearFieldDataCache,
                                    boolean clearRequestCache){
        // indices names can't be empty
        // if indices names is null, it will clear all indices' cache, but if do so, the performance of ES cluster may be very poor
        if(indicesNames != null && indicesNames.length == 0){
            return Result.getResult(false, null, String.format("No indices names inputted to clear cache, it can be null but can't be empty."));
        }

        ClearIndicesCacheRequest clearIndicesCacheRequest = new ClearIndicesCacheRequest(indicesNames);
        // clear cache of query result
        clearIndicesCacheRequest.queryCache(clearQueryCache);
        // clear cache of fields' data
        clearIndicesCacheRequest.fieldDataCache(clearFieldDataCache);
        // clear cache of request
        clearIndicesCacheRequest.requestCache(clearRequestCache);

        // begin to clear cache
        try {
            ClearIndicesCacheResponse clearIndicesCacheResponse = this.client.indices().clearCache(clearIndicesCacheRequest, RequestOptions.DEFAULT);
            int totalShards = clearIndicesCacheResponse.getTotalShards();
            int successfulShards = clearIndicesCacheResponse.getSuccessfulShards();
            int failedShards = clearIndicesCacheResponse.getFailedShards();
            String shardsInfo = String.format("TotalShards=[%s], SuccessfulShards=[%s], FailedShards=[%s].", totalShards, successfulShards, failedShards);
            return Result.getResult(true, null, String.format("Cleared cache of indices [%s]. %s", transformArrayToStringWithoutBracket(indicesNames), shardsInfo));
        }
        catch (Exception e){
            return Result.getResult(false, null, String.format("Failed to clear cache of indices [%s].", transformArrayToStringWithoutBracket(indicesNames)));
        }
    }

    /**
     * clear indices cache
     * @param indicesNames indices names <br/>
     *      *              if indices names is null, it will clear all indices' cache, but if do so, the performance of ES cluster may be very poor
     * @return
     */
    public Result clearIndicesCache(String[] indicesNames){
        return clearIndicesCache(indicesNames, true, true, true);
    }

    /**
     * clear index cache
     * @param indexName indices names, this parameter can't be null or empty
     * @param clearQueryCache clear cache of query result
     * @param clearFieldDataCache clear cache of fields' data
     * @param clearRequestCache clear cache of request
     * @return
     */
    public Result clearIndexCache(String indexName,
                                  boolean clearQueryCache,
                                  boolean clearFieldDataCache,
                                  boolean clearRequestCache){
        if(indexName == null || indexName.trim().length() == 0){
            return Result.getResult(false, null, "No index name inputted to clear cache.");
        }

        String[] indicesNames = {indexName};
        return clearIndicesCache(indicesNames, clearQueryCache, clearFieldDataCache, clearRequestCache);
    }

    /**
     * clear index cache
     * @param indexName indices names, this parameter can't be null or empty
     * @return
     */
    public Result clearIndexCache(String indexName){
        return clearIndexCache(indexName, true, true, true);
    }

    /**
     * flush indices <br/>
     * do this operation will move data from memory to disk, and remove translog
     * @param indicesNames indices names, this parameter can be null that it will clear all indices
     * @return
     */
    public Result flushIndices(String[] indicesNames){
        if(indicesNames != null && indicesNames.length == 0){
            return Result.getResult(false, null, "Failed to flush indices names. Indices names can be null, but can't be empty.");
        }

        FlushRequest flushRequest = new FlushRequest(indicesNames);
        try {
            FlushResponse flushResponse = this.client.indices().flush(flushRequest, RequestOptions.DEFAULT);
            int totalShards = flushResponse.getTotalShards();
            int successfulShards = flushResponse.getSuccessfulShards();
            int failedShards = flushResponse.getFailedShards();
            String shardsInfo = String.format("TotalShards=[%s], SuccessfulShards=[%s], FailedShards=[%s].", totalShards, successfulShards, failedShards);
            String info = indicesNames == null ? "Flushed all indices." : String.format("Flushed indices [%s].", transformArrayToStringWithoutBracket(indicesNames));
            info += " " + shardsInfo;
            return Result.getResult(true, null, info);
        }
        catch (Exception e){
            String info = indicesNames == null ? "Failed to flush all indices," : String.format("Failed to flushed indices [%s],", transformArrayToStringWithoutBracket(indicesNames));
            info += String.format(" more info = [%s].", e.getMessage());
            return Result.getResult(false, null, info);
        }
    }

    /**
     * flush index <br/>
     * do this operation will move data from memory to disk, and remove translog file
     * @param indexName index name
     * @return
     */
    public Result flushIndex(String indexName){
        if(indexName == null || indexName.trim().length() == 0){
            return Result.getResult(false, null, "No index name inputted to flush, it can't be null or empty.");
        }

        String[] indicesNames = {indexName};
        return flushIndices(indicesNames);
    }

    /**
     * refresh indices <br/>
     * do this operation will move data from memory to disk, but not remove translog
     * @param indicesNames indices names, this parameter can be null that it will clear all indices
     * @return
     */
    public Result refreshIndices(String[] indicesNames){
        if(indicesNames != null && indicesNames.length == 0){
            return Result.getResult(false, null, "Failed to refresh indices names. Indices names can be null, but can't be empty.");
        }

        RefreshRequest refreshRequest = new RefreshRequest(indicesNames);
        try {
            RefreshResponse refreshResponse = this.client.indices().refresh(refreshRequest, RequestOptions.DEFAULT);
            int totalShards = refreshResponse.getTotalShards();
            int successfulShards = refreshResponse.getSuccessfulShards();
            int failedShards = refreshResponse.getFailedShards();
            String shardsInfo = String.format("TotalShards=[%s], SuccessfulShards=[%s], FailedShards=[%s].", totalShards, successfulShards, failedShards);
            String info = indicesNames == null ? "Refreshed all indices." : String.format("Refreshed indices [%s].", transformArrayToStringWithoutBracket(indicesNames));
            info += " " + shardsInfo;
            return Result.getResult(true, null, info);
        }
        catch (Exception e){
            String info = indicesNames == null ? "Failed to refresh all indices," : String.format("Failed to refresh indices [%s],", transformArrayToStringWithoutBracket(indicesNames));
            info += String.format(" more info = [%s].", e.getMessage());
            return Result.getResult(false, null, info);
        }
    }


    /**
     * refresh index <br/>
     * do this operation will move data from memory to disk, but not remove translog file
     * @param indexName index name
     * @return
     */
    public Result refreshIndex(String indexName){
        if(indexName == null || indexName.trim().length() == 0){
            return Result.getResult(false, null, "No index name inputted to refresh, it can't be null or empty.");
        }

        String[] indicesNames = {indexName};
        return flushIndices(indicesNames);
    }

    /**
     * synced flush indices <br/>
     * do this operation will move data from memory to disk, and remove translog file
     * @param indicesNames indices names, this parameter can be null that it will clear all indices
     * @return
     */
    public Result syncedFlushIndices(String[] indicesNames){
        if(indicesNames != null && indicesNames.length == 0){
            return Result.getResult(false, null, "Failed to flush indices names. Indices names can be null, but can't be empty.");
        }

        SyncedFlushRequest syncedFlushRequest = new SyncedFlushRequest(indicesNames);
        try {
            SyncedFlushResponse syncedFlushResponse = this.client.indices().flushSynced(syncedFlushRequest, RequestOptions.DEFAULT);
            int totalShards = syncedFlushResponse.totalShards();
            int successfulShards = syncedFlushResponse.successfulShards();
            int failedShards = syncedFlushResponse.failedShards();
            String shardsInfo = String.format("TotalShards=[%s], SuccessfulShards=[%s], FailedShards=[%s].", totalShards, successfulShards, failedShards);
            String info = indicesNames == null ? "Synced flushed all indices." : String.format("Synced flushed indices [%s].", transformArrayToStringWithoutBracket(indicesNames));
            info += " " + shardsInfo;
            return Result.getResult(true, null, info);
        }
        catch (Exception e){
            String info = indicesNames == null ? "Failed to synced flush all indices," : String.format("Failed to synced flushed indices [%s],", transformArrayToStringWithoutBracket(indicesNames));
            info += String.format(" more info = [%s].", e.getMessage());
            return Result.getResult(false, null, info);
        }
    }

    /**
     * synced flush index <br/>
     * do this operation will move data from memory to disk, and remove translog file
     * @param indexName index name
     * @return
     */
    public Result syncedFlushIndex(String indexName){
        if(indexName == null || indexName.trim().length() == 0){
            return Result.getResult(false, null, "No index name inputted to synced flush, it can't be null or empty.");
        }

        String[] indicesNames = {indexName};
        return syncedFlushIndices(indicesNames);
    }

    /**
     * force merge indices <br/>
     * do this operation will cost a lot of computing resource
     * @param indicesNames indices names, this parameter can be null that it will force merge all indices
     * @return
     */
    public Result forceMergeIndices(String[] indicesNames){
        // indices names can be null but can't be empty
        // if indices names is null, it will clear all indices' cache, but if do so, the performance of ES cluster may be very poor
        if(indicesNames != null && indicesNames.length == 0){
            return Result.getResult(false, null, String.format("No indices names inputted to force merge, it can be null but can't be empty."));
        }

        ForceMergeRequest forceMergeRequest = new ForceMergeRequest(indicesNames);
        // the official recommendation value is [1], it's better not to increase it because of performance
        forceMergeRequest.maxNumSegments(1);
        // flush index when merge
        forceMergeRequest.flush(true);
        // begin merge
        try{
            ForceMergeResponse forceMergeResponse = this.client.indices().forcemerge(forceMergeRequest, RequestOptions.DEFAULT);
            int totalShards = forceMergeResponse.getTotalShards();
            int successfulShards = forceMergeResponse.getTotalShards();
            int failedShards = forceMergeResponse.getTotalShards();
            String shardsInfo = String.format("TotalShards=[%s], SuccessfulShards=[%s], FailedShards=[%s].", totalShards, successfulShards, failedShards);
            String info = indicesNames == null ? "Force merged all indices." : String.format("Force merged indices [%s].", transformArrayToStringWithoutBracket(indicesNames));
            info += " " + shardsInfo;
            return Result.getResult(true, null, info);
        }
        catch (Exception e){
            String info = indicesNames == null ? "Failed to force merge all indices," : String.format("Failed to force merge indices [%s],", transformArrayToStringWithoutBracket(indicesNames));
            info += String.format(" more info = [%s].", e.getMessage());
            return Result.getResult(false, null, info);
        }
    }

    /**
     * force merge indices <br/>
     * do this operation will cost a lot of computing resource
     * @param indexName index name
     * @return
     */
    public Result forceMergeIndex(String indexName){
        if(indexName == null || indexName.trim().length() == 0){
            return Result.getResult(false, null, "Failed to force merge index, no index name or it's empty.");
        }

        String[] indicesNames = {indexName};
        return forceMergeIndices(indicesNames);
    }

    /**
     * get aliases
     * @param aliasesNames aliases names
     * @return the type of Result.data is [Map<String, Set<AliasMetaData>>]
     */
    public Result getAliases(String[] aliasesNames){
        if(aliasesNames != null && aliasesNames.length == 0){
            return Result.getResult(false, null, "Failed to get aliases, aliases names can be null but can't be empty.");
        }

        GetAliasesRequest request;
        if(aliasesNames == null){
            request = new GetAliasesRequest("_all");
        }
        else{
            request = new GetAliasesRequest(aliasesNames);
        }

        try {
            GetAliasesResponse getAliasesResponse = this.client.indices().getAlias(request, RequestOptions.DEFAULT);
            Map<String, Set<AliasMetaData>> aliases = getAliasesResponse.getAliases();
            String info = aliasesNames == null ? "Got aliases." : String.format("Got aliases [%s].", transformArrayToStringWithoutBracket(aliasesNames));
            return Result.getResult(true, aliases, info);
        }
        catch (Exception e){
            String info = aliasesNames == null ? "Failed to get aliases," : String.format("Failed to get aliases [%s],", transformArrayToStringWithoutBracket(aliasesNames));
            info += String.format(" more info = [%s].", e.getMessage());
            return Result.getResult(false, null, info);
        }
    }

    /**
     * get alias
     * @param aliasName alias name
     * @return the type of Result.data is [Map<String, Set<AliasMetaData>>]
     */
    public Result getAlias(String aliasName){
        if(aliasName == null || aliasName.trim().length() == 0){
            return Result.getResult(false, null, "Failed to get alias, alias name can't be null or empty.");
        }

        String[] aliasesNames = {aliasName};
        return getAliases(aliasesNames);
    }

    /**
     * get setting of index
     * @param indexName index name
     * @param settingName setting name                                                                                 <br/>
     *        [setting name] index.creation_date          [remark] timestamp, its length is 13                         <br/>
     *        [setting name] index.number_of_shards                                                                    <br/>
     *        [setting name] index.number_of_replicas                                                                  <br/>
     *        [setting name] index.refresh_interval       [remark] the unit of value is second                         <br/>
     *        [setting name] index.blocks.read_only       [remark] its value is [true] or [false] in type of String
     * @return the type of Result.data is [String]
     */
    public Result getIndexSetting(String indexName, String settingName){
        Result indexExistResult = existIndex(indexName);
        if(indexExistResult.isSuccess() == false){
            return Result.getResult(false, null, String.format("Failed to get setting of index [%s]. %s", indexName, indexExistResult.getMessage()));
        }
        if((boolean)indexExistResult.getData() == false){
            return Result.getResult(false, null, String.format("Failed to get setting of index [%s], the index doesn't exist.", indexName));
        }

        GetSettingsRequest request = new GetSettingsRequest().indices(indexName);
        try{
            GetSettingsResponse getSettingsResponse = this.client.indices().getSettings(request, RequestOptions.DEFAULT);
            String settingValue = getSettingsResponse.getSetting(indexName, settingName);
            return Result.getResult(true, settingValue, String.format("Got setting [%s] of index [%s], the value is [%s].", settingName, indexName, settingValue));
        }
        catch (Exception e){
            return Result.getResult(false, null, String.format("Failed to get setting [%s] of index [%s], more info = [%s].", settingName, indexName, e.getMessage()));
        }
    }

    /**
     * get creation date of index, it's timestamp
     * @param indexName index name
     * @return the type of Result.data is [String]
     */
    public Result getIndexCreationDate(String indexName){
        Result indexSettingResult = getIndexSetting(indexName, "index.creation_date");
        if(indexSettingResult.isSuccess() == false){
            return Result.getResult(false, null, String.format("Failed to get creation date of index [%s]. %s", indexName, indexSettingResult.getMessage()));
        }

        String creationDate = (String)indexSettingResult.getData();
        return Result.getResult(true, creationDate, String.format("Got creation date [%s] of index [%s].", creationDate, indexName));
    }

    /**
     * get shards number of index
     * @param indexName index name
     * @return the type of Result.data is [int]
     */
    public Result getIndexShardsNum(String indexName){
        Result indexSettingResult = getIndexSetting(indexName, "index.number_of_shards");
        if(indexSettingResult.isSuccess() == false){
            return Result.getResult(false, null, String.format("Failed to get shards number of index [%s]. %s", indexName, indexSettingResult.getMessage()));
        }

        int shardsNum = Integer.parseInt((String)indexSettingResult.getData());
        return Result.getResult(true, shardsNum, String.format("Got shards number [%s] of index [%s].", shardsNum, indexName));
    }

    /**
     * get replicas number of index
     * @param indexName index name
     * @return the type of Result.data is [int]
     */
    public Result getIndexReplicasNum(String indexName){
        Result indexSettingResult = getIndexSetting(indexName, "index.number_of_replicas");
        if(indexSettingResult.isSuccess() == false){
            return Result.getResult(false, null, String.format("Failed to get replicas number of index [%s]. %s", indexName, indexSettingResult.getMessage()));
        }

        int replicasNumber = Integer.parseInt((String)indexSettingResult.getData());
        return Result.getResult(true, replicasNumber, String.format("Got replicas number [%s] of index [%s].", replicasNumber, indexName));
    }

    /**
     * get refresh interval(seconds) of index
     * @param indexName index name
     * @return the type of Result.data is [int]
     */
    public Result getIndexRefreshInterval(String indexName){
        Result indexSettingResult = getIndexSetting(indexName, "index.refresh_interval");
        if(indexSettingResult.isSuccess() == false){
            return Result.getResult(false, null, String.format("Failed to get replicas interval of index [%s]. %s", indexName, indexSettingResult.getMessage()));
        }

        int replicasInterval = Integer.parseInt((String)indexSettingResult.getData());
        return Result.getResult(true, replicasInterval, String.format("Got replicas interval [%s] of index [%s].", replicasInterval, indexName));
    }

    /**
     * get readonly status of index
     * @param indexName index name
     * @return the type of Result.data is [boolean]
     */
    public Result getIndexReadonly(String indexName){
        Result indexSettingResult = getIndexSetting(indexName, "index.blocks.read_only");
        if(indexSettingResult.isSuccess() == false){
            return Result.getResult(false, null, String.format("Failed to get readonly status of index [%s]. %s", indexName, indexSettingResult.getMessage()));
        }

        boolean readonlyStatus = (boolean)indexSettingResult.getData();
        return Result.getResult(true, readonlyStatus, String.format("Got readonly status [%s] of index [%s].", readonlyStatus, indexName));
    }

    /**
     * open index
     * @param indexName index name
     * @return
     */
    public Result openIndex(String indexName){
        OpenIndexRequest request = new OpenIndexRequest(indexName);

        // set timeout
        request.masterNodeTimeout(TimeValue.timeValueMinutes(this.timeoutMinutesMasterNode));
        request.timeout(TimeValue.timeValueMinutes(this.timeoutMinutesAllNodes));

        try{
            this.client.indices().open(request, RequestOptions.DEFAULT);
            return Result.getResult(true, null, String.format("Opened index [%s].", indexName));
        }
        catch (Exception e){
            return Result.getResult(false, null, String.format("Failed to open index [%s], more info = [%s].", indexName, e.getMessage()));
        }
    }

    /**
     * close index
     * @param indexName index name
     * @return
     */
    public Result closeIndex(String indexName){
        CloseIndexRequest request = new CloseIndexRequest(indexName);

        // set timeout
        request.setMasterTimeout(TimeValue.timeValueMinutes(this.timeoutMinutesMasterNode));
        request.setTimeout(TimeValue.timeValueMinutes(this.timeoutMinutesAllNodes));

        try{
            this.client.indices().close(request, RequestOptions.DEFAULT);
            return Result.getResult(true, null, String.format("Closed index [%s].", indexName));
        }
        catch (Exception e){
            return Result.getResult(false, null, String.format("Failed to close index [%s], more info = [%s].", indexName, e.getMessage()));
        }
    }

    /**
     * set indices' settings
     * @param indicesNames indices names
     * @param settingsMap settings map <br/>
     *                    [key] number_of_replicas      [value type] String   [value example] 2     [remark] replicas number of index
     *                    [key] blocks.read_only        [value type] String   [value example] true  [remark] this only supports "true"
     *                    [key] index.blocks.read_only  [value type] String   [value example] false [remark] this only supports "false"
     * @return
     */
    public Result setIndicesSettings(String[] indicesNames, Map<String, String> settingsMap){
        if(indicesNames == null || indicesNames.length == 0){
            return Result.getResult(false, null, String.format("Failed to set indices settings, indices names can't be null or empty."));
        }
        if(settingsMap == null || settingsMap.size() ==0){
            return Result.getResult(false, null, String.format("Failed to set indices settings, settings' map can't be null or empty."));
        }

        UpdateSettingsRequest request = new UpdateSettingsRequest(indicesNames);
        request.settings(settingsMap);
        // set [false] to cover original setting
        request.setPreserveExisting(false);
        // set timeout
        request.masterNodeTimeout(TimeValue.timeValueMinutes(this.timeoutMinutesMasterNode));
        request.timeout(TimeValue.timeValueMinutes(this.timeoutMinutesAllNodes));

        try{
            this.client.indices().putSettings(request, RequestOptions.DEFAULT);
            return Result.getResult(true, null, String.format("Finished set settings of index [%s].", transformArrayToStringWithoutBracket(indicesNames)));
        }
        catch (Exception e){
            return Result.getResult(true, null, String.format("Failed to set settings of index [%s], more info = [%s].", transformArrayToStringWithoutBracket(indicesNames), e.getMessage()));
        }
    }

    /**
     * set index's settings
     * @param indexName index names
     * @param settingsMap settings map <br/>
     *                    [key] number_of_replicas      [value type] String   [value example] 2     [remark] replicas number of index
     *                    [key] blocks.read_only        [value type] String   [value example] true  [remark] this only supports "true"
     *                    [key] index.blocks.read_only  [value type] String   [value example] false [remark] this only supports "false"
     * @return
     */
    public Result setIndexSettings(String indexName, Map<String, String> settingsMap){
        if(indexName == null || indexName.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to set index settings, index name can't be null or empty."));
        }

        String[] indicesNames = {indexName};
        return setIndicesSettings(indicesNames, settingsMap);
    }

    /**
     * set readonly to indices
     * @param indicesNames indices names
     * @param readonly readonly status
     * @return
     */
    public Result setIndicesReadonly(String[] indicesNames, boolean readonly){
        String settingKey = readonly == true ? "blocks.read_only" : "index.blocks.read_only";
        Map<String, String> settingMap = new HashMap<>(1);
        settingMap.put(settingKey, String.valueOf(readonly));
        return setIndicesSettings(indicesNames, settingMap);
    }

    /**
     * set readonly to index
     * @param indexName index name
     * @param readonly readonly status
     * @return
     */
    public Result setIndexReadonly(String indexName, boolean readonly){
        if(indexName == null || indexName.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to set readonly [%s] to index, index name can't be null or empty.", readonly));
        }

        String[] indicesNames = {indexName};
        return setIndicesReadonly(indicesNames, readonly);
    }

    /**
     * set replicas number to indices
     * @param indicesNames indices names
     * @param replicasNum replicas number, its values can't be less than [0]
     * @return
     */
    public Result setIndicesReplicasNum(String[] indicesNames, int replicasNum){
        if(indicesNames == null || indicesNames.length == 0){
            return Result.getResult(false, null, String.format("Failed to set replicas number of indices names, indices names can't be null or empty."));
        }

        if(replicasNum < 0){
            return Result.getResult(false, null, String.format("Failed to set replicas number of indices names, it can't be less than [0]."));
        }

        Map<String, String> settingMap = new HashMap<>(1);
        settingMap.put("index.number_of_replicas", String.valueOf(replicasNum));
        return setIndicesSettings(indicesNames, settingMap);
    }

    /**
     * set replicas number to index
     * @param indexName index names
     * @param replicasNum replicas number
     * @return
     */
    public Result setIndexReplicasNum(String indexName, int replicasNum){
        if(indexName == null || indexName.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to set replicas number [%s] to index, index name can't be null or empty.", replicasNum));
        }

        String[] indicesNames = {indexName};
        return setIndicesReplicasNum(indicesNames, replicasNum);
    }

    /**
     * shrink source index to new index
     * @param sourceIndexName source index name
     * @param newIndexName new index name
     * @param newIndexShardsNum shards number of new index. <br/>
     *                          the shards number of source index must be multiple than this, and this must be bigger than [0]
     * @param newIndexAliasName index alias of new index, this parameter can be null or empty
     * @return
     */
    public Result shrinkIndex(String sourceIndexName, String newIndexName, int newIndexShardsNum, String newIndexAliasName){
        /* check if source index exists */
        Result sourceIndexExistResult = existIndex(sourceIndexName);
        if(sourceIndexExistResult.isSuccess() == false){
            return Result.getResult(false, null, String.format("Failed to shrink source index [%s] to new index [%s]. %s", sourceIndexName, newIndexName, sourceIndexExistResult.getMessage()));
        }
        if((boolean)sourceIndexExistResult.getData() == false){
            return Result.getResult(false, null, String.format("Failed to shrink source index [%s] to new index [%s], the source index doesn't exist.", sourceIndexName, newIndexName));
        }

        /* check if new index exists */
        Result newIndexExistResult = existIndex(sourceIndexName);
        if(newIndexExistResult.isSuccess() == false){
            return Result.getResult(false, null, String.format("Failed to shrink source index [%s] to new index [%s]. %s", sourceIndexName, newIndexName, sourceIndexExistResult.getMessage()));
        }
        if((boolean)newIndexExistResult.getData() == true){
            return Result.getResult(false, null, String.format("Failed to shrink source index [%s] to new index [%s], the new index already exists.", sourceIndexName, newIndexName));
        }

        /* check shards number of new index */
        if(newIndexShardsNum < 1){
            return Result.getResult(false, null, String.format("Failed to shrink source index [%s] to new index [%s], the shards number of new index can't be less than 1.", sourceIndexName, newIndexName));
        }
        Result sourceIndexshardsNumResult = getIndexShardsNum(sourceIndexName);
        if(sourceIndexshardsNumResult.isSuccess() == false){
            return Result.getResult(false, null, String.format("Failed to shrink source index [%s] to new index [%s]. %s.", sourceIndexName, newIndexName, sourceIndexshardsNumResult.getMessage()));
        }
        int sourceIndexShardsNum = (int)sourceIndexshardsNumResult.getData();
        if(sourceIndexShardsNum / newIndexShardsNum < 1 || sourceIndexShardsNum % newIndexShardsNum != 0){
            return Result.getResult(false, null, String.format("Failed to shrink source index [%s] to new index [%s], the shards number [%s] of source index must be multiple than the shards number [%s] of new index.", sourceIndexName, newIndexName, sourceIndexShardsNum, newIndexShardsNum));
        }

        /**
         * shrink index
         */
        ResizeRequest request = new ResizeRequest(newIndexName, sourceIndexName);
        /* set timeout */
        request.masterNodeTimeout(TimeValue.timeValueMinutes(this.timeoutMinutesMasterNode));
        request.timeout(TimeValue.timeValueMinutes(this.timeoutMinutesAllNodes));
        // set alias of new index
        if(newIndexAliasName != null && newIndexAliasName.trim().length() > 0){
            // 新索引的别名
            request.getTargetIndexRequest().alias(new Alias(newIndexAliasName));
        }
        // set shrink request
        request.getTargetIndexRequest().settings(Settings.builder()
                .put("index.number_of_shards", newIndexShardsNum)
                // remove configuration requirements of new index
                .putNull("index.routing.allocation.require._name"));
        try{
            this.client.indices().shrink(request, RequestOptions.DEFAULT);
            return Result.getResult(true, null, String.format("Shrank source index [%s] to new index [%s].", sourceIndexName, newIndexName));
        }
        catch (Exception e){
            return Result.getResult(false, null, String.format("Failed to shrink source index [%s] to new index [%s]. %s.", sourceIndexName, newIndexName, e.getMessage()));
        }
    }

    /**
     * shrink source index to new index
     * @param sourceIndexName source index name
     * @param newIndexName new index name
     * @param newIndexShardsNum shards number of new index. <br/>
     *                          the shards number of source index must be multiple than this, and this must be bigger than [0]
     * @return
     */
    public Result shrinkIndex(String sourceIndexName, String newIndexName, int newIndexShardsNum){
        return shrinkIndex(sourceIndexName, newIndexName, newIndexShardsNum, null);
    }
}
