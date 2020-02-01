package org.vpzlin.javago.utils.elastic.elasticsearch;

import com.alibaba.fastjson.JSONArray;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.*;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import org.locationtech.jts.geom.Coordinate;

import org.locationtech.jts.geom.Geometry;
import org.vpzlin.javago.utils.Result;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class GeoUtil {
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
     * init class witch connection to ElasticSearch server
     * @param client
     * @throws Exception
     */
    public GeoUtil(RestHighLevelClient client) throws Exception{
        if(client == null){
            throw new Exception("Failed to init class [GeoUtil], the input parameter [RestHighLevelClient client] can't be null.");
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
    public GeoUtil(String[] serversIP, String serverPort, String connectProtocol, int connectTimeout, int socketTimeout) throws Exception{
        Result result = ClientUtil.getClient(serversIP, serverPort, connectProtocol, connectTimeout, socketTimeout);
        if(result.isSuccess()){
            this.client = (RestHighLevelClient)result.getData();
        }
        else {
            throw new Exception(String.format("Failed to init class [GeoUtil]. %s", result.getMessage()));
        }
    }

    /**
     * init class witch connection to ElasticSearch server
     * @param serversIP the servers' IP to connect
     * @param serverPort the servers' port to connect, the default value is [9200]
     * @throws Exception
     */
    public GeoUtil(String[] serversIP, String serverPort) throws Exception{
        Result result = ClientUtil.getClient(serversIP, serverPort);
        if(result.isSuccess()){
            this.client = (RestHighLevelClient)result.getData();
        }
        else {
            throw new Exception(String.format("Failed to init class [GeoUtil]. %s", result.getMessage()));
        }
    }

    /**
     * init class witch connection to ElasticSearch server
     * @param serversIP the servers' IP to connect
     * @throws Exception
     */
    public GeoUtil(String[] serversIP) throws Exception{
        Result result = ClientUtil.getClient(serversIP);
        if(result.isSuccess()){
            this.client = (RestHighLevelClient)result.getData();
        }
        else {
            throw new Exception(String.format("Failed to init class [GeoUtil]. %s", result.getMessage()));
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
    public GeoUtil(String serverIP, String serverPort, String connectProtocol, int connectTimeout, int socketTimeout) throws Exception{
        Result result = ClientUtil.getClient(serverIP, serverPort, connectProtocol, connectTimeout, socketTimeout);
        if(result.isSuccess()){
            this.client = (RestHighLevelClient)result.getData();
        }
        else {
            throw new Exception(String.format("Failed to init class [GeoUtil]. %s", result.getMessage()));
        }
    }

    /**
     * init class witch connection to ElasticSearch server
     * @param serverIP the server's IP to connect
     * @param serverPort the servers' port to connect, the default value is [9200]
     * @throws Exception
     */
    public GeoUtil(String serverIP, String serverPort) throws Exception{
        Result result = ClientUtil.getClient(serverIP, serverPort);
        if(result.isSuccess()){
            this.client = (RestHighLevelClient)result.getData();
        }
        else {
            throw new Exception(String.format("Failed to init class [GeoUtil]. %s", result.getMessage()));
        }
    }

    /**
     * init class witch connection to ElasticSearch server
     * @param serverIP the server's IP to connect
     * @throws Exception
     */
    public GeoUtil(String serverIP) throws Exception{
        Result result = ClientUtil.getClient(serverIP);
        if(result.isSuccess()){
            this.client = (RestHighLevelClient)result.getData();
        }
        else {
            throw new Exception(String.format("Failed to init class [GeoUtil]. %s", result.getMessage()));
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
     * query geo by bounding box
     * @param indexName index name
     * @param fieldName field name
     * @param geoTopLeftLongitude geo top left longitude
     * @param geoTopLeftLatitude geo top left latitude
     * @param geoBottomRightLongitude geo bottom left longitude
     * @param geoBottomRightLatitude geo bottom left latitude
     * @param recordsNumberStart records number to start read, it starts with [1] which means the first record
     * @param recordsSize the size of records to return
     * @return the type of Result.data is [com.alibaba.fastjson.JSONArray]
     */
    public Result queryGeoByBoundingBox(String indexName, String fieldName,
                                        double geoTopLeftLongitude, double geoTopLeftLatitude, double geoBottomRightLongitude, double geoBottomRightLatitude,
                                        int recordsNumberStart, int recordsSize){
        if(indexName == null || indexName.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to query geo by bounding box, the index name can't be null or empty."));
        }
        if(fieldName == null || fieldName.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to query geo by bounding box, the field name can't be null or empty."));
        }
        if(recordsNumberStart < 1){
            return Result.getResult(false, null, String.format("Failed to query geo by bounding box, the records number to start read can't be less than [1]."));
        }

        JSONArray jsonArray = new JSONArray();
        try {
            SearchRequest request = new SearchRequest(indexName);
            request.source(new SearchSourceBuilder()
                    .query(QueryBuilders.geoBoundingBoxQuery(fieldName)
                            .setCorners(geoTopLeftLatitude, geoTopLeftLongitude, geoBottomRightLatitude, geoBottomRightLongitude))
                    .from(recordsNumberStart - 1)
                    .size(recordsSize)
            );
            SearchResponse searchResponse = this.client.search(request, RequestOptions.DEFAULT);
            searchResponse.getHits().forEach(e -> {
                jsonArray.add(e.getSourceAsMap());
            });
            return Result.getResult(true, jsonArray, String.format("Finished querying geo by bounding box from index [%s] with field [%s].", indexName, fieldName));
        }
        catch (Exception e){
            return Result.getResult(true, jsonArray, String.format("Failed to query geo by bounding box from index [%s] with field [%s], more info = [%s].", indexName, fieldName, e.getMessage()));
        }
    }

    /**
     * query geo by distance
     * @param indexName index name
     * @param fieldName field name </br>
     *                  the type of field must be [geo_point] in ElasticSearch, it could be multilevel structure which field's structure likes [pin.location]
     * @param geoLongitude geo longitude
     * @param geoLatitude geo latitude
     * @param kiloMeters the distance to geo point (unit: kilometer)
     * @param recordsNumberStart records number to start read, it starts with [1] which means the first record
     * @param recordsSize the size of records to return
     * @return the type of Result.data is [com.alibaba.fastjson.JSONArray]
     */
    public Result queryGeoByDistance(String indexName, String fieldName,
                                     double geoLongitude, double geoLatitude,
                                     double kiloMeters,
                                     int recordsNumberStart, int recordsSize){
        if(indexName == null || indexName.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to query geo by bounding box, the index name can't be null or empty."));
        }
        if(fieldName == null || fieldName.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to query geo by bounding box, the field name can't be null or empty."));
        }
        if(recordsNumberStart < 1){
            return Result.getResult(false, null, String.format("Failed to query geo by bounding box, the records number to start read can't be less than [1]."));
        }

        JSONArray jsonArray = new JSONArray();
        try {
            SearchRequest request = new SearchRequest(indexName);
            request.source(new SearchSourceBuilder()
                    .query(QueryBuilders.geoDistanceQuery(fieldName)
                            .point(geoLatitude, geoLongitude).distance(kiloMeters, DistanceUnit.KILOMETERS))
                    .from(recordsNumberStart - 1)
                    .size(recordsSize)
            );
            SearchResponse searchResponse = this.client.search(request, RequestOptions.DEFAULT);
            searchResponse.getHits().forEach(e -> {
                jsonArray.add(e.getSourceAsMap());
            });

            return Result.getResult(true, jsonArray, String.format("Finished querying geo by distance from index [%s] with field [%s].", indexName, fieldName));
        }
        catch (Exception e){
            return Result.getResult(false, jsonArray, String.format("Failed to query geo by distance from index [%s] with field [%s], more info = [%s].", indexName, fieldName, e.getMessage()));
        }
    }

    /**
     * query geo by polygon
     * @param indexName index name
     * @param fieldName field name
     * @param geoPoints geo points, the first of each element in arrays is [latitude], the second one is [longitude]
     * @param recordsNumberStart records number to start read, it starts with [1] which means the first record
     * @param recordsSize the size of records to return
     * @return the type of Result.data is [com.alibaba.fastjson.JSONArray]
     */
    public Result queryGeoByPolygon(String indexName, String fieldName,
                                    double[][] geoPoints,
                                    int recordsNumberStart, int recordsSize){
        if(indexName == null || indexName.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to query geo by bounding box, the index name can't be null or empty."));
        }
        if(fieldName == null || fieldName.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to query geo by bounding box, the field name can't be null or empty."));
        }
        if(recordsNumberStart < 1){
            return Result.getResult(false, null, String.format("Failed to query geo by bounding box, the records number to start read can't be less than [1]."));
        }

        JSONArray jsonArray = new JSONArray();
        try {
            SearchRequest request = new SearchRequest(indexName);
            List<GeoPoint> geoPointList = new ArrayList<>();
            for(int i = 0; i < geoPoints.length; i++){
                geoPointList.add(new GeoPoint(geoPoints[i][1], geoPoints[i][0]));
            }
            request.source(new SearchSourceBuilder()
                    .query(QueryBuilders.geoPolygonQuery(fieldName, geoPointList))
                    .from(recordsNumberStart - 1)
                    .size(recordsSize)
            );
            SearchResponse searchResponse = this.client.search(request, RequestOptions.DEFAULT);
            searchResponse.getHits().forEach(e -> {
                jsonArray.add(e.getSourceAsMap());
            });

            return Result.getResult(true, jsonArray, String.format("Finished querying geo by polygon from index [%s] with field [%s].", indexName, fieldName));
        }
        catch (Exception e){
            return Result.getResult(false, jsonArray, String.format("Failed to query geo by polygon from index [%s] with field [%s], more info = [%s].", indexName, fieldName, e.getMessage()));
        }
    }


    /**
     * query geo by shape envelope
     * @param indexName index name
     * @param fieldName field name
     * @param geoTopLeftLatitude geo top left latitude
     * @param geoTopLeftLongitude geo top left longitude
     * @param geoBottomRightLongitude geo bottom right longitude
     * @param geoBottomRightLatitude geo bottom right latitude
     * @param geoShapeRelation relation ship of geo shape, this must be one of [INTERSECTS], [DISJOINT], [WITHIN]
     * @param recordsNumberStart records number to start read, it starts with [1] which means the first record
     * @param recordsSize the size of records to return
     * @return the type of Result.data is [com.alibaba.fastjson.JSONArray]
     */
    public Result queryGeoByShapeEnvelope(String indexName, String fieldName,
                                          double geoTopLeftLongitude, double geoTopLeftLatitude, double geoBottomRightLongitude, double geoBottomRightLatitude,
                                          String geoShapeRelation,
                                          int recordsNumberStart, int recordsSize){
        if(indexName == null || indexName.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to query geo by shape envelope, the index name can't be null or empty."));
        }
        if(fieldName == null || fieldName.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to query geo by shape envelope, the field name can't be null or empty."));
        }
        if(recordsNumberStart < 1){
            return Result.getResult(false, null, String.format("Failed to query geo by shape envelope, the records number to start read can't be less than [1]."));
        }

        geoShapeRelation = geoShapeRelation.trim().toUpperCase();
        if(!(geoShapeRelation.equals("INTERSECTS") || geoShapeRelation.equals("DISJOINT") ||geoShapeRelation.equals("WITHIN"))){
            return Result.getResult(false, null, String.format("Failed to query geo by shape envelope, the relation ship of geo shape must be one of [INTERSECTS], [DISJOINT], [WITHIN]."));
        }

        JSONArray jsonArray = new JSONArray();
        try {
            ShapeRelation shapeRelation = ShapeRelation.INTERSECTS;
            if(geoShapeRelation.equals("DISJOINT")){
                shapeRelation = ShapeRelation.DISJOINT;
            }
            else if(geoShapeRelation.equals("WITHIN")){
                shapeRelation = ShapeRelation.WITHIN;
            }

            SearchRequest request = new SearchRequest(indexName);
            EnvelopeBuilder envelopeBuilder = new EnvelopeBuilder(
                    new Coordinate(geoTopLeftLongitude,geoTopLeftLatitude),
                    new Coordinate(geoBottomRightLongitude,geoBottomRightLatitude));
            request.source(new SearchSourceBuilder().query(QueryBuilders.geoShapeQuery(fieldName,envelopeBuilder)
                    .relation(shapeRelation))
                    .from(recordsNumberStart - 1)
                    .size(recordsSize)
            );
            SearchResponse searchResponse = this.client.search(request, RequestOptions.DEFAULT);
            searchResponse.getHits().forEach(e -> {
                jsonArray.add(e.getSourceAsMap());
            });

            return Result.getResult(true, jsonArray, String.format("Finished querying geo by shape envelope from index [%s] with field [%s].", indexName, fieldName));
        }
        catch (Exception e){
            return Result.getResult(false, jsonArray, String.format("Failed to query geo by shape envelope from index [%s] with field [%s], more info = [%s].", indexName, fieldName, e.getMessage()));
        }
    }

    /**
     * query geo by shape point
     * @param indexName index name
     * @param fieldName field name
     * @param geoLongitude geo point longitude
     * @param geoLatitude geo point latitude
     * @param geoShapeRelation relation ship of geo shape, this must be one of [INTERSECTS], [DISJOINT], [WITHIN]
     * @param recordsNumberStart records number to start read, it starts with [1] which means the first record
     * @param recordsSize the size of records to return
     * @return the type of Result.data is [com.alibaba.fastjson.JSONArray]
     */
    public Result queryGeoByShapePoint(String indexName, String fieldName,
                                       double geoLongitude, double geoLatitude,
                                       String geoShapeRelation,
                                       int recordsNumberStart, int recordsSize){
        if(indexName == null || indexName.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to query geo by shape point, the index name can't be null or empty."));
        }
        if(fieldName == null || fieldName.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to query geo by shape point, the field name can't be null or empty."));
        }
        if(recordsNumberStart < 1){
            return Result.getResult(false, null, String.format("Failed to query geo by shape point, the records number to start read can't be less than [1]."));
        }

        geoShapeRelation = geoShapeRelation.trim().toUpperCase();
        if(!(geoShapeRelation.equals("INTERSECTS") || geoShapeRelation.equals("DISJOINT") ||geoShapeRelation.equals("WITHIN"))){
            return Result.getResult(false, null, String.format("Failed to query geo by shape point, the relation ship of geo shape must be one of [INTERSECTS], [DISJOINT], [WITHIN]."));
        }

        JSONArray jsonArray = new JSONArray();
        try {
            ShapeRelation shapeRelation = ShapeRelation.INTERSECTS;
            if(geoShapeRelation.equals("DISJOINT")){
                shapeRelation = ShapeRelation.DISJOINT;
            }
            else if(geoShapeRelation.equals("WITHIN")){
                shapeRelation = ShapeRelation.WITHIN;
            }

            SearchRequest request = new SearchRequest(indexName);
            PointBuilder pointBuilder = new PointBuilder(geoLongitude, geoLatitude);
            request.source(new SearchSourceBuilder().query(QueryBuilders.geoShapeQuery(fieldName, pointBuilder)
                    .relation(shapeRelation))
                    .from(recordsNumberStart - 1)
                    .size(recordsSize)
            );
            SearchResponse searchResponse = this.client.search(request, RequestOptions.DEFAULT);
            searchResponse.getHits().forEach(e -> {
                jsonArray.add(e.getSourceAsMap());
            });
            
            return Result.getResult(true, jsonArray, String.format("Finished querying geo by shape point from index [%s] with field [%s].", indexName, fieldName));
        }
        catch (Exception e){
            return Result.getResult(false, jsonArray, String.format("Failed to query geo by shape point from index [%s] with field [%s], more info = [%s].", indexName, fieldName, e.getMessage()));
        }
    }

    /**
     * query geo by shape line string
     * @param indexName index name
     * @param fieldName field name
     * @param geoPoints geo points, the first of each element in arrays is [longitude], the second one is [latitude]
     * @param geoShapeRelation relation ship of geo shape, this must be one of [INTERSECTS], [DISJOINT], [WITHIN]
     * @param recordsNumberStart records number to start read, it starts with [1] which means the first record
     * @param recordsSize the size of records to return
     * @return the type of Result.data is [com.alibaba.fastjson.JSONArray]
     */
    public Result queryGeoByShapeLineString(String indexName, String fieldName,
                                            double[][] geoPoints,
                                            String geoShapeRelation,
                                            int recordsNumberStart, int recordsSize){
        if(indexName == null || indexName.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to query geo by shape line string, the index name can't be null or empty."));
        }
        if(fieldName == null || fieldName.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to query geo by shape line string, the field name can't be null or empty."));
        }
        if(recordsNumberStart < 1){
            return Result.getResult(false, null, String.format("Failed to query geo by shape line string, the records number to start read can't be less than [1]."));
        }
        
        geoShapeRelation = geoShapeRelation.trim().toUpperCase();
        if(!(geoShapeRelation.equals("INTERSECTS") || geoShapeRelation.equals("DISJOINT") ||geoShapeRelation.equals("WITHIN"))){
            return Result.getResult(false, null, String.format("Failed to query geo by shape point, the relation ship of geo shape must be one of [INTERSECTS], [DISJOINT], [WITHIN]."));
        }

        int minPointsCount = 2;
        if(geoPoints.length < minPointsCount){
            return Result.getResult(false, null, String.format("Failed to query geo by shape line string, the points size must be at least [2]."));
        }

        JSONArray jsonArray = new JSONArray();
        try {
            ShapeRelation shapeRelation = ShapeRelation.INTERSECTS;
            if(geoShapeRelation.equals("DISJOINT")){
                shapeRelation = ShapeRelation.DISJOINT;
            }
            else if(geoShapeRelation.equals("WITHIN")){
                shapeRelation = ShapeRelation.WITHIN;
            }

            SearchRequest request = new SearchRequest(indexName);
            List<Coordinate> coordinateList = new LinkedList<>();
            for(int i = 0; i < geoPoints.length; i++){
                coordinateList.add(new Coordinate(geoPoints[i][0], geoPoints[i][1]));
            }
            // begin query
            LineStringBuilder lineStringBuilder = new LineStringBuilder(coordinateList);
            request.source(new SearchSourceBuilder().query(QueryBuilders.geoShapeQuery(fieldName, lineStringBuilder)
                    .relation(shapeRelation))
                    .from(recordsNumberStart - 1)
                    .size(recordsSize)
            );
            SearchResponse searchResponse = this.client.search(request, RequestOptions.DEFAULT);
            searchResponse.getHits().forEach(e -> {
                jsonArray.add(e.getSourceAsMap());
            });

            return Result.getResult(true, jsonArray, String.format("Finished querying geo by shape line string from index [%s] with field [%s].", indexName, fieldName));
        }
        catch (Exception e){
            return Result.getResult(false, jsonArray, String.format("Failed to query geo by shape line string from index [%s] with field [%s], more info = [%s].", indexName, fieldName, e.getMessage()));
        }
    }

    /**
     * query geo by shape polygon, this can't be used to query triangle
     * @param indexName index name
     * @param fieldName field name
     * @param geoPoints geo points, the first of each element in arrays is [longitude], the second one is [latitude]
     * @param geoShapeRelation relation ship of geo shape, this must be one of [INTERSECTS], [DISJOINT], [WITHIN]
     * @param recordsNumberStart records number to start read, it starts with [1] which means the first record
     * @param recordsSize the size of records to return
     * @return the type of Result.data is [com.alibaba.fastjson.JSONArray]
     */
    public Result queryGeoByShapePolygon(String indexName, String fieldName,
                                         double[][] geoPoints,
                                         String geoShapeRelation,
                                         int recordsNumberStart, int recordsSize){
        if(indexName == null || indexName.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to query geo by shape polygon, the index name can't be null or empty."));
        }
        if(fieldName == null || fieldName.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to query geo by shape polygon, the field name can't be null or empty."));
        }
        if(recordsNumberStart < 1){
            return Result.getResult(false, null, String.format("Failed to query geo by shape polygon, the records number to start read can't be less than [1]."));
        }

        geoShapeRelation = geoShapeRelation.trim().toUpperCase();
        if(!(geoShapeRelation.equals("INTERSECTS") || geoShapeRelation.equals("DISJOINT") ||geoShapeRelation.equals("WITHIN"))){
            return Result.getResult(false, null, String.format("Failed to query geo by polygon, the relation ship of geo shape must be one of [INTERSECTS], [DISJOINT], [WITHIN]."));
        }

        int minPointsCount = 4;
        if(geoPoints.length < minPointsCount){
            return Result.getResult(false, null, String.format("Failed to query geo by shape polygon, the points size must be at least [4]."));
        }

        JSONArray jsonArray = new JSONArray();
        try {
            ShapeRelation shapeRelation = ShapeRelation.INTERSECTS;
            if(geoShapeRelation.equals("DISJOINT")){
                shapeRelation = ShapeRelation.DISJOINT;
            }
            else if(geoShapeRelation.equals("WITHIN")){
                shapeRelation = ShapeRelation.WITHIN;
            }

            SearchRequest request = new SearchRequest(indexName);
            CoordinatesBuilder coordinatesBuilder = new CoordinatesBuilder();
            for(int i = 0; i < geoPoints.length; i++){
                coordinatesBuilder.coordinate(geoPoints[i][0], geoPoints[i][1]);
            }
            // begin query
            PolygonBuilder polygonBuilder = new PolygonBuilder(coordinatesBuilder);
            request.source(new SearchSourceBuilder().query(QueryBuilders.geoShapeQuery(fieldName, polygonBuilder)
                    .relation(shapeRelation))
                    .from(recordsNumberStart - 1)
                    .size(recordsSize)
            );
            SearchResponse searchResponse = this.client.search(request, RequestOptions.DEFAULT);
            searchResponse.getHits().forEach(e -> {
                jsonArray.add(e.getSourceAsMap());
            });

            return Result.getResult(true, jsonArray, String.format("Finished querying geo by shape polygon from index [%s] with field [%s].", indexName, fieldName));
        }
        catch (Exception e){
            return Result.getResult(false, jsonArray, String.format("Failed to query geo by shape polygon from index [%s] with field [%s], more info = [%s].", indexName, fieldName, e.getMessage()));
        }
    }

    /**
     * query geo by shape multi polygon
     * @param indexName index name
     * @param fieldName field name
     * @param geoPoints geo points, the first of each element in arrays is [longitude], the second one is [latitude]
     * @param geoShapeRelation relation ship of geo shape, this must be one of [INTERSECTS], [DISJOINT], [WITHIN]
     * @param recordsNumberStart records number to start read, it starts with [1] which means the first record
     * @param recordsSize the size of records to return
     * @return the type of Result.data is [com.alibaba.fastjson.JSONArray]
     */
    public Result queryGeoByShapeMultiPoints(String indexName, String fieldName,
                                             double[][] geoPoints,
                                             String geoShapeRelation,
                                             int recordsNumberStart, int recordsSize){
        if(indexName == null || indexName.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to query geo by shape multi polygon, the index name can't be null or empty."));
        }
        if(fieldName == null || fieldName.trim().length() == 0){
            return Result.getResult(false, null, String.format("Failed to query geo by shape multi polygon, the field name can't be null or empty."));
        }
        if(recordsNumberStart < 1){
            return Result.getResult(false, null, String.format("Failed to query geo by shape multi polygon, the records number to start read can't be less than [1]."));
        }

        geoShapeRelation = geoShapeRelation.trim().toUpperCase();
        if(!(geoShapeRelation.equals("INTERSECTS") || geoShapeRelation.equals("DISJOINT") ||geoShapeRelation.equals("WITHIN"))){
            return Result.getResult(false, null, String.format("Failed to query geo by multi polygon, the relation ship of geo shape must be one of [INTERSECTS], [DISJOINT], [WITHIN]."));
        }

        int minPointsCount = 2;
        if(geoPoints.length < minPointsCount){
            return Result.getResult(false, null, String.format("Failed to query geo by shape multi polygon, the points size must be at least [2]."));
        }

        JSONArray jsonArray = new JSONArray();
        try {
            ShapeRelation shapeRelation = ShapeRelation.INTERSECTS;
            if(geoShapeRelation.equals("DISJOINT")){
                shapeRelation = ShapeRelation.DISJOINT;
            }
            else if(geoShapeRelation.equals("WITHIN")){
                shapeRelation = ShapeRelation.WITHIN;
            }

            // begin query
            SearchRequest request = new SearchRequest(indexName);
            List<Coordinate> coordinateList = new LinkedList<>();
            for(int i = 0; i < geoPoints.length; i++){
                coordinateList.add(new Coordinate(geoPoints[i][0], geoPoints[i][1]));
            }
            MultiPointBuilder multiPointBuilder = new MultiPointBuilder(coordinateList);
            request.source(new SearchSourceBuilder().query(QueryBuilders.geoShapeQuery(fieldName, multiPointBuilder)
                    .relation(shapeRelation))
                    .from(recordsNumberStart - 1)
                    .size(recordsSize)
            );
            SearchResponse searchResponse = this.client.search(request, RequestOptions.DEFAULT);
            searchResponse.getHits().forEach(e -> {
                jsonArray.add(e.getSourceAsMap());
            });

            return Result.getResult(true, jsonArray, String.format("Finished querying geo by shape multi polygon from index [%s] with field [%s].", indexName, fieldName));
        }
        catch (Exception e){
            return Result.getResult(false, jsonArray, String.format("Failed to query geo by shape multi polygon from index [%s] with field [%s], more info = [%s].", indexName, fieldName, e.getMessage()));
        }
    }
}
