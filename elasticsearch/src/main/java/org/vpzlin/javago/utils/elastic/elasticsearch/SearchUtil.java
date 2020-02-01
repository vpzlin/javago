package org.vpzlin.javago.utils.elastic.elasticsearch;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;

import java.util.*;

public class SearchUtil {
    /**
     * ES访问默认方式
     */
    private String hostProtocol = "http";
    /**
     * ES连接池
     */
    private RestHighLevelClient client;
    /**
     * 日志工具
     */
    private static final Logger logger = Logger.getLogger(GeoUtil.class);
    /**
     * 超时默认分钟数
     */
    private int timeoutMinutes = 2;
    private String textAnalyzer = "";

    private static String SYMBOL_BRACKET_LEFT = "(";
    private static String SYMBOL_BRACKET_RIGHT = ")";
    private static String SYMBOL_AND = "&";
    private static String SYMBOL_OR = "|";
    private static String SYMBOL_MINUS = "-";
    private static String SYMBOL_STAR = "*";
    private static String SYMBOL_COLON = ":";
    private static String SYMBOL_COMMA = ",";
    private static String SYMBOL_EQUAL = "=";
    private static String SYMBOL_AT = "@";
    private static String SYMBOL_POUND = "#";
    private static String SYMBOL_PERCENT = "%";
    private static String STR_ANALYZER_IK_SMART = "ik_smart";
    private static String STR_ANALYZER_IK_MAX_WORD = "ik_max_word";
    private static String STR_GEO_DISTANCE = "geo_distance";
    private static String STR_GEO_DISTANCE_INTERSECTION = "geo_distance_intersection";
    private static String STR_GEO_DISTANCE_UNION = "geo_distance_union";
    private static String STR_GEO_BOUNDING_BOX = "geo_bounding_box";
    private static String STR_GEO_BOUNDING_BOX_INTERSECTION = "geo_bounding_box_intersection";
    private static String STR_GEO_BOUNDING_BOX_UNION = "geo_bounding_box_union";
    private static String STR_FLAG_COUNT = "count";
    private static String STR_FIELD_ASSIGN_MATCH_FULL = "@#";
    private static String STR_FIELD_ASSIGN_MATCH_PART = "@%";
    private static char CHAR_BRACKET_LEFT = '(';
    private static char CHAR_BRACKET_RIGHT = ')';
    private static char CHAR_TAB = '\t';
    private static char CHAR_BLANK = ' ';
    private static char CHAR_SYMBOL_AND = '&';
    private static char CHAR_SYMBOL_OR = '|';
    private static char CHAR_SYMBOL_MINUS = '-';
    private static char CHAR_SYMBOL_POUND = '#';
    private static char CHAR_SYMBOL_PERCENT = '%';

    /**
     * 构造函数
     * @param client 访问ES RestHighLevelClient
     */
    public SearchUtil(RestHighLevelClient client){
        this.client = client;
    }

    /**
     * 构造函数
     * @param hostsIP 访问ES服务器的ip集
     * @param hostPort 访问ES服务器的端口
     */
    public SearchUtil(String[] hostsIP, String hostPort){
        this.setClient(hostsIP, hostPort);
    }

    /**
     * 设置ES并进行连接
     * @param hostIP ES服务器IP
     * @param hostPort
     */
    public void setClient(String hostIP, String hostPort){
        this.client = ClientUtil.getClient(hostIP, hostPort, this.hostProtocol);
        if(this.client == null){
            logger.error("程序执行失败！程序退出。");
            System.exit(1);
        }
    }

    /**
     * 设置ES并进行连接
     * @param hostsIP ES服务器IP
     * @param hostPort
     */
    public void setClient(String[] hostsIP, String hostPort){
        this.client = ClientUtil.getClient(hostsIP, hostPort, this.hostProtocol);
        if(this.client == null){
            logger.error("程序执行失败！程序退出。");
            System.exit(1);
        }
    }

    /**
     * 关闭ES连接
     * @return 0: 成功
     *         1: 失败
     */
    public int closeClient(){
        return ClientUtil.closeClient(this.client);
    }

    /**
     * 设置分词器
     * @param analyzer 分词器文本
     */
    private void setTextAnalyzer(String analyzer){
        /* 设置分词器 */
        if(analyzer != null && analyzer.trim().length() > 0){
            analyzer = analyzer.trim().toLowerCase();
            if(STR_ANALYZER_IK_SMART.equals(analyzer)){
                this.textAnalyzer = STR_ANALYZER_IK_SMART;
            }
            else if(STR_ANALYZER_IK_MAX_WORD.equals(analyzer)){
                this.textAnalyzer = STR_ANALYZER_IK_MAX_WORD;
            }
            else {
                this.textAnalyzer = "";
            }
        }
    }

    /**
     * 匹配查询
     * @param textToQuery 查询的文本
     * @param fieldNameToQuery 查询的字段
     * @param fieldNamesToHighlight 高亮字段集（可为 null）
     * @param indicesNamesToQuery 查询的表
     * @param fieldsToOrderBy 排序字段（可为 null）
     * @param rowNumStart 开始行号
     * @param rowSize 获取的数据量
     * @return
     */
    public JSONObject matchQuery(String textToQuery, String fieldNameToQuery,
                                 String[] fieldNamesToHighlight,
                                 String[] indicesNamesToQuery,
                                 Map<String, String> fieldsToOrderBy,
                                 String analyzer,
                                 int rowNumStart, int rowSize){
        /* 设置分词器 */
        setTextAnalyzer(analyzer);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.matchQuery(fieldNameToQuery, textToQuery));

        /* 设置排序 */
        // 先以得分降序排序
        searchSourceBuilder.sort("_score", SortOrder.DESC);
        for(Map.Entry<String, String> entry: fieldsToOrderBy.entrySet()){
            String fieldName = entry.getKey().toString();
            // 默认升序
            SortOrder sortOrder = SortOrder.ASC;
            // 设置为降序
            if("DESC".equals(entry.getValue().trim().toUpperCase())){
                sortOrder = SortOrder.DESC;
            }
            searchSourceBuilder.sort(fieldName, sortOrder);
        }
        /* 设置超时 */
        searchSourceBuilder.timeout(TimeValue.timeValueMinutes(this.timeoutMinutes));
        /* 设置分页 */
        if(rowNumStart > 0) {
            searchSourceBuilder.from(rowNumStart);
        }
        if(rowSize > 0) {
            searchSourceBuilder.size(rowSize);
        }
        /* 设置高亮 */
        HighlightBuilder highlightBuilder = new HighlightBuilder().preTags("<highlight>").postTags("</highlight>");
        if(fieldNamesToHighlight.length == 1 && SYMBOL_STAR.equals(fieldNamesToHighlight[0].trim())){
            highlightBuilder.field("*");
        }
        else {
            for (String fieldNameToHighlight : fieldNamesToHighlight) {
                highlightBuilder.field(fieldNameToHighlight);
            }
        }
        searchSourceBuilder.highlighter(highlightBuilder);

        /* 开始进行查询 */
        SearchRequest request = new SearchRequest(indicesNamesToQuery).source(searchSourceBuilder);
        JSONObject data = new JSONObject();
        try {
            /* 查询明细数据 */
            SearchResponse dataSearchResponse = this.client.search(request, RequestOptions.DEFAULT);
            // 获取命中数（ES默认设置上限为1万条）
            data.put("_hits", dataSearchResponse.getHits().getTotalHits().value);
            // 获取耗时（秒）
            data.put("_took", dataSearchResponse.getTook().seconds());
            // 分片数
            data.put("_shards_total", dataSearchResponse.getTotalShards());
            data.put("_shards_successful", dataSearchResponse.getSuccessfulShards());
            data.put("_shards_failed", dataSearchResponse.getFailedShards());
            data.put("_shards_skipped", dataSearchResponse.getSkippedShards());
            List<Map<String, Object>> rows = new LinkedList<>();
            // 遍历明细数据
            dataSearchResponse.getHits().forEach(e ->{
                // 每行的数据
                JSONObject rowData = new JSONObject();
                // 得分
                rowData.put("_score", e.getScore());

                // 若设置了高亮字段，则取高亮的字段数据
                Map<String, HighlightField> highlightFieldMap = e.getHighlightFields();
                if(highlightFieldMap.size() > 0){
                    for(Map.Entry<String, HighlightField> entry: highlightFieldMap.entrySet()){
                        rowData.put(entry.getKey(), entry.getValue().fragments()[0]);
                    }
                }
                // 若没设置高亮，则取普通的字段数据
                else {
                    for(Map.Entry<String, Object> sourceMap: e.getSourceAsMap().entrySet()){
                        rowData.put(sourceMap.getKey(), sourceMap.getValue());
                    }
                }

                rows.add(rowData);
            });
            data.put("rows", rows);
            return data;
        }
        catch (Exception e){
            e.printStackTrace();
            logger.error("ElasticSearch match query失败！");
            return null;
        }
    }

    /**
     * 将搜索框中的条件转成对应的SQL中的条件
     * 不支持搜索的特殊符号： & | " ( )
     *     [ 搜索方案 ]
     *         空格        ： 同时满足空格左右2边的条件
     *         双and符 &   ： “与”条件，同空格
     *         双竖线  |   ： “或”条件
     *         双引号 ""   ： 完全匹配搜索（取消该功能，采用ES分词的评分机制来控制）
     *         减号    -   ： （ES-SQL API无法实现）不搜索减号后面的词（减号"-"前必须要有一个空格，否则将当成一整个字符串）
     *         @#= 三字符  ： 精确字段搜索，例： @#tel=0592-5969680 表示搜索电话号码字段tel值等于"0592-5969680"
     *         英文括号()  ： 条件组合（括号内的东西是解析的最高优先级）
     * @param text 搜索框中的条件文本
     */
    private BoolQueryBuilder getBoolQueryBuilderFromText(String text){
        text = text.trim();

        /**
         * 处理中英文符号异常问题
         */
        // 将中文括号替换为英文括号
        text = text.replace("（", "(");
        text = text.replace("）", ")");
        // 将中文双引号替换为英文双引号
        text = text.replace("“", "\"");
        text = text.replace("“", "\"");

        /**
         * 处理匹配异常问题
         */
        // 删除指定字段字符对"@#="的异常情况
        text = deleteUnmatchedFieldAssign(text);
        // 删除不匹配的英文括号
        text = deleteUnmatchedBrackets(text);
        // 删除匹配异常的双引号
        text = deleteUnmatchedDoubleQuote(text);
        // 再删除一次不匹配的英文括号（删掉异常双印号后，可能产生新的不匹配的英文括号）
        text = deleteUnmatchedBrackets(text);

        /**
         * 处理操作符问题
         */
        // 将重复空格替换成1个空格，然后将空格替换为 & 符号
        text = text.replaceAll(" +", " ").replace(" ", SYMBOL_AND);
        // 将重复的多个 &、|、-  这3种字符替换为1个
        text = text.replaceAll("&+", SYMBOL_AND);
        text = text.replaceAll("\\|+", SYMBOL_OR);
        text = text.replaceAll("-+", SYMBOL_MINUS);
        // 修补缺失的操作符
        text = fixMissingOperator(text);

        /**
         * 处理完字符串的异常问题后，若长度为0，则终止处理
         */
        if(text.trim().length() == 0){
            return null;
        }

        /**
         * 将文本转换成树节点
         */
        // 树根节点
        TreeNode rootNode = new TreeNode();
        // 将字符串中的括号拆解成节点列表（节点可能包含子节点）
        List<TreeNode> subTreeNodes = getConditionTreeNodeList(text);
        if(subTreeNodes != null){
            rootNode.subTreeNodeList.addAll(subTreeNodes);
        }

        /* 将树节点转换成 BoolQueryBuilder */
        BoolQueryBuilder boolQueryBuilder = getBoolQueryBuilderFromTreeNode(rootNode);

        return boolQueryBuilder;
    }

    /**
     * 删除不匹配的英文括号
     *     查找顺序先右再左进行括号匹配查找
     * @param str 待删除不匹配括号的字符串
     * @return 删除了不匹配括号后的字符串
     */
    private String deleteUnmatchedBrackets(String str){
        if(str == null){
            return null;
        }
        str = str.trim();

        // 括号索引数组，默认填充为0表示非括号或非异常
        // 值为 1 时表示括号有对应的匹配；值为 -1 时表示待删除的字符（未找到匹配的括号，或者仅包含多余的重复空格的括号）
        int[] idx = new int[str.length()];
        Arrays.fill(idx, 0);
        /* 将字符串转成字符数组，进行遍历 */
        char[] chars = str.toCharArray();
        // 先找不匹配的右括号
        for(int iRight = 0; iRight < str.length(); iRight++){
            if(chars[iRight] == ')'){
                // 右括号是否找到可匹配的左括号
                boolean found = false;
                int iLeft = iRight - 1;
                while(iLeft >= 0){
                    if(chars[iLeft] == '(' && idx[iLeft] == 0){
                        // 括号索引数组，值为 1 时表示括号有对应的匹配
                        idx[iLeft] = 1;
                        found = true;
                        break;
                    }
                    iLeft--;
                }
                // 右括号找到匹配的左括号
                if(found == true){
                    // 如果找到匹配的左右括号之间是多余的重复空格 或 左右括号之间无字符，则标记异常，以待删除
                    String tmp = str.substring(iLeft + 1, iRight);
                    if(countCharInString(tmp, CHAR_BLANK) == iRight - iLeft - 1){
                        // 将左右括号以及括号之间仅包含重复空格的字符，都索引标记为 -1
                        for(int i = iLeft; i <= iRight; i++){
                            idx[i] = -1;
                        }
                    }
                    // 正常的左右括号
                    else {
                        // 左右括号索引标记为 1
                        idx[iLeft] = 1;
                        idx[iRight] = 1;
                    }
                }
                else{
                    idx[iRight] = -1;
                }
            }
        }
        // 再将没有右括号可匹配的左括号，索引标记为 -1
        for(int i = 0; i < str.length(); i++){
            if(chars[i] == '(' && idx[i] == 0){
                idx[i] = -1;
            }
        }

        // 删除字符索引标记为 -1 的字符
        str = deleteStringByCharIndex(str, idx);
        /* 删除左右括号前后的空格 */
        chars = str.toCharArray();
        idx = new int[str.length()];
        Arrays.fill(idx, 0);
        // 将左括号后紧挨着的空格标记为 -1
        // 遍历索引的结束位， -3 是为了要判断引号后的字符时不会导致数组下表溢出
        int idxEnd = str.length() - 3;
        for(int i = 0; i < idxEnd; i++){
            if(chars[i] == '(' && chars[i+1] == CHAR_BLANK){
                idx[i+1] = -1;
            }
        }
        // 将右括号后紧挨着的空格标记为 -1
        for(int i = 2; i < str.length(); i++){
            if(chars[i] == ')' && chars[i-1] == CHAR_BLANK){
                idx[i-1] = -1;
            }
        }
        // 再删除标记为 -1 的字符
        str = deleteStringByCharIndex(str, idx);

        return str;
    }

    /**
     * 统计字符在字符串中出现的次数
     * @param str 字符串
     * @param c 字符
     * @return -1: 执行失败，传入null值的空字符串
     *         其他 >=0 的值: 字符在字符串中出现的次数
     */
    private int countCharInString(String str, char c){
        if(str == null){
            return -1;
        }

        int count = 0;
        char[] chars = str.toCharArray();
        for(int i = 0; i < chars.length; i++){
            if(chars[i] == c){
                count++;
            }
        }

        return count;
    }

    /**
     * 删除字符索引标记为 -1 的字符
     * @param str 待删除字符的字符串
     * @param idx 待删除字符在字符串中的索引数组
     * @return 删除标记为 -1 的字符后的字符串
     */
    private static String deleteStringByCharIndex(String str, int[] idx){
        if(str == null || str.length() != idx.length){
            return null;
        }

        // 将索引位标记为 -1 的字符删除
        StringBuilder strFinal = new StringBuilder();
        char[] chars = str.toCharArray();
        for(int i = 0; i < str.length(); i++){
            if(idx[i] != -1){
                strFinal.append(chars[i]);
            }
        }

        // 去除字符串中多余的空格，只保留 1 个空格
        str = strFinal.toString().replaceAll("\\s{2,}", " ");

        return str;
    }

    /**
     * 删除匹配异常的双引号
     * @param str
     * @return
     */
    private String deleteUnmatchedDoubleQuote(String str){
        if(str == null){
            return null;
        }
        str = str.trim();
        // 将连续的多个重复空格替换为 1个空格
        str = str.replaceAll("\\s{2,}", " ");

        // 字符串索引数组。默认填充值 0 表示普通字符，值 1 表示双引号有匹配；值 -1 表示匹配异常的双引号
        int[] idx = new int[str.length()];
        Arrays.fill(idx, 0);
        /* 将字符串转成字符数组，进行遍历 */
        char[] chars = str.toCharArray();
        // 查找左双引号
        for(int iLeft = 0; iLeft < str.length();){
            if(chars[iLeft] == '"' && idx[iLeft] == 0){
                // 左双引号已是最后一位，直接标记为 -1
                if(iLeft == str.length() - 1){
                    idx[iLeft] = -1;
                    break;
                }
                // 查找右双引号
                int iRight = iLeft + 1;
                while(iRight < str.length()){
                    if(chars[iRight] == '"' && idx[iRight] == 0){
                        // 左右双引号内的字符串
                        String strInnerQuote = str.substring(iLeft + 1, iRight);
                        // 如果左右双引号之间仅有重复的空格，或无字符，则将此对左右双引号及之间的字符标记为 -1
                        if(countCharInString(strInnerQuote, CHAR_BLANK) == iRight - iLeft - 1){
                            for(int i = iLeft; i <= iRight; i++){
                                idx[i] = -1;
                            }
                            // 左双引号索引位直接跳到右双引号索引位的下一位
                            iLeft = iLeft + iRight - iLeft + 1;
                            break;
                        }
                        // 如果左右双引号之间包含 左括号 或 右括号，则将 左双引号 标记为 -1
                        if(strInnerQuote.contains("(") == true || strInnerQuote.contains(")") == true){
                            idx[iLeft] = -1;
                            // 查找下一个 左双引号
                            iLeft++;
                            break;
                        }
                        // 找到了右双引号，标记索引位为 1
                        idx[iLeft] = 1;
                        idx[iRight] = 1;
                        // 左双引号索引位直接跳到右双引号索引位的下一位
                        iLeft = iLeft + iRight - iLeft + 1;
                        break;
                    }
                    iRight++;
                }
                // 未找到可匹配的右双引号，将左双引号标记为 -1
                if(iRight >= str.length()) {
                    idx[iLeft] = -1;
                    // 已没有可以匹配的双引号，终止循环查找
                    break;
                }
            }
            iLeft++;
        }

        // 删除字符索引标记为 -1 的字符
        str = deleteStringByCharIndex(str, idx);
        // 去除将双引号前后的空格
        idx = new int[str.length()];
        chars = str.toCharArray();
        // 将左双引号后紧挨着的空格标记为 -1
        // 遍历索引的结束位， -3 是为了要判断引号后的字符时不会导致数组下表溢出
        int idxEnd = str.length() - 3;
        for(int i = 0; i < idxEnd; i++){
            if(chars[i] == '"' && chars[i+1] == CHAR_BLANK){
                idx[i+1] = -1;
            }
        }
        // 将右双引号后紧挨着的空格标记为 -1
        for(int i = 2; i < str.length(); i++){
            if(chars[i] == '"' && chars[i-1] == CHAR_BLANK){
                idx[i-1] = -1;
            }
        }
        // 再删除标记为 -1 的字符
        str = deleteStringByCharIndex(str, idx);

        return str;
    }

    /**
     * 删除指定字段字符对"@#="与"@%="的异常情况，即"@#="、"@%="各是3字符为一组的字符串对
     * @param str
     * @return
     */
    private static String deleteUnmatchedFieldAssign(String str){
        if(str == null || str.trim().length() == 0){
            return str;
        }
        str = str.trim();

        // "@"符号的索引位
        int idxSymbolAt = 0;
        /**
         * 开始遍历处理
         */
        while(str.indexOf(SYMBOL_AT, idxSymbolAt) != -1){
            idxSymbolAt = str.indexOf(SYMBOL_AT, idxSymbolAt);

            // "@"符号已是字符串最后的2个字符
            if(idxSymbolAt >= str.length() - 2){
                break;
            }
            // "@"符号后面不是"#"符号或"%"符号的，进入下一个循环，找下一个"@"符号
            char symbolAtNextChar = str.charAt(idxSymbolAt + 1);
            if(!(symbolAtNextChar == CHAR_SYMBOL_POUND || symbolAtNextChar == CHAR_SYMBOL_PERCENT)){
                idxSymbolAt++;
                continue;
            }

            // 没有找到"="符号的，则去掉本轮的"@#"或"@%"标识符，并进入下一个循环
            int idxSymbolEqual = str.indexOf(SYMBOL_EQUAL, idxSymbolAt + 1);
            if(idxSymbolEqual == -1) {
                str = str.substring(0, idxSymbolAt) + str.substring(idxSymbolAt + 2);
                continue;
            }
            // 若"="符号已经是字符串的最后字符，则直接舍弃掉"@"及之后的字符串，并终止循环
            if(str.substring(idxSymbolEqual + 1).replace("\t", "").trim().length() == 0){
                str = str.substring(0, idxSymbolAt);
                break;
            }

            String insideSymbol = str.substring(idxSymbolAt + 2, idxSymbolEqual);
            // 若"="符号与"@#"或"@%"标识符之间还包含了其他的"@#"或"@%"标识符或者左右圆括号"()"，则去除本轮的标识符（去除包含了括号是因为括号优先原则）
            if(insideSymbol.contains(SYMBOL_POUND) || insideSymbol.contains(SYMBOL_PERCENT)
                    || insideSymbol.contains(SYMBOL_BRACKET_LEFT) || insideSymbol.contains(SYMBOL_BRACKET_RIGHT)){
                str = str.substring(0, idxSymbolAt) + str.substring(idxSymbolAt + 2);
                continue;
            }
            // 若"="符号与"@#"或"@%"标识符之间仅有空格或tab制表符，则去除本轮的"="符号与标识符"@#"或标识符"@%"
            if(insideSymbol.replace("\t", "").trim().length() == 0){
                str = str.substring(0, idxSymbolAt) + str.substring(idxSymbolEqual + 1);
                continue;
            }
            // 去除"="符号与"@#"或"@%"标识符之间包含的空格与tab制表符
            else if(insideSymbol.length() != insideSymbol.replace("\t", "").trim().length()){
                // 待删除索引位，0标识正常，-1标识待删除
                int[] idxToDelete = new int[str.length()];
                Arrays.fill(idxToDelete, 0);
                for(int i = idxSymbolAt + 1; i < idxSymbolEqual; i++){
                    if(str.charAt(i) == CHAR_BLANK || str.charAt(i) == CHAR_TAB){
                        idxToDelete[i] = -1;
                    }
                }
                str = deleteStringByCharIndex(str, idxToDelete);
                // 由于删除了空格，重新查找"="符号的索引位
                idxSymbolEqual = str.indexOf(SYMBOL_EQUAL, idxSymbolAt);
            }

            // 去掉"="符号后紧接着的空格或tab制表符
            while(str.charAt(idxSymbolEqual + 1) == CHAR_BLANK || str.charAt(idxSymbolEqual + 1) == CHAR_TAB){
                str = str.substring(0, idxSymbolEqual + 1) + str.substring(idxSymbolEqual + 2);
            }

            idxSymbolAt = idxSymbolEqual + 1;
        }

        return str;
    }

    /**
     * 修补缺失的操作符
     * @param text
     * @return
     */
    private String fixMissingOperator(String text){
        if(text == null || text.trim().length() == 0){
            return null;
        }
        text = text.trim();

        // 去掉连续的2个操作符，保留后1个操作符
        int i = 0;
        while(i < text.length() - 1){
            // 第1个操作符
            String sCurrent = String.valueOf(text.charAt(i));
            if(sCurrent.equals(SYMBOL_AND) || sCurrent.equals(SYMBOL_OR) || sCurrent.equals(SYMBOL_MINUS)){
                // 第2个操作符
                String sNext = String.valueOf(text.charAt(i + 1));
                if(sNext.equals(SYMBOL_AND) || sNext.equals(SYMBOL_OR) || sNext.equals(SYMBOL_MINUS)
                        || sNext.equals(SYMBOL_BRACKET_LEFT) || sNext.equals(SYMBOL_BRACKET_RIGHT)){
                    text = text.substring(0, i) + text.substring(i + 1);
                }
            }
            i++;
        }

        // 先处理右括号后没有操作符的情况，若无，则添加"&"与操作
        int idxBracketRight = 0;
        while(idxBracketRight < text.length() - 1){
            idxBracketRight = text.indexOf(SYMBOL_BRACKET_RIGHT, idxBracketRight);
            // 没找到右括号 或 右括号已达末尾
            if(idxBracketRight == -1 || idxBracketRight >= text.length() - 1){
                break;
            }
            String s = text.substring(idxBracketRight + 1, idxBracketRight + 2);
            if(!(s.equals(SYMBOL_AND) || s.equals(SYMBOL_OR) || s.equals(SYMBOL_MINUS))){
                text = text.substring(0, idxBracketRight + 1) + SYMBOL_AND + text.substring(idxBracketRight + 1);
                idxBracketRight +=1;
            }

            idxBracketRight +=1;
        }

        // 再处理左括号前没有操作符的情况
        int idxBracketLeft = 0;
        while (idxBracketLeft < text.length()) {
            idxBracketLeft = text.indexOf(SYMBOL_BRACKET_LEFT, idxBracketLeft);
            if(idxBracketLeft == -1){
                break;
            }
            // 左括号在最左
            if(idxBracketLeft == 0){
                // 与左括号匹配的右括号后面是"|"操作符
                // 查找与左括号匹配的右括号
                idxBracketRight = text.indexOf(SYMBOL_BRACKET_RIGHT);
                while(countCharInString(text.substring(idxBracketLeft + 1, idxBracketRight), CHAR_BRACKET_LEFT)
                        != countCharInString(text.substring(idxBracketLeft + 1, idxBracketRight), CHAR_BRACKET_RIGHT)){
                    idxBracketRight = text.indexOf(")", idxBracketRight + 1);
                }
                if(idxBracketRight < text.length() - 1
                        && text.substring(idxBracketRight + 1, idxBracketRight + 2).equals(SYMBOL_OR)){
                    text = text.substring(0, idxBracketLeft + 1) + SYMBOL_OR + text.substring(idxBracketLeft + 1);
                    idxBracketLeft += 1;
                }
                else {
                    // 其他操作符
                    text = SYMBOL_AND + text;
                    idxBracketLeft += 1;
                }
            }
            // 左括号在中间
            else {
                String s = text.substring(idxBracketLeft - 1, idxBracketLeft);
                // 位置处于中间的左括号前没有操作符，则视为"&"操作
                if(!(s.equals(SYMBOL_AND) || s.equals(SYMBOL_OR) || s.equals(SYMBOL_MINUS))){
                    text = text.substring(0, idxBracketLeft) + SYMBOL_AND + text.substring(idxBracketLeft);
                    idxBracketLeft += 1;
                }
            }
            idxBracketLeft += 1;
        }

        // 左括号后面紧跟的不是操作符的情况
        idxBracketLeft = 0;
        while(idxBracketLeft < text.length() - 1){
            // 左括号索引位
            idxBracketLeft = text.indexOf(SYMBOL_BRACKET_LEFT, idxBracketLeft);
            if(idxBracketLeft == -1){
                break;
            }
            // 如果当前左括号后面已是操作符，则遍历寻找下一个操作符
            String s = String.valueOf(text.charAt(idxBracketLeft + 1));
            if(s.equals(SYMBOL_AND) || s.equals(SYMBOL_OR) || s.equals(SYMBOL_MINUS)){
                idxBracketLeft++;
                continue;
            }

            // 左括号后没有操作符的情况，查找操作符并添加进去
            String subText = text.substring(idxBracketLeft + 1);
            char[] subChars = subText.toCharArray();
            for(i = 0; i < subChars.length; i++){
                s = String.valueOf(subChars[i]);
                if(s.equals(SYMBOL_OR)){
                    // 补充在左括号后
                    text = text.substring(0, idxBracketLeft + 1) + SYMBOL_OR + text.substring(idxBracketLeft + 1);
                    break;
                }
                else if(s.equals(SYMBOL_AND) || s.equals(SYMBOL_MINUS)
                        || s.equals(SYMBOL_BRACKET_LEFT) || s.equals(SYMBOL_BRACKET_RIGHT)){
                    // 补充在左括号后
                    text = text.substring(0, idxBracketLeft + 1) + SYMBOL_AND + text.substring(idxBracketLeft + 1);
                    break;
                }
            }

            idxBracketLeft++;
        }

        // 再处理字符串首个条件的问题
        String s = text.substring(0, 1);
        if(!(s.equals(SYMBOL_AND) || s.equals(SYMBOL_OR) || s.equals(SYMBOL_MINUS))){
            int idxSymbolAnd = text.indexOf(SYMBOL_AND);
            int idxSymbolOr = text.indexOf(SYMBOL_OR);

            // 查找最近匹配的符号
            if(idxSymbolOr > -1){
                if(idxSymbolAnd == -1){
                    text = SYMBOL_OR + text;
                }
                else if(idxSymbolOr < idxSymbolAnd){
                    text = SYMBOL_OR + text;
                }
                else {
                    text = SYMBOL_AND + text;
                }
            }
            else {
                text = SYMBOL_AND + text;
            }
        }

        // 修补"@#"前没有操作符的情况，默认为and操作符（需已经提前处理好"@#="不匹配的情况）
        int idx = 0;
        while(idx != -1 && text.indexOf(STR_FIELD_ASSIGN_MATCH_FULL, idx) != -1){
            idx = text.indexOf(STR_FIELD_ASSIGN_MATCH_FULL, idx);
            if(idx == 0){
                text = SYMBOL_AND + text;
                idx++;
            }
            else if(!(text.charAt(idx - 1) == CHAR_SYMBOL_AND
                    || text.charAt(idx - 1) == CHAR_SYMBOL_OR
                    || text.charAt(idx - 1) == CHAR_SYMBOL_MINUS)){
                text = text.substring(0, idx) + SYMBOL_AND + text.substring(idx);
                idx++;
            }

            idx = text.indexOf(SYMBOL_EQUAL, idx + 1) + 1;
        }

        // 修补"@%"前没有操作符的情况，默认为and操作符（需已经提前处理好"@%="不匹配的情况）
        idx = 0;
        while(idx != -1 && text.indexOf(STR_FIELD_ASSIGN_MATCH_PART, idx) != -1){
            idx = text.indexOf(STR_FIELD_ASSIGN_MATCH_PART, idx);
            if(idx == 0){
                text = SYMBOL_AND + text;
                idx++;
            }
            else if(!(text.charAt(idx - 1) == CHAR_SYMBOL_AND
                    || text.charAt(idx - 1) == CHAR_SYMBOL_OR
                    || text.charAt(idx - 1) == CHAR_SYMBOL_MINUS)){
                text = text.substring(0, idx) + SYMBOL_AND + text.substring(idx);
                idx++;
            }

            idx = text.indexOf(SYMBOL_EQUAL, idx + 1) + 1;
        }

        return text;
    }

    /**
     * 根据带操作符的字符串，处理成树与树节点
     * @param text 原文本
     * @return 有父子结构的树节点
     */
    private List<TreeNode> getConditionTreeNodeList(String text){
        if(text == null || text.trim().length() == 0){
            return null;
        }
        text = text.trim();

        List<TreeNode> treeNodeList = new LinkedList<>();

        /**
         * 先处理括号问题，左括号匹配原则。括号内的字符串处理为子节点
         */
        while(text.contains(SYMBOL_BRACKET_LEFT) == true){
            // 查找匹配的括号对（这里只考虑左右括号数量相等的情况，因此需提前处理括号不相等的异常情况）
            int idxBracketLeft = text.indexOf(SYMBOL_BRACKET_LEFT);
            int idxBracketRight = text.indexOf(SYMBOL_BRACKET_RIGHT);
            while(countCharInString(text.substring(idxBracketLeft + 1, idxBracketRight), CHAR_BRACKET_LEFT)
                    != countCharInString(text.substring(idxBracketLeft + 1, idxBracketRight), CHAR_BRACKET_RIGHT)){
                // 右括号索引移动到下一个右括号
                idxBracketRight = text.indexOf(SYMBOL_BRACKET_LEFT, idxBracketRight + 1);
            }

            // 添加节点
            TreeNode treeNode= new TreeNode();
            treeNode.operator = text.substring(idxBracketLeft -1, idxBracketLeft);
            treeNode.subTreeNodeList = getConditionTreeNodeList(text.substring(idxBracketLeft + 1, idxBracketRight));
            treeNodeList.add(treeNode);
            // 将已处理的从字符串中去除
            text = text.substring(0, idxBracketLeft - 1) + text.substring(idxBracketRight + 1);
        }

        /**
         * 再处理非括号对之内的数据（已提前处理补充了操作符）
         */
        // 字符串转为字符数组
        char[] args = text.toCharArray();
        // 操作符，如"|"、"&"、"-"
        String operator = null;
        StringBuilder sb = null;
        for(int i = 0; i < args.length; i++){
            if(operator == null){
                operator = String.valueOf(args[i]);
                sb = new StringBuilder();
                continue;
            }
            String sI = String.valueOf(args[i]);
            if(sI.equals(SYMBOL_AND) || sI.equals(SYMBOL_OR) || sI.equals(SYMBOL_MINUS)){
                // 可能输入了错误的连续操作符，只取连续操作符中的最后一个
                if(sb.length() == 0){
                    operator = String.valueOf(args[i]);
                }
                else {
                    TreeNode treeNode = new TreeNode();
                    treeNode.operator = operator;
                    treeNode.text = sb.toString();
                    treeNodeList.add(treeNode);
                    // 重置参数
                    operator = String.valueOf(args[i]);
                    sb = new StringBuilder();
                }
            }
            else {
                sb.append(args[i]);
            }
        }
        // 处理循环结束后剩余的节点信息
        if(sb.length() > 0){
            TreeNode treeNode = new TreeNode();
            treeNode.operator = operator;
            treeNode.text = sb.toString();
            treeNodeList.add(treeNode);
        }

        return treeNodeList;
    }

    /**
     * 将条件树转成字符串形式的条件
     * @param node 节点
     * @return SQL条件字符串
     */
    private BoolQueryBuilder getBoolQueryBuilderFromTreeNode(TreeNode node){
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        for(TreeNode subTreeNode: node.subTreeNodeList){
            // and 条件
            if(subTreeNode.operator.equals(SYMBOL_AND)){
                // 括号节点，添加括号节点内的节点为子节点
                if(subTreeNode.subTreeNodeList.size() > 0){
                    boolQueryBuilder.must(getBoolQueryBuilderFromTreeNode(subTreeNode));
                }
                // 非括号节点
                else{
                    // 以"@#"开头的指定字段查询
                    if(subTreeNode.text.trim().startsWith(STR_FIELD_ASSIGN_MATCH_FULL) == true){
                        String fieldName = subTreeNode.text.substring(STR_FIELD_ASSIGN_MATCH_FULL.length(), subTreeNode.text.indexOf(SYMBOL_EQUAL, STR_FIELD_ASSIGN_MATCH_FULL.length()));
                        String fieldValue = subTreeNode.text.substring(subTreeNode.text.indexOf(SYMBOL_EQUAL) + 1);
                        boolQueryBuilder.must(QueryBuilders.termQuery(fieldName, fieldValue));
                    }
                    // 以"@%"开头的指定字段查询
                    else if(subTreeNode.text.trim().startsWith(STR_FIELD_ASSIGN_MATCH_PART) == true){
                        String fieldName = subTreeNode.text.substring(STR_FIELD_ASSIGN_MATCH_PART.length(), subTreeNode.text.indexOf(SYMBOL_EQUAL, STR_FIELD_ASSIGN_MATCH_PART.length()));
                        String fieldValue = subTreeNode.text.substring(subTreeNode.text.indexOf(SYMBOL_EQUAL) + 1);
                        // 以"@%"开头的模糊查询，需要在值前后加上星号
                        fieldValue = SYMBOL_STAR + fieldValue + SYMBOL_STAR;
                        boolQueryBuilder.must(QueryBuilders.matchPhraseQuery(fieldName, fieldValue));
                    }
                    // 不以"@#"或"@%"开头的情况
                    else {
                        MultiMatchQueryBuilder multiMatchQueryBuilder = new MultiMatchQueryBuilder(subTreeNode.text);
                        if(textAnalyzer != null && textAnalyzer.trim().length() > 0) {
                            multiMatchQueryBuilder = multiMatchQueryBuilder.analyzer(textAnalyzer);
                        }
                        boolQueryBuilder.must(multiMatchQueryBuilder);
                    }
                }
            }
            // or 条件
            else if(subTreeNode.operator.equals(SYMBOL_OR)){
                // 括号节点，添加括号节点内的节点为子节点
                if(subTreeNode.subTreeNodeList.size() > 0){
                    boolQueryBuilder.should(getBoolQueryBuilderFromTreeNode(subTreeNode));
                }
                // 非括号节点
                else{
                    // 以"@#"开头的指定字段查询
                    if(subTreeNode.text.trim().startsWith(STR_FIELD_ASSIGN_MATCH_FULL) == true){
                        String fieldName = subTreeNode.text.substring(STR_FIELD_ASSIGN_MATCH_FULL.length(), subTreeNode.text.indexOf(SYMBOL_EQUAL, STR_FIELD_ASSIGN_MATCH_FULL.length()));
                        String fieldValue = subTreeNode.text.substring(subTreeNode.text.indexOf(SYMBOL_EQUAL) + 1);
                        boolQueryBuilder.should(QueryBuilders.termQuery(fieldName, fieldValue));
                    }
                    // 以"@%"开头的指定字段查询
                    else if(subTreeNode.text.trim().startsWith(STR_FIELD_ASSIGN_MATCH_PART) == true){
                        String fieldName = subTreeNode.text.substring(STR_FIELD_ASSIGN_MATCH_PART.length(), subTreeNode.text.indexOf(SYMBOL_EQUAL, STR_FIELD_ASSIGN_MATCH_PART.length()));
                        String fieldValue = subTreeNode.text.substring(subTreeNode.text.indexOf(SYMBOL_EQUAL) + 1);
                        // 以"@%"开头的模糊查询，需要在值前后加上星号
                        fieldValue = SYMBOL_STAR + fieldValue + SYMBOL_STAR;
                        boolQueryBuilder.should(QueryBuilders.matchPhraseQuery(fieldName, fieldValue));
                    }
                    // 不以"@#"或"@%"开头的情况
                    else {
                        MultiMatchQueryBuilder multiMatchQueryBuilder = new MultiMatchQueryBuilder(subTreeNode.text);
                        if(textAnalyzer != null && textAnalyzer.trim().length() > 0) {
                            multiMatchQueryBuilder = multiMatchQueryBuilder.analyzer(textAnalyzer);
                        }
                        boolQueryBuilder.should(multiMatchQueryBuilder);
                    }
                }
            }
            // minus 条件
            else{
                // 括号节点，添加括号节点内的节点为子节点
                if(subTreeNode.subTreeNodeList.size() > 0){
                    boolQueryBuilder.mustNot(getBoolQueryBuilderFromTreeNode(subTreeNode));
                }
                // 非括号节点
                else{
                    // 以"@#"开头的指定字段查询
                    if(subTreeNode.text.trim().startsWith(STR_FIELD_ASSIGN_MATCH_FULL) == true){
                        String fieldName = subTreeNode.text.substring(STR_FIELD_ASSIGN_MATCH_FULL.length(), subTreeNode.text.indexOf(SYMBOL_EQUAL, STR_FIELD_ASSIGN_MATCH_FULL.length()));
                        String fieldValue = subTreeNode.text.substring(subTreeNode.text.indexOf(SYMBOL_EQUAL) + 1);
                        boolQueryBuilder.mustNot(QueryBuilders.termQuery(fieldName, fieldValue));
                    }
                    // 以"@%"开头的指定字段查询
                    else if(subTreeNode.text.trim().startsWith(STR_FIELD_ASSIGN_MATCH_PART) == true){
                        String fieldName = subTreeNode.text.substring(STR_FIELD_ASSIGN_MATCH_PART.length(), subTreeNode.text.indexOf(SYMBOL_EQUAL, STR_FIELD_ASSIGN_MATCH_PART.length()));
                        String fieldValue = subTreeNode.text.substring(subTreeNode.text.indexOf(SYMBOL_EQUAL) + 1);
                        // 以"@%"开头的模糊查询，需要在值前后加上星号
                        fieldValue = SYMBOL_STAR + fieldValue + SYMBOL_STAR;
                        boolQueryBuilder.mustNot(QueryBuilders.matchPhraseQuery(fieldName, fieldValue));
                    }
                    // 不以"@#"或"@%"开头的情况
                    else {
                        MultiMatchQueryBuilder multiMatchQueryBuilder = new MultiMatchQueryBuilder(subTreeNode.text);
                        if(textAnalyzer != null && textAnalyzer.trim().length() > 0) {
                            multiMatchQueryBuilder = multiMatchQueryBuilder.analyzer(textAnalyzer);
                        }
                        boolQueryBuilder.mustNot(multiMatchQueryBuilder);
                    }
                }
            }
        }

        return boolQueryBuilder;
    }

    /**
     * 字符串数组去重
     * @param args 字符串数组
     * @return 去重后的字符串数组
     */
    private String[] removeDuplicates(String[] args){
        if(args == null){
            return null;
        }

        List<String> list = new LinkedList<>();
        for(String s:args){
            // 只保留非空字段
            if(list.contains(s) == false && s.trim().length() > 0){
                list.add(s.trim());
            }
        }

        String[] argsFinal = new String[list.size()];
        for(int i = 0; i < list.size(); i++){
            argsFinal[i] = list.get(i);
        }

        return argsFinal;
    }

    /**
     * 根据以逗号分隔的索引名字符串，获取索引名数组
     * @param indicesNamesString 以逗号分隔的索引名字符串
     * @return 索引名数组
     */
    private String[] getIndicesNames(String indicesNamesString){
        String[] indicesNames = null;
        if(indicesNamesString != null && indicesNamesString.trim().length() > 0){
            // 去除空格
            indicesNamesString = indicesNamesString.replace(" ", "");
            // 将字符";"替换为字符","，以防输入错误
            indicesNamesString = indicesNamesString.replace(";", ",");
            String[] argsSplited = indicesNamesString.split(",");
            if(argsSplited.length > 0){
                indicesNames = argsSplited;
            }
        }

        return indicesNames;
    }

    /**
     * 添加地图搜索设置
     * @param boolQueryBuilder
     * @param geoLimit 地图搜索限制字符串
     * @return
     */
    private BoolQueryBuilder addGeoLimit(BoolQueryBuilder boolQueryBuilder, String geoLimit){
        // 地图坐标设置
        if(geoLimit != null && geoLimit.trim().length() > 0){
            // 替换空格与tab符，并根据":"转成链表
            geoLimit = geoLimit.replace(" ", "").replace("\t", "").trim();
            LinkedList<String> list = new LinkedList<>();
            for(String str: geoLimit.split(SYMBOL_COLON)){
                if(str.trim().length() > 0){
                    list.add(str.trim());
                }
            }

            String geoType = list.pop();
            String fieldName = list.pop();
            String value = list.pop();
            // 中心圆的查询
            if(geoType.equals(STR_GEO_DISTANCE)){
                String[] values = value.split(SYMBOL_COMMA);
                // 中心圆点经度
                double lon = Double.valueOf(values[0]);
                // 中心圆点纬度
                double lat = Double.valueOf(values[1]);
                // 半径距离（单位：公里）
                double distance = Double.valueOf(values[2]);
                boolQueryBuilder.must(QueryBuilders.geoDistanceQuery(fieldName)
                        .point(lat, lon).distance(distance, DistanceUnit.KILOMETERS));
            }
            // 中心圆的交集
            else if(geoType.equals(STR_GEO_DISTANCE_INTERSECTION)){
                int idx = 0;
                while(idx >=0 && value.indexOf(SYMBOL_BRACKET_LEFT, idx) != -1){
                    /**
                     * 处理每对括号
                     */
                    idx = value.indexOf(SYMBOL_BRACKET_LEFT, idx);
                    String[] values = value.substring(idx + 1, value.indexOf(SYMBOL_BRACKET_RIGHT, idx)).split(SYMBOL_COMMA);
                    // 中心圆点经度
                    double lon = Double.valueOf(values[0]);
                    // 中心圆点纬度
                    double lat = Double.valueOf(values[1]);
                    // 半径距离（单位：公里）
                    double distance = Double.valueOf(values[2]);
                    boolQueryBuilder.must(QueryBuilders.geoDistanceQuery(fieldName)
                            .point(lat, lon).distance(distance, DistanceUnit.KILOMETERS));

                    // 遍历下1对括号
                    idx = value.indexOf(SYMBOL_BRACKET_LEFT, idx);
                }
            }
            // 中心圆的并集
            else if(geoType.equals(STR_GEO_DISTANCE_UNION)){
                int idx = 0;
                while(idx >=0 && value.indexOf(SYMBOL_BRACKET_LEFT, idx) != -1){
                    /**
                     * 处理每对括号
                     */
                    idx = value.indexOf(SYMBOL_BRACKET_LEFT, idx);
                    String[] values = value.substring(idx + 1, value.indexOf(SYMBOL_BRACKET_RIGHT, idx)).split(SYMBOL_COMMA);
                    // 中心圆点经度
                    double lon = Double.valueOf(values[0]);
                    // 中心圆点纬度
                    double lat = Double.valueOf(values[1]);
                    // 半径距离（单位：公里）
                    double distance = Double.valueOf(values[2]);
                    boolQueryBuilder.should(QueryBuilders.geoDistanceQuery(fieldName)
                            .point(lat, lon).distance(distance, DistanceUnit.KILOMETERS));

                    // 遍历下1对括号
                    idx = value.indexOf(SYMBOL_BRACKET_LEFT, idx);
                }
            }
            // 矩形坐标查询
            else if(geoType.equals(STR_GEO_BOUNDING_BOX)){
                String[] values = value.split(SYMBOL_COMMA);
                // 矩形左上角经度
                double lonTopLeft = Double.valueOf(values[0]);
                // 矩形左上角纬度
                double latTopLeft = Double.valueOf(values[1]);
                // 矩形右下角经度
                double lonBottomRight = Double.valueOf(values[2]);
                // 矩形右下角纬度
                double latBottomRight = Double.valueOf(values[3]);
                // 接口的左边是先纬度、再经度
                boolQueryBuilder.must(QueryBuilders.geoBoundingBoxQuery(fieldName)
                        .setCorners(latTopLeft, lonTopLeft, latBottomRight, lonBottomRight));
            }
            // 矩形坐标交集
            else if(geoType.equals(STR_GEO_BOUNDING_BOX_INTERSECTION)){
                int idx = 0;
                while(idx >=0 && value.indexOf(SYMBOL_BRACKET_LEFT, idx) != -1){
                    /**
                     * 处理每对括号
                     */
                    idx = value.indexOf(SYMBOL_BRACKET_LEFT, idx);
                    String[] values = value.split(SYMBOL_COMMA);
                    // 矩形左上角经度
                    double lonTopLeft = Double.valueOf(values[0]);
                    // 矩形左上角纬度
                    double latTopLeft = Double.valueOf(values[1]);
                    // 矩形右下角经度
                    double lonBottomRight = Double.valueOf(values[2]);
                    // 矩形右下角纬度
                    double latBottomRight = Double.valueOf(values[3]);
                    // 接口的左边是先纬度、再经度
                    boolQueryBuilder.must(QueryBuilders.geoBoundingBoxQuery(fieldName)
                            .setCorners(latTopLeft, lonTopLeft, latBottomRight, lonBottomRight));

                    // 遍历下1对括号
                    idx = value.indexOf(SYMBOL_BRACKET_LEFT, idx);
                }
            }
            // 矩形坐标并集
            else if(geoType.equals(STR_GEO_BOUNDING_BOX_UNION)){
                int idx = 0;
                while(idx >=0 && value.indexOf(SYMBOL_BRACKET_LEFT, idx) != -1){
                    /**
                     * 处理每对括号
                     */
                    idx = value.indexOf(SYMBOL_BRACKET_LEFT, idx);
                    String[] values = value.split(SYMBOL_COMMA);
                    // 矩形左上角经度
                    double lonTopLeft = Double.valueOf(values[0]);
                    // 矩形左上角纬度
                    double latTopLeft = Double.valueOf(values[1]);
                    // 矩形右下角经度
                    double lonBottomRight = Double.valueOf(values[2]);
                    // 矩形右下角纬度
                    double latBottomRight = Double.valueOf(values[3]);
                    // 接口的左边是先纬度、再经度
                    boolQueryBuilder.should(QueryBuilders.geoBoundingBoxQuery(fieldName)
                            .setCorners(latTopLeft, lonTopLeft, latBottomRight, lonBottomRight));

                    // 遍历下1对括号
                    idx = value.indexOf(SYMBOL_BRACKET_LEFT, idx);
                }
            }
        }

        return boolQueryBuilder;
    }

    /**
     * 添加限定字符串的匹配
     * @param boolQueryBuilder
     * @param valueLimit
     * @return
     */
    private BoolQueryBuilder addValueLimit(BoolQueryBuilder boolQueryBuilder, String valueLimit){
        // 限定字符串的匹配
        if(valueLimit != null && valueLimit.trim().length() > 0){
            valueLimit = valueLimit.trim();
            // 去空格
            valueLimit = valueLimit.replace(" ", "").trim();
            // 分号 分隔限定值，然后进行
            String[] argsLimits = removeDuplicates(valueLimit.split(";"));
            for(String fieldLimit: argsLimits){
                String fieldName = fieldLimit.substring(0, fieldLimit.indexOf(":"));
                String fieldValue = fieldLimit.substring(fieldLimit.indexOf(":") + 1);
                boolQueryBuilder.must(QueryBuilders.termQuery(fieldName, fieldValue));
            }
        }

        return boolQueryBuilder;
    }

    /**
     * 添加数值区间的限定
     * @param boolQueryBuilder
     * @param numericalRangeLimit
     * @return
     */
    private BoolQueryBuilder addNumericalRangeLimit(BoolQueryBuilder boolQueryBuilder, String numericalRangeLimit){
        // 添加数值区间的限定
        if(numericalRangeLimit != null && numericalRangeLimit.trim().length() > 0){
            numericalRangeLimit = numericalRangeLimit.trim();
            // 去除空格
            numericalRangeLimit = numericalRangeLimit.replace(" ", "");
            // 以分号分隔字符串，并进行遍历
            String[] argsLimits = removeDuplicates(numericalRangeLimit.split(";"));
            for(String limit: argsLimits){
                String fieldName = limit.substring(0, limit.indexOf(":"));
                // 范围限定明细
                String[] limitsDetail = removeDuplicates(limit.substring(limit.indexOf(":") + 1).split(","));
                for(String limitDetail: limitsDetail){
                    RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(fieldName);
                    if(limitDetail.contains(">=") == true){
                        rangeQueryBuilder.gte(limitDetail.substring(limitDetail.indexOf(">=") + 2));
                    }
                    else if(limitDetail.contains("<=") == true){
                        rangeQueryBuilder.lte(limitDetail.substring(limitDetail.indexOf("<=") + 2));
                    }
                    else if(limitDetail.contains(">") == true){
                        rangeQueryBuilder.gt(limitDetail.substring(limitDetail.indexOf(">") + 1));
                    }
                    else if(limitDetail.contains("<") == true){
                        rangeQueryBuilder.lt(limitDetail.substring(limitDetail.indexOf("<") + 1));
                    }
                    boolQueryBuilder.must(rangeQueryBuilder);
                }
            }
        }

        return boolQueryBuilder;
    }

    /**
     * 查询分组统计信息
     * @param text 待全文搜索的文本
     * @param indicesToQuery （可选）限定搜索的表（表之间以英文逗号分隔，表集最后不带括号。）<br/>
     *                       值参考： tab_01,tab_02   表示2张表
     * @param numericalRangeLimit （可选）数值区间限定字符串（字段之间以英文分号来分隔，限定字符串最后不带英文分号。值区间的符号只能是 >= > < <= 这4种） <br/>
     *                            值参考： field_01:>=1,<2; field_02:>100   表示字段 field_01 限定取大于等于1、小于2的值，字段 field_02 限定取值大于100的值
     * @param valueLimit （可选）限定字符串（字段之间以英文分号来分隔，同字段的值之间以逗号分隔；限定字符串最后不带英文分号。）<br/>
     *                   值参考： name:张三,李四; grade:一年级    表示搜索name为张三、李四，并且限定grade取一年级
     * @param fieldsToCountGroupBy （可选）用于count的字段，字段之间用英文逗号分隔
     * @param geoLimit （可选）坐标的筛选，搜索方式如下（字段名区分大小写）：  <br/>
     *                  中心圆的查询： geo_distance:field_name:11.11,22.22,33   field_name为字段名，11.11为中心坐标的经度，22.22为中心坐标的纬度，33是半径公里数； <br/>
     *                  中心圆的交集： geo_distance_intersection:field_name:(11.11,12.12,13),(21.21,22.22,23)   每对括号表示1个圆，可以有多个圆。  <br/>
     *                  中心圆的并集： geo_distance_union:field_name:(11.11,12.12,13),(21.21,22.22,23)   每对括号表示1个圆，可以有多个圆。  <br/>
     *                  矩形坐标查询： geo_bounding_box:field_name:11.11,22.22,33.33,44.44   field_name为字段名，11.11为矩形左上角坐标点的经度，22.22为矩形左上角坐标点的纬度，33.33,位矩形右下角坐标点的经度，44.44为矩形右下角坐标点的纬度；
     *                  矩形坐标交集： geo_bounding_box_intersection:field_name:(11.11,12.12,13.13,14.14),(21.21,22.22,23.23,24.24)   每对括号表示1个矩形，可以有多个矩形。  <br/>
     *                  矩形坐标并集： geo_bounding_box_union:field_name:(11.11,12.12,13.13,14.14),(21.21,22.22,23.23,24.24)   每对括号表示1个矩形，可以有多个矩形。  <br/>
     * @param analyzer （可选）文本分词器设置，目前可传入 ik_smart、ik_max_word 这2种特定分词，传入其他值或空字符串""则使用默认分词器
     * @return
     */
    public Map<String, Object> textCountGroup(String text,
                                              String indicesToQuery,
                                              String numericalRangeLimit, String valueLimit,
                                              String fieldsToCountGroupBy,
                                              String geoLimit,
                                              String analyzer){
        /* BoolQueryBuilder查询设置 */
        // 设置分词器
        setTextAnalyzer(analyzer);
        // 将查询结果转换为 BoolQueryBuilder 条件
        BoolQueryBuilder boolQueryBuilder = getBoolQueryBuilderFromText(text);
        // 将表名字符串转为数组
        String[] argsIndicesNames = getIndicesNames(indicesToQuery);

        // 添加数值区间的限定
        boolQueryBuilder = addNumericalRangeLimit(boolQueryBuilder, numericalRangeLimit);

        // 添加限定字符串的匹配
        boolQueryBuilder = addValueLimit(boolQueryBuilder, valueLimit);

        // 添加地图搜索设置
        boolQueryBuilder = addGeoLimit(boolQueryBuilder, geoLimit);

        // 分组统计设置
        SearchRequest dataCountRequest = null;
        if(fieldsToCountGroupBy != null && fieldsToCountGroupBy.trim().length() > 0){
            SearchSourceBuilder dataCountSourceBuilder = new SearchSourceBuilder().query(boolQueryBuilder);
            /* 设置超时 */
            dataCountSourceBuilder.timeout(TimeValue.timeValueMinutes(this.timeoutMinutes));
            // 将分组字符串转为数组，并进行条件限制的添加
            String[] fieldNamesToGroupBy = removeDuplicates(fieldsToCountGroupBy.replace(";", ",").split(","));
            for(String fieldNameToGroupBy: fieldNamesToGroupBy){
                TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("count").field(fieldNameToGroupBy);
                dataCountSourceBuilder.aggregation(termsAggregationBuilder);
            }
            /* 分组查询请求 */
            // 不返回实际数据，以加快数据分组的统计
            dataCountSourceBuilder.from(0).size(0);
            // 指定了要查询的表
            if(argsIndicesNames != null && argsIndicesNames.length > 0){
                dataCountRequest = new SearchRequest(argsIndicesNames).source(dataCountSourceBuilder);
            }
            else {
                dataCountRequest = new SearchRequest().source(dataCountSourceBuilder);
            }
        }

        /* 开始查询 */
        try {
            // 最后返回的数据
            Map<String, Object> data = new HashMap<>(16);

            /* 查询分组统计数据 */
            SearchResponse dataCountResponse = null;
            if(dataCountRequest != null){
                dataCountResponse = this.client.search(dataCountRequest, RequestOptions.DEFAULT);
                Map<String, Object> rowData = new HashMap<>(16);

                for(Terms.Bucket bucket: ((Terms) dataCountResponse.getAggregations().getAsMap().get(STR_FLAG_COUNT)).getBuckets()){
                    rowData.put(bucket.getKey().toString(), bucket.getDocCount());
                }
                data.put("counts", rowData);
                data.put("_took_millisecond", dataCountResponse.getTook().millis());
            }

            return data;
        }
        catch (Exception e){
            e.printStackTrace();
            logger.error("全文搜索字符串[ " + text + " ]失败！");
            return null;
        }
    }


    /**
     * 文本搜索
     * @param text 待全文搜索的文本
     * @param indicesToQuery （可选）限定搜索的表（表之间以英文逗号分隔，表集最后不带括号。）<br/>
     *                       值参考： tab_01,tab_02   表示2张表
     * @param numericalRangeLimit （可选）数值区间限定字符串（字段之间以英文分号来分隔，限定字符串最后不带英文分号。值区间的符号只能是 >= > < <= 这4种） <br/>
     *                            值参考： field_01:>=1,<2; field_02:>100   表示字段 field_01 限定取大于等于1、小于2的值，字段 field_02 限定取值大于100的值
     * @param valueLimit （可选）限定字符串（字段之间以英文分号来分隔，同字段的值之间以逗号分隔；限定字符串最后不带英文分号。）<br/>
     *                   值参考： name:张三,李四; grade:一年级    表示搜索name为张三、李四，并且限定grade取一年级
     * @param fieldsToHighlight （可选）用于高亮的字段，字段之间用英文逗号分隔。传入星号字符"*"表示匹配到的所有字符都高亮显示。
     * @param fieldsToOrderBy （可选）用于排序的字段，字段之间用英文逗号分隔。<br/>
     *                         值参考： id:DESC; orni:ASC; 其他值升序
     * @param geoLimit （可选）坐标的筛选，搜索方式如下（字段名区分大小写）：  <br/>
     *                 中心圆的查询： geo_distance:field_name:11.11,22.22,33   field_name为字段名，11.11为中心坐标的经度，22.22为中心坐标的纬度，33是半径公里数； <br/>
     *                 中心圆的交集： geo_distance_intersection:field_name:(11.11,12.12,13),(21.21,22.22,23)   每对括号表示1个圆，可以有多个圆。  <br/>
     *                 中心圆的并集： geo_distance_union:field_name:(11.11,12.12,13),(21.21,22.22,23)   每对括号表示1个圆，可以有多个圆。  <br/>
     *                 矩形坐标查询： geo_bounding_box:field_name:11.11,22.22,33.33,44.44   field_name为字段名，11.11为矩形左上角坐标点的经度，22.22为矩形左上角坐标点的纬度，33.33,位矩形右下角坐标点的经度，44.44为矩形右下角坐标点的纬度；
     *                 矩形坐标交集： geo_bounding_box_intersection:field_name:(11.11,12.12,13.13,14.14),(21.21,22.22,23.23,24.24)   每对括号表示1个矩形，可以有多个矩形。  <br/>
     *                 矩形坐标并集： geo_bounding_box_union:field_name:(11.11,12.12,13.13,14.14),(21.21,22.22,23.23,24.24)   每对括号表示1个矩形，可以有多个矩形。  <br/>
     * @param analyzer （可选）文本分词器设置，目前可传入 ik_smart、ik_max_word 这2种特定分词，传入其他值或空字符串""则使用默认分词器
     * @param rowNumStart 分页查询的开始行号（从0开始，值-1表示不分页）
     * @param rowsSize 每次获取的数量（值-1表示不限制数量）
     * @return 查询失败返回null
     */
    public JSONObject textSearch(String text,
                                 String indicesToQuery,
                                 String numericalRangeLimit, String valueLimit,
                                 String fieldsToHighlight,
                                 String fieldsToOrderBy,
                                 String geoLimit,
                                 String analyzer,
                                 int rowNumStart, int rowsSize){
        // 设置分词器
        setTextAnalyzer(analyzer);

        // 将查询结果转换为 BoolQueryBuilder 条件
        BoolQueryBuilder boolQueryBuilder = getBoolQueryBuilderFromText(text);

        // 将表名字符串转为数组
        String[] argsIndicesNames = null;
        if(indicesToQuery != null && indicesToQuery.trim().length() > 0){
            // 去除空格
            indicesToQuery = indicesToQuery.replace(" ", "");
            // 将字符";"替换为字符","，以防输入错误
            indicesToQuery = indicesToQuery.replace(";", ",");
            String[] argsSplited = indicesToQuery.split(",");
            if(argsSplited.length > 0){
                argsIndicesNames = argsSplited;
            }
        }

        // 添加数值区间的限定
        boolQueryBuilder = addNumericalRangeLimit(boolQueryBuilder, numericalRangeLimit);

        // 添加限定字符串的匹配
        boolQueryBuilder = addValueLimit(boolQueryBuilder, valueLimit);

        // 添加地图搜索设置
        boolQueryBuilder = addGeoLimit(boolQueryBuilder, geoLimit);

        /**
         * 数据查询设置
         */
        SearchSourceBuilder dataSearchSourceBuilder = new SearchSourceBuilder().query(boolQueryBuilder);
        // 超时设置
        dataSearchSourceBuilder.timeout(TimeValue.timeValueMinutes(this.timeoutMinutes));

        SearchRequest dataSearchRequest;

        // 分页设置
        if(rowNumStart >= 0){
            dataSearchSourceBuilder.from(rowNumStart);
        }
        if(rowsSize >= 0){
            dataSearchSourceBuilder.size(rowsSize);
        }

        // 排序设置，将排序字符串转为map键值对
        if(fieldsToOrderBy != null && fieldsToOrderBy.trim().length() > 0){
            // 替换空格
            fieldsToOrderBy = fieldsToOrderBy.replace(" ", "");
            // 替换英文分好为逗号
            fieldsToOrderBy = fieldsToOrderBy.replace(";", ",");
            String[] argsFieldsOrder = removeDuplicates(fieldsToOrderBy.split(","));
            for(String fieldOrder: argsFieldsOrder){
                String fieldName = fieldOrder.substring(0, fieldOrder.indexOf(":"));
                String orderType = fieldOrder.substring(fieldOrder.indexOf(":") + 1);
                // 默认升序
                SortOrder sortOrder = SortOrder.ASC;
                if("DESC".equals(orderType.toUpperCase())){
                    sortOrder = SortOrder.DESC;
                }
                dataSearchSourceBuilder.sort(fieldName, sortOrder);
            }
        }
        else{
            // 默认已 _score 得分字段降序排列
            dataSearchSourceBuilder.sort("_score", SortOrder.DESC);
        }

        // 高亮设置
        if(fieldsToHighlight != null && fieldsToHighlight.trim().length() > 0){
            // 去除空格
            fieldsToHighlight = fieldsToHighlight.replace(" ", "");
            // 将字符";"替换为字符","，以防输入错误
            indicesToQuery = indicesToQuery.replace(";", ",");
            HighlightBuilder highlightBuilder = new HighlightBuilder().preTags("<highlight>").postTags("</highlight>");
            String[] fieldNames = removeDuplicates(fieldsToHighlight.split(","));
            if(SYMBOL_STAR.equals(fieldNames[0].trim())){
                highlightBuilder.field(SYMBOL_STAR);
            }
            else {
                for (String fieldName : fieldNames) {
                    highlightBuilder.field(fieldName, 200);
                }
            }
            dataSearchSourceBuilder.highlighter(highlightBuilder);
        }

        // 指定了要查询的表
        if(argsIndicesNames != null && argsIndicesNames.length > 0){
            dataSearchRequest = new SearchRequest(argsIndicesNames).source(dataSearchSourceBuilder);
        }
        else {
            dataSearchRequest = new SearchRequest().source(dataSearchSourceBuilder);
        }

        /* 开始查询 */
        try {
            // 最后返回的数据
            JSONObject data = new JSONObject();

            /* 查询明细数据 */
            SearchResponse dataSearchResponse = this.client.search(dataSearchRequest, RequestOptions.DEFAULT);
            // 获取命中数（ES默认设置上限为1万条，如有必要，需要在配置文件里进行更改）
            data.put("_hits", dataSearchResponse.getHits().getTotalHits().value);
            // 查询明细数据的分片数
            data.put("_shards_total", dataSearchResponse.getTotalShards());
            data.put("_shards_successful", dataSearchResponse.getSuccessfulShards());
            data.put("_shards_failed", dataSearchResponse.getFailedShards());
            data.put("_shards_skipped", dataSearchResponse.getSkippedShards());
            JSONArray rowsData = new JSONArray();
            // 遍历明细数据
            dataSearchResponse.getHits().forEach(e ->{
                // 每行的数据
                JSONObject rowData = new JSONObject();
                // 得分
                rowData.put("_score", e.getScore());
                // 索引名
                rowData.put("_index", e.getIndex());
                // 文档id
                rowData.put("_id", e.getId());

                // 若设置了高亮字段，则取高亮的字段数据
                List<String> highlightFieldsList = new LinkedList<>();
                Map<String, HighlightField> highlightFieldMap = e.getHighlightFields();
                if(highlightFieldMap.size() > 0){
                    for(Map.Entry<String, HighlightField> entry: highlightFieldMap.entrySet()){
                        String fieldName = entry.getKey().toString();
                        String fieldValue = entry.getValue().fragments()[0].toString();
                        if (fieldValue == null || fieldValue.trim().length() == 0){
                            continue;
                        }

                        // 保存高亮的值
                        rowData.put(fieldName, fieldValue);
                        // 保存高亮的字段名
                        highlightFieldsList.add(fieldName);
                    }
                }
                // 保存未设高亮的字段数据
                for(Map.Entry<String, Object> sourceMap: e.getSourceAsMap().entrySet()){
                    try {
                        String fieldName = sourceMap.getKey().toString();
                        String fieldValue = sourceMap.getValue().toString();
                        if (highlightFieldsList.indexOf(fieldName) == -1 && fieldValue != null && fieldValue.trim().length() > 0) {
                            rowData.put(fieldName, fieldValue);
                        }
                    }
                    catch (Exception e1){}
                }
                rowsData.add(rowData);
            });
            data.put("data", rowsData);

            /* 查询耗时（秒） */
            // 查询数据的耗时
            long secondsTookOnData = dataSearchResponse.getTook().getMillis();
            // 查询分组的耗时
            data.put("_took_millisecond", secondsTookOnData);

            return data;
        }
        catch (Exception e){
            e.printStackTrace();
            logger.error("全文搜索字符串[ " + text + " ]失败！");
            return null;
        }
    }

    /**
     * 根据首部文字自动建议补全
     * @param indexName 索引名
     * @param fieldName 字段名
     * @param text 首部文字
     * @param maxListLength 建议补全返回的最大文本数
     * @return 建议补全的文本列表
     */
    public List<String> textCompletionSuggester(String indexName, String fieldName, String text, int maxListLength){
        // 返回的列表
        List<String> result = new LinkedList<>();

        // 创建search请求
        SearchRequest searchRequest = new SearchRequest(indexName);
        // 用SearchSourceBuilder来构造查询请求体
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        SuggestionBuilder termSuggestionBuilder = SuggestBuilders.completionSuggestion(fieldName).prefix(text).skipDuplicates(true).size(maxListLength);
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        // 自定义名称
        String suggestionName = "one_suggestion";
        suggestBuilder.addSuggestion(suggestionName, termSuggestionBuilder);
        searchSourceBuilder.suggest(suggestBuilder);
        searchSourceBuilder.sort("_score", SortOrder.DESC);
        searchRequest.source(searchSourceBuilder);

        try {
            SearchResponse searchResponse = this.client.search(searchRequest, RequestOptions.DEFAULT);
            // 搜索结果状态信息
            if(RestStatus.OK.equals(searchResponse.status())){
                Suggest suggest = searchResponse.getSuggest();
                CompletionSuggestion completionSuggestion = suggest.getSuggestion(suggestionName);
                // 遍历请求结果
                for(CompletionSuggestion.Entry entry: completionSuggestion.getEntries()){
                    for(CompletionSuggestion.Entry.Option option: entry){
                        String suggestText = option.getText().string();
                        result.add(suggestText);
                    }
                }
            }
        }
        catch (Exception e){
            e.printStackTrace();
            return null;
        }

        return result;
    }

    /**
     * 存储搜索记录，以作推荐用
     * @param indexName 索引名
     * @param fieldName 字段名
     * @param fieldValue 字段值
     * @return 0:成功； -1:失败；
     */
    public int textCompletionSuggesterPutter(String indexName, String fieldName, String fieldValue){
        Map<String, String> data = new HashMap<>(16);
        data.put(fieldName, fieldValue);

        // 添加文档
        IndexRequest indexRequest = new IndexRequest(indexName).source(data);
        try {
            this.client.index(indexRequest, RequestOptions.DEFAULT);
            return 0;
        }
        catch (Exception e){
            e.printStackTrace();
            return -1;
        }
    }
}

/**
 * 树节点类，用于将搜索字符串拆分成细项
 */
class TreeNode{
    /**
     * 节点操作符：& 与, | 或, - 差集
     */
    String operator;
    /**
     * 节点文本
     */
    String text;
    /**
     * 子节点
     */
    List<TreeNode> subTreeNodeList = new LinkedList<>();
}