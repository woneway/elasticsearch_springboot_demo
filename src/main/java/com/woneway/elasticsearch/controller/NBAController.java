package com.woneway.elasticsearch.controller;

import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/nba")
public class NBAController {

    private Logger logger = LoggerFactory.getLogger(NBAController.class);

    @Autowired
    private TransportClient client;

    private String index = "nba";

    private String type = "player";

    //1.新增记录
    @RequestMapping("/add")
    public ResponseEntity add(@RequestBody JSONObject req) {
        Map<String, Object> map = new HashMap<>();
        map.put("name", req.getString("name"));
        map.put("age", req.getIntValue("age"));
        map.put("position", req.getString("position"));
        IndexRequestBuilder builder = client.prepareIndex(index, type, "1");
        builder.setSource(map);
        IndexResponse indexResponse = builder.execute().actionGet();
        return new ResponseEntity(indexResponse.getResult(), HttpStatus.OK);
    }

    //2.更新记录
    @RequestMapping("/update")
    public ResponseEntity update(@RequestBody JSONObject req) {
        UpdateRequest updateDoc = new UpdateRequest(index, type, req.getString("id"));
        Map<String, Object> map = new HashMap<>();
        if (!StringUtils.isEmpty(req.getString("name"))) {
            map.put("name", req.getString("name"));
        }

        if (!StringUtils.isEmpty(req.getString("age"))) {
            map.put("age", req.getIntValue("age"));
        }

        if (!StringUtils.isEmpty(req.getString("position"))) {
            map.put("position", req.getString("position"));
        }
        updateDoc.doc(map);
        UpdateResponse updateResponse = client.update(updateDoc).actionGet();
        return new ResponseEntity(updateResponse.getResult(), HttpStatus.OK);
    }

    //3.查询记录
    @RequestMapping("/query")
    public ResponseEntity query(@RequestBody JSONObject req) {
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();

        if (!StringUtils.isEmpty(req.getString("name"))) {
            queryBuilder.should(QueryBuilders.termQuery("name", req.getString("name")));
        }

        if (!StringUtils.isEmpty(req.getString("age"))) {
            queryBuilder.should(QueryBuilders.termQuery("age", req.getIntValue("age")));
        }

        if (!StringUtils.isEmpty(req.getString("position"))) {
            queryBuilder.should(QueryBuilders.termQuery("position.keyword", req.getString("position")));
        }

        SearchResponse searchResponse = client.prepareSearch(index)
                .setTypes(type)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(queryBuilder).execute().actionGet();

        SearchHit[] hits = searchResponse.getHits().getHits();
        List<Map<String,Object>> list = new ArrayList<>();

        if (searchResponse.getHits().getTotalHits() > 0) {
            for (SearchHit hitFields : hits) {
                list.add(hitFields.getSource());
            }
            return new ResponseEntity(list, HttpStatus.OK);
        } else {
            return new ResponseEntity(list,HttpStatus.OK);
        }
    }

    //4.删除记录
    @RequestMapping("/delete")
    public ResponseEntity delete(@RequestBody JSONObject req) {
        DeleteRequestBuilder builder = client.prepareDelete(index, type, req.getString("id"));
        DeleteResponse deleteResponse = builder.get();
        return new ResponseEntity(deleteResponse.getResult(), HttpStatus.OK);
    }


    //5.根据id进行查询
    @RequestMapping("/queryById")
    public ResponseEntity queryById(@RequestBody JSONObject req) {
        GetRequestBuilder builder = client.prepareGet(index, type, req.getString("id"));
        GetResponse getResponse = builder.execute().actionGet();
        return new ResponseEntity(getResponse.getSource(),HttpStatus.OK);
    }

}
