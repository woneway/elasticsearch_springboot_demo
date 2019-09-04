package com.woneway.elasticsearch.config;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import java.net.InetAddress;
import java.net.UnknownHostException;

@Configuration
@PropertySource(value = "classpath:config/elasticsearch.properties")
public class ElasticSearchConfig {

    @Value("${es.hostName}")
    private String hostName;

    @Value("${es.master.transport}")
    private Integer masterPort;

    @Value("${es.slave1.transport}")
    private Integer slave1Port;

    @Value("${es.slave2.transport}")
    private Integer slave2Port;

    @Value("${es.cluster.name}")
    private String clusterName;

    private Logger logger = LoggerFactory.getLogger(ElasticSearchConfig.class);

    @Bean
    public TransportClient client() {
        logger.info("start config transport client.");
        TransportClient client = null;

        Settings settings = Settings.builder()
                .put("cluster.name", clusterName)
                .build();

        client = new PreBuiltTransportClient(settings);
        TransportAddress master = null, slave1 = null, slave2 = null;
        try {
            master = new InetSocketTransportAddress(InetAddress.getByName(hostName), masterPort);
            slave1 = new InetSocketTransportAddress(InetAddress.getByName(hostName), slave1Port);
            slave2 = new InetSocketTransportAddress(InetAddress.getByName(hostName), slave2Port);

            client.addTransportAddress(master);
            client.addTransportAddress(slave1);
            client.addTransportAddress(slave2);
        } catch (UnknownHostException e) {
            logger.error("config transport client failed.", e);
        }
        logger.info("finish config transport client.");
        return client;
    }
}
