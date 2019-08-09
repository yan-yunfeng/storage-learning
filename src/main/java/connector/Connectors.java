package connector;

import com.alibaba.druid.pool.DruidDataSource;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisClientConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * author Yan YunFeng  Email:twd.wuyun@163.com
 * create 19-8-9 下午2:21
 */
public class Connectors {

    private static final String ES_NODES = "localhost:9200";
    private static final String ES_USERNAME = "username";
    private static final String ES_PASSWORD = "password";
    private static final String MONGO_URI = "mongodb://username:password@localhost:27701";
    private static final String REDIS_HOST = "localhosst";
    private static final int REDIS_PORT = 6379;
    private static final String REDIS_PASSWORD = "password";
    private static final String KAFKA_URL = "localhost:6667";
    private static final String MYSQL_URI = "jdbc:mysql://localhost:3306/database";
    private static final String MYSQL_USERNAME = "username";
    private static final String MYSQL_PASSWORD = "password";

    private static RestClient restClient;
    private static MongoClient mongoClient;
    private static Map<String, StringRedisTemplate> stringRedisTemplateMap = new ConcurrentHashMap<>();
    private static Map<String, KafkaConsumer<String,String>> kafkaConsumerMap = new ConcurrentHashMap<>();
    private static KafkaProducer<String, String> kafkaProducer;
    private static DruidDataSource source;




    /**
     * 获取MongoClient，之后可以根据需求获取DB和collection
     *
     * @return MongoClient
     */
    public static MongoClient getMongoClient() {
        if (mongoClient == null) {
            synchronized (Connectors.class) {
                if (mongoClient == null) {
                    mongoClient = new MongoClient(new MongoClientURI(MONGO_URI));
                }
            }
        }
        return mongoClient;
    }

    /**
     * 获取ES的RestClient
     *
     * @return RestClient
     */
    public static RestClient getRestClient() {
        if (restClient == null) {
            synchronized (Connectors.class) {
                if (restClient == null) {
                    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(ES_USERNAME, ES_PASSWORD));
                    restClient = RestClient.builder(Stream.of(ES_NODES.split(","))
                                                          .map(host -> new HttpHost(host.split(":")[0], Integer.parseInt(host.split(":")[1])))
                                                          .toArray(HttpHost[]::new))
                                           .setRequestConfigCallback(requestConfigBuilder -> {
                                               requestConfigBuilder.setConnectTimeout(5000);
                                               requestConfigBuilder.setSocketTimeout(40000);
                                               requestConfigBuilder.setConnectionRequestTimeout(1000);
                                               return requestConfigBuilder;
                                           })
                                           .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                                               @Override
                                               public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                                                   return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                                               }
                                           })
                                           .setMaxRetryTimeoutMillis(2 * 60 * 1000)
                                           .build();

                }
            }
        }
        return restClient;
    }

    /**
     * 获取单台redis的StringRedisTemplate
     *
     * @return StringRedisTemplate
     */
    public static StringRedisTemplate getStringRedisTemplate(int database) {
        String key = "" + database;
        //如下为集群设置
//        RedisClusterConfiguration redisClusterConfiguration = new RedisClusterConfiguration();
        //如下为哨兵设置
//        RedisSentinelConfiguration redisSentinelConfiguration = new RedisSentinelConfiguration();
        //如下为单台设置
        if (stringRedisTemplateMap.get(key) == null) {
            synchronized (Connectors.class) {
                if (stringRedisTemplateMap.get(key) == null) {
                    RedisStandaloneConfiguration redisStandaloneConfiguration = new RedisStandaloneConfiguration();
                    redisStandaloneConfiguration.setHostName(REDIS_HOST);
                    redisStandaloneConfiguration.setPort(REDIS_PORT);
                    redisStandaloneConfiguration.setPassword(RedisPassword.of(REDIS_PASSWORD));
                    redisStandaloneConfiguration.setDatabase(database);

                    JedisClientConfiguration.JedisClientConfigurationBuilder jedisClientConfigurationBuilder = JedisClientConfiguration.builder();
                    jedisClientConfigurationBuilder.connectTimeout(Duration.ofMillis(100000L));
                    jedisClientConfigurationBuilder.readTimeout(Duration.ofMillis(100000L));

                    RedisConnectionFactory factory = new JedisConnectionFactory(redisStandaloneConfiguration, jedisClientConfigurationBuilder.build());
                    RedisSerializer<String> stringRedisSerializer = new StringRedisSerializer();
                    StringRedisTemplate stringRedisTemplate = new StringRedisTemplate();
                    stringRedisTemplate.setConnectionFactory(factory);
                    stringRedisTemplate.setValueSerializer(stringRedisSerializer);
                    stringRedisTemplate.setHashValueSerializer(stringRedisSerializer);
                    stringRedisTemplate.setKeySerializer(stringRedisSerializer);
                    stringRedisTemplate.setHashKeySerializer(stringRedisSerializer);
                    stringRedisTemplate.afterPropertiesSet();
                    stringRedisTemplateMap.put(key, stringRedisTemplate);
                }
            }
        }
        return stringRedisTemplateMap.get(key);
    }

    /**
     * 获取kafka消费者
     *
     * @return KafkaConsumer<String                                                                                                                                                                                                                                                               ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               String>
     */
    public static KafkaConsumer<String, String> getKafkaConsumer(String topic, String groupId) {
        String key = topic + "|" + groupId;
        if (kafkaConsumerMap.get(key) == null) {
            synchronized (Connectors.class) {
                if (kafkaConsumerMap.get(key) == null) {
                    Properties properties = new Properties();
                    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_URL);
                    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
                    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
                    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);//设置偏移量自动提交。
                    properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 60 * 1000 * 5);
                    properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 60 * 1000 * 5);
                    properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60 * 1000 * 10);

                    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
                    consumer.subscribe(Arrays.asList(topic));
                    kafkaConsumerMap.put(key, consumer);
                }
            }
        }

        return kafkaConsumerMap.get(key);
    }

    /**
     * 获取kafka生产者
     *
     * @return KafkaProducer<String, String>
     */
    public static KafkaProducer<String, String> getKafkaProducer() {
        if (kafkaProducer == null) {
            synchronized (Connectors.class) {
                if (kafkaProducer == null) {
                    Properties properties = new Properties();
                    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_URL);
                    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                    kafkaProducer = new KafkaProducer<>(properties);
                }
            }
        }

        return kafkaProducer;
    }

    public static DruidDataSource getSource(){
        if ( source ==null){
            synchronized (Connectors.class){
                if (source == null){
                    source = new DruidDataSource();
                    source.setDriverClassName("com.mysql.jdbc.Driver");
                    source.setUrl(MYSQL_URI);
                    source.setUsername(MYSQL_USERNAME);
                    source.setPassword(MYSQL_PASSWORD);
                }
            }
        }
        return source;
    }

}
