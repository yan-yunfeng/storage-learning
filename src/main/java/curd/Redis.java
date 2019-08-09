package curd;

import org.springframework.data.redis.core.StringRedisTemplate;

import connector.Connectors;

/**
 * author Yan YunFeng  Email:twd.wuyun@163.com
 * create 19-8-9 下午3:01
 */
public class Redis {

    public static void main(String[] args) {

        StringRedisTemplate stringRedisTemplate = Connectors.getStringRedisTemplate(0);
        //删
        stringRedisTemplate.delete("key");

        //查

        //String
        stringRedisTemplate.opsForValue().get("key");

        //map
        stringRedisTemplate.opsForHash().entries("key");

        //list
        stringRedisTemplate.opsForList().range("key",0,10);

        //set
        stringRedisTemplate.opsForSet().members("key");

    }
}
