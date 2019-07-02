package org.jeecg.config;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * @author zcd
 * @date 2019-07-02 18:06
 */
@Component
public class MailHealthIndicator implements HealthIndicator
{
    @Override
    public Health health() {
        int errorCode = check();
        if (errorCode != 0) {
            return Health.down().withDetail("Error Code", errorCode)  .build();
        }
        return Health.up().build();
    }

    int check(){
        //可以实现自定义的数据库检测逻辑
        return 0;
    }
}