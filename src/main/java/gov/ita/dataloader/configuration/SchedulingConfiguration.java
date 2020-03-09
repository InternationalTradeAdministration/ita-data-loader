package gov.ita.dataloader.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.time.OffsetDateTime;

@Configuration
@EnableScheduling
public class SchedulingConfiguration {

  @Bean
  OffsetDateTime now() {
    return OffsetDateTime.now();
  }
}

