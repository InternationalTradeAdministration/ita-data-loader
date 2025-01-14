package gov.ita.dataloader.configuration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;

@Configuration
@PropertySource("classpath:build.properties")
public class BuildConfiguration {

    @Autowired
    private Environment env;

    public String getBuildId() {
        return env.getProperty("build.id");
    }
}
