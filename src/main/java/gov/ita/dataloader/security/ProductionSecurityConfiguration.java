package gov.ita.dataloader.security;

import com.azure.spring.aad.webapp.AADWebSecurityConfigurerAdapter;
import org.springframework.context.annotation.Profile;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;

@EnableWebSecurity
@EnableGlobalMethodSecurity(prePostEnabled = true)
@Profile("production")
public class ProductionSecurityConfiguration extends AADWebSecurityConfigurerAdapter {

  @Override
  protected void configure(HttpSecurity http) throws Exception {
    super.configure(http);
    http.csrf().disable()
      .authorizeRequests()
      .anyRequest().authenticated();
  }

}
