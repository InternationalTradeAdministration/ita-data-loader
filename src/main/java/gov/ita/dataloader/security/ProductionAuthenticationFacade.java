package gov.ita.dataloader.security;

import org.springframework.context.annotation.Profile;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.core.oidc.user.DefaultOidcUser;
import org.springframework.stereotype.Service;

@Service
@Profile("production")
public class ProductionAuthenticationFacade implements AuthenticationFacade {

  @Override
  public String getUserName() {
    return ((DefaultOidcUser) SecurityContextHolder.getContext().getAuthentication()
      .getPrincipal()).getAttributes().get("preferred_username").toString();
  }

}

