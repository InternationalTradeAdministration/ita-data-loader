package gov.ita.dataloader.version;

import gov.ita.dataloader.configuration.BuildConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class VersionController {

  @Autowired
  private BuildConfiguration configuration;

  @GetMapping("/api/version")
  public String getVersion() {
    return "v1.0.0_beta_" + configuration.getBuildId();
  }
}
