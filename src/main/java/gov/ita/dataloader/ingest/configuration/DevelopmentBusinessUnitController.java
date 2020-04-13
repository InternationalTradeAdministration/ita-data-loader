package gov.ita.dataloader.ingest.configuration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@Profile({"development", "staging"})
public class DevelopmentBusinessUnitController {

  @Autowired
  private BusinessUnitService businessUnitService;

  @GetMapping(value = "/api/business-units", produces = MediaType.APPLICATION_JSON_VALUE)
  public List<BusinessUnit> getBusinessUnits() throws Exception {
    return businessUnitService.getBusinessUnits();
  }

  @GetMapping(value = "/api/storage-containers", produces = MediaType.APPLICATION_JSON_VALUE)
  public List<String> getStorageContainers() throws Exception {
    return businessUnitService.getStorageContainers();
  }

}
