package gov.ita.dataloader.ingest.configuration;

import gov.ita.dataloader.security.AuthenticationFacade;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.stream.Collectors;

@RestController
@Profile("production")
public class ProductionBusinessUnitController {

  @Autowired
  private AuthenticationFacade authenticationFacade;

  @Autowired
  private BusinessUnitService businessUnitService;

  @GetMapping(value = "/api/business-units", produces = MediaType.APPLICATION_JSON_VALUE)
  public List<BusinessUnit> getBusinessUnits() throws Exception {
    List<BusinessUnit> businessUnits = businessUnitService.getBusinessUnits();
    String user = authenticationFacade.getUserName().toLowerCase();

    boolean isAdmin = businessUnits.stream().filter(businessUnit -> businessUnit.users.contains(user)).count() > 0;
    if (isAdmin) {
      return businessUnits;
    } else {
      return businessUnits.stream()
        .filter(businessUnit -> businessUnit.getUsers().contains(user))
        .collect(Collectors.toList());
    }
  }

  @GetMapping(value = "/api/storage-containers", produces = MediaType.APPLICATION_JSON_VALUE)
  public List<String> getStorageContainers() throws Exception {
    return businessUnitService.getStorageContainers();
  }

}
