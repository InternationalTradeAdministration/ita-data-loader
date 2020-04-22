package gov.ita.dataloader.ingest.configuration;

import gov.ita.dataloader.security.AuthenticationFacade;
import org.springframework.context.annotation.Profile;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.stream.Collectors;

@RestController
@Profile("production")
public class ProductionBusinessUnitController {

  private AuthenticationFacade authenticationFacade;

  private BusinessUnitService businessUnitService;

  public ProductionBusinessUnitController(AuthenticationFacade authenticationFacade,
                                          BusinessUnitService businessUnitService) {
    this.authenticationFacade = authenticationFacade;
    this.businessUnitService = businessUnitService;
  }

  @GetMapping(value = "/api/business-units", produces = MediaType.APPLICATION_JSON_VALUE)
  public List<BusinessUnit> getBusinessUnits() throws Exception {
    List<BusinessUnit> businessUnits = businessUnitService.getBusinessUnits();
    String user = authenticationFacade.getUserName().toLowerCase();

    boolean noAdminsExist = businessUnits.stream().anyMatch(BusinessUnit::isAdminEmpty);
    boolean userIsAdmin = businessUnits.stream().anyMatch(businessUnit -> businessUnit.isAdmin(user));

    if (userIsAdmin || noAdminsExist) {
      return businessUnits;
    } else {
      return businessUnits.stream().filter(businessUnit -> businessUnit.includes(user)).collect(Collectors.toList());
    }
  }

  @GetMapping(value = "/api/storage-containers", produces = MediaType.APPLICATION_JSON_VALUE)
  public List<String> getStorageContainers() throws Exception {
    return businessUnitService.getStorageContainers();
  }

}
