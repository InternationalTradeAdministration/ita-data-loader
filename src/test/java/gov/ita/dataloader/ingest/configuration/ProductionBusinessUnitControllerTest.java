package gov.ita.dataloader.ingest.configuration;

import gov.ita.dataloader.security.AuthenticationFacade;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ProductionBusinessUnitControllerTest {

  @Mock
  private AuthenticationFacade authenticationFacade;

  @Mock
  private BusinessUnitService businessUnitService;

  private List<String> users = new ArrayList<>();
  private List<BusinessUnit> businessUnits = new ArrayList<>();

  @Before
  public void setUp() throws Exception {
    businessUnits.add(new BusinessUnit("The Matrix", "cool", users));
    businessUnits.add(new BusinessUnit("Speed", "neo", Collections.emptyList()));
    when(businessUnitService.getBusinessUnits()).thenReturn(businessUnits);
    when(authenticationFacade.getUserName()).thenReturn("john.wick@trade.gov");
  }

  @Test
  public void applies_uac_when_retrieving_business_units_denies_access() throws Exception {
    ProductionBusinessUnitController productionBusinessUnitController = new ProductionBusinessUnitController(authenticationFacade, businessUnitService);
    List<BusinessUnit> results = productionBusinessUnitController.getBusinessUnits();
    assertEquals(0, results.size());
  }

  @Test
  public void applies_uac_when_retrieving_business_units_grants_access() throws Exception {
    users.add("john.wick@trade.gov");
    ProductionBusinessUnitController productionBusinessUnitController = new ProductionBusinessUnitController(authenticationFacade, businessUnitService);
    List<BusinessUnit> results = productionBusinessUnitController.getBusinessUnits();
    assertEquals(1, results.size());
    assertEquals("john.wick@trade.gov", results.get(0).users.get(0));
  }

  @Test
  public void dataloader_admin_is_granted_access_to_all_business_units() throws Exception {
    users.add("john.wick@trade.gov");
    businessUnits.add(new BusinessUnit("Dataloader", "dataloader", users));
    ProductionBusinessUnitController productionBusinessUnitController = new ProductionBusinessUnitController(authenticationFacade, businessUnitService);
    List<BusinessUnit> results = productionBusinessUnitController.getBusinessUnits();
    assertEquals(3, results.size());
    assertEquals("cool", results.get(0).getContainerName());
    assertEquals("neo", results.get(1).getContainerName());
    assertEquals("dataloader", results.get(2).getContainerName());
  }

  @Test
  public void when_no_admin_exists_all_business_units_are_returned() throws Exception {
    businessUnits.add(new BusinessUnit("Dataloader", "dataloader", Collections.emptyList()));
    ProductionBusinessUnitController productionBusinessUnitController = new ProductionBusinessUnitController(authenticationFacade, businessUnitService);
    List<BusinessUnit> results = productionBusinessUnitController.getBusinessUnits();
    assertEquals(3, results.size());
    assertEquals("cool", results.get(0).getContainerName());
    assertEquals("neo", results.get(1).getContainerName());
    assertEquals("dataloader", results.get(2).getContainerName());
  }

}