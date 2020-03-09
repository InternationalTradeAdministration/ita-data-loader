package gov.ita.dataloader.business_unit;

import com.fasterxml.jackson.databind.ObjectMapper;
import gov.ita.dataloader.storage.Storage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.GetMapping;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class BusinessUnitService {

  @Autowired
  private Storage storage;

  @Autowired
  private ObjectMapper objectMapper;

  public List<BusinessUnit> getBusinessUnits() throws Exception {
    byte[] dataloaderConfig = storage.getBlob("dataloader", "configuration.json");
    BusinessUnitConfigResponse buc = objectMapper.readValue(dataloaderConfig, BusinessUnitConfigResponse.class);
    return buc.getBusinessUnits();
  }

  @GetMapping(value = "/api/storage-containers", produces = MediaType.APPLICATION_JSON_VALUE)
  public List<String> getStorageContainers() throws Exception {
    return getBusinessUnits().stream().map(BusinessUnit::getContainerName).collect(Collectors.toList());
  }
}
