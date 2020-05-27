package gov.ita.dataloader.ingest.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import gov.ita.dataloader.storage.Storage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

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
    DataloaderAdminConfiguration dataloaderAdminConfiguration = objectMapper.readValue(dataloaderConfig, DataloaderAdminConfiguration.class);
    return dataloaderAdminConfiguration.getBusinessUnits();
  }

  public List<String> getStorageContainers() throws Exception {
    return getBusinessUnits().stream().map(BusinessUnit::getContainerName).collect(Collectors.toList());
  }
}