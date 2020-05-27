package gov.ita.dataloader.ingest.configuration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import gov.ita.dataloader.security.AuthenticationFacade;
import gov.ita.dataloader.storage.Storage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping(value = "/api/configuration", produces = MediaType.APPLICATION_JSON_VALUE)
public class ConfigurationController {

  @Autowired
  private Storage storage;

  @Autowired
  private ObjectMapper objectMapper;

  @Autowired
  private AuthenticationFacade authenticationFacade;

  @GetMapping("/automated-ingest")
  private byte[] getAutomatedIngestConfiguration(@RequestParam("containerName") String containerName) {
    return storage.getBlob(containerName, "configuration.json");
  }

  @GetMapping("/dataloader-admin")
  private byte[] getDataloaderAdminConfiguration() {
    return storage.getBlob("dataloader", "configuration.json");
  }

  @PutMapping("/dataloader-admin")
  public void saveAdminConfiguration(@RequestBody DataloaderAdminConfiguration dataloaderAdminConfiguration) throws JsonProcessingException {
    log.info("Saving Dataloader ADMIN configuration.json");
    byte[] dataSetConfigsJsonBytes = objectMapper.writeValueAsString(dataloaderAdminConfiguration).getBytes();
    storage.save("configuration.json", dataSetConfigsJsonBytes, authenticationFacade.getUserName(), "dataloader", true, false);
    storage.makeSnapshot("dataloader", "configuration.json");
  }

  @PutMapping("/automated-ingest")
  public void saveAutomatedIngestConfiguration(@RequestBody AutomatedIngestConfiguration automatedIngestConfiguration,
                                               @RequestParam("containerName") String containerName) throws JsonProcessingException {
    log.info("Saving {} configuration.json", containerName);
    byte[] dataSetConfigsJsonBytes = objectMapper.writeValueAsString(automatedIngestConfiguration).getBytes();
    storage.save("configuration.json", dataSetConfigsJsonBytes, authenticationFacade.getUserName(), containerName, true, false);
    storage.makeSnapshot(containerName, "configuration.json");
  }

}
