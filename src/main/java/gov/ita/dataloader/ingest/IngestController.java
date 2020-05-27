package gov.ita.dataloader.ingest;

import gov.ita.dataloader.ingest.configuration.AutomatedIngestConfiguration;
import gov.ita.dataloader.security.AuthenticationFacade;
import gov.ita.dataloader.storage.Storage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Slf4j
@RestController
@RequestMapping(value = "/api/ingest", produces = MediaType.APPLICATION_JSON_VALUE)
public class IngestController {

  private Storage storage;
  private AutomatedIngestProcessor automatedIngestProcessor;
  private AuthenticationFacade authenticationFacade;
  private ManualIngestProcessor manualIngestProcessor;
  private Map<String, CompletableFuture> automatedIngestProcesses = new HashMap<>();
  private Map<String, CompletableFuture> manualIngestProcesses = new HashMap<>();

  public IngestController(Storage storage,
                          AutomatedIngestProcessor automatedIngestProcessor,
                          AuthenticationFacade authenticationFacade,
                          ManualIngestProcessor manualIngestProcessor) {
    this.storage = storage;
    this.automatedIngestProcessor = automatedIngestProcessor;
    this.authenticationFacade = authenticationFacade;
    this.manualIngestProcessor = manualIngestProcessor;
  }

  @PostMapping("/process")
  public String startIngestProcess(@RequestParam("containerName") String containerName,
                                   @RequestBody AutomatedIngestConfiguration automatedIngestConfiguration) {
    if (automatedIngestProcesses.get(containerName) != null && !automatedIngestProcesses.get(containerName).isDone())
      return "running";

    CompletableFuture<String> process = automatedIngestProcessor.process(
      automatedIngestConfiguration.getDataSetConfigs(),
      containerName,
      authenticationFacade.getUserName(),
      5000);
    automatedIngestProcesses.put(containerName, process);
    return "started";
  }

  @GetMapping("/automated/status")
  public ProcessorStatus getAutomatedIngestStatus(@RequestParam("containerName") String containerName) {
    return new ProcessorStatus(
      automatedIngestProcesses.get(containerName) != null ? automatedIngestProcesses.get(containerName).isDone() : null,
      automatedIngestProcessor.getLog(containerName)
    );
  }

  @GetMapping("/automated/log/clear")
  public void clearAutomatedIngestLog(@RequestParam("containerName") String containerName) {
    automatedIngestProcessor.clearLog(containerName);
  }

  @GetMapping("/automated/stop")
  public void stopAutomatedIngest(@RequestParam("containerName") String containerName) {
    automatedIngestProcessor.stopProcessing(containerName);
  }

  @GetMapping("/manual/status")
  public ProcessorStatus getManualIngestStatus(@RequestParam("containerName") String containerName) {
    return new ProcessorStatus(
      manualIngestProcesses.get(containerName) != null ? manualIngestProcesses.get(containerName).isDone() : null,
      manualIngestProcessor.getLog(containerName)
    );
  }

  @GetMapping("/manual/log/clear")
  public void clearManualIngestLog(@RequestParam("containerName") String containerName) {
    manualIngestProcessor.clearLog(containerName);
  }

  @PutMapping("/file")
  public void saveFile(@RequestParam("file") MultipartFile file,
                       @RequestParam("containerName") String containerName,
                       @RequestParam("pii") Boolean pii) throws IOException {
    storage.save(file.getOriginalFilename(), file.getBytes(), authenticationFacade.getUserName(), containerName, true, pii);
    storage.makeSnapshot(containerName, file.getOriginalFilename());
    manualIngestProcessor.process(containerName, file.getOriginalFilename(), file.getBytes());
  }

}
