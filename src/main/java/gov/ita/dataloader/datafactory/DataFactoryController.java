package gov.ita.dataloader.datafactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DataFactoryController {

  @Autowired
  private DataFactoryGateway dataFactoryGateway;

  @GetMapping(value = "/api/data-factory/pipeline-status", produces = MediaType.APPLICATION_JSON_VALUE)
  public PipelineRun getPipelineStatus(@RequestParam("pipelineName") String pipelineName) {
    return dataFactoryGateway.getPipelineStatus(pipelineName);
  }
}
