package gov.ita.dataloader.datafactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/api/data-factory", produces = MediaType.APPLICATION_JSON_VALUE)
public class DataFactoryController {

  @Autowired
  private DataFactoryGateway dataFactoryGateway;

  @GetMapping("/pipeline-status")
  public PipelineRun getPipelineStatus(@RequestParam("pipelineName") String pipelineName) {
    return dataFactoryGateway.getPipelineStatus(pipelineName);
  }

  @GetMapping("/run-pipeline")
  public void runPipeline(@RequestParam("pipelineName") String pipelineName) {
    dataFactoryGateway.runPipeline(pipelineName);
  }
}
