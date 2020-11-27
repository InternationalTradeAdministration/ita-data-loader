package gov.ita.dataloader.data_factory;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Slf4j
@Service
@Profile("development")
public class DevelopmentDataFactoryGateway implements DataFactoryGateway {
  @Override
  public PipelineRun getPipelineStatus(String pipelineName) {
    return new PipelineRun(
      "some-run-id",
      pipelineName,
      666,
      "Succeeded",
      null,
      true,
      LocalDateTime.now(),
      LocalDateTime.now(),
      LocalDateTime.now());
  }

  @Override
  public void runPipeline(String pipelineName, String fileName) {
    log.info("Starting pipeline run: {}", pipelineName);
  }
}
