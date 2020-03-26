package gov.ita.dataloader.datafactory;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

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
}
