package gov.ita.dataloader.data_factory;

import com.fasterxml.jackson.core.JsonProcessingException;

public interface DataFactoryGateway {
  PipelineRun getPipelineStatus(String pipelineName) throws JsonProcessingException;

  void runPipeline(String pipelineName);
}
