package gov.ita.dataloader.datafactory;

import lombok.Data;

import java.util.List;

@Data
public class DataFactoryPipelineResponse {
  List<PipelineRun> value;
}
