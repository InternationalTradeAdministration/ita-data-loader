package gov.ita.dataloader.data_factory;

import lombok.Data;

import java.util.List;

@Data
public class QueryPipelineResponse {
  List<PipelineRun> value;
}
