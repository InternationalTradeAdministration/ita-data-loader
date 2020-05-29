package gov.ita.dataloader.data_factory;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PipelineRun {
  String runId;
  String pipelineName;
  Integer durationInMs;
  String status;
  String message;
  Boolean isLatest;
  LocalDateTime runStart;
  LocalDateTime runEnd;
  LocalDateTime lastUpdated;
}
