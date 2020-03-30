package gov.ita.dataloader.ingest;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class ProcessorStatus {
  Boolean isDone;
  List<LogItem> logItems;
}
