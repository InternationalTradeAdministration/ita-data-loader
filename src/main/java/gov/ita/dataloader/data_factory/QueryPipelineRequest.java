package gov.ita.dataloader.data_factory;

import lombok.Data;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Data
public class QueryPipelineRequest {
  String lastUpdatedAfter = "1999-03-25T00:00:00.000Z";
  String lastUpdatedBefore = ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT);
  List<Filter> filters;
  List<OrderBy> orderBy;

  public QueryPipelineRequest(String pipelineName) {
    filters = new ArrayList<>();
    filters.add(new Filter("PipelineName", "Equals", Collections.singletonList(pipelineName)));
    filters.add(new Filter("LatestOnly", "Equals", Collections.singletonList("true")));

    orderBy = new ArrayList<>();
    orderBy.add(new OrderBy("DESC", "RunEnd"));
  }
}
