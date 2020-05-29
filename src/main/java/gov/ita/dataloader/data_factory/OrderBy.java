package gov.ita.dataloader.data_factory;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class OrderBy {
  String order;
  String orderBy;
}
