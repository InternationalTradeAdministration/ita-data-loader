package gov.ita.dataloader.data_factory;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class Filter {
  String operand;
  String operator;
  List<String> values;
}
