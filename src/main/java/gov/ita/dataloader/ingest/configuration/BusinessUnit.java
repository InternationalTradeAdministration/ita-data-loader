package gov.ita.dataloader.ingest.configuration;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class BusinessUnit {
  String businessName;
  String containerName;
  List<String> users;
}
