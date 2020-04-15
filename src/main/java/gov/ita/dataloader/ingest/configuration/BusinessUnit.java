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

  boolean isAdmin(String user) {
    return containerName.equals("dataloader") && users.contains(user);
  }

  boolean includes(String user) {
    return users.contains(user);
  }

  boolean isAdminEmpty() {
    return containerName.equals("dataloader") && users.size() == 0;
  }
}
