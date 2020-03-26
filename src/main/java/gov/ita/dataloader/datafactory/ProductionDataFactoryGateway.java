package gov.ita.dataloader.datafactory;

import gov.ita.dataloader.azure_auth.AccessTokenGateway;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

@Service
@Profile({"production", "staging"})
public class ProductionDataFactoryGateway implements DataFactoryGateway {

  @Value("${datafactory.client-id}")
  private String clientId;

  @Value("${datafactory.client-secret}")
  private String clientSecret;

  @Value("${datafactory.name}")
  private String datafactoryName;

  @Value("${datafactory.resource-group}")
  private String resourceGroup;

  @Value("${azure-subscription-id}")
  private String subscriptionId;

  private AccessTokenGateway accessTokenGateway;
  private final RestTemplate restTemplate;

  public ProductionDataFactoryGateway(AccessTokenGateway accessTokenGateway, RestTemplate restTemplate) {
    this.accessTokenGateway = accessTokenGateway;
    this.restTemplate = restTemplate;
  }

  @Override
  public PipelineRun getPipelineStatus(String pipelineName) {
    String accessToken = accessTokenGateway.getAccessToken(clientId, clientSecret, "https://management.azure.com");

    String datafacotryApiUrl = String.format(
      "https://management.azure.com/subscriptions/%s/resourceGroups/%s/providers/Microsoft.DataFactory/factories/%s/queryPipelineRuns?api-version=2018-06-01",
      subscriptionId, resourceGroup, datafactoryName);

    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    headers.setBearerAuth(accessToken);

    String requestBody = "{ \"lastUpdatedAfter\": \"1999-03-25T00:00:00.000Z\", \"lastUpdatedBefore\": " + today() + " }";
    HttpEntity<String> requestEntity = new HttpEntity<>(requestBody, headers);
    DataFactoryPipelineResponse responseBody = restTemplate.exchange(
      datafacotryApiUrl,
      HttpMethod.POST,
      requestEntity,
      DataFactoryPipelineResponse.class
    ).getBody();

    List<PipelineRun> pipelineRuns = responseBody.getValue();
    PipelineRun latestPipelineRun = null;
    for (PipelineRun run : pipelineRuns) {
      if (run.pipelineName.equals(pipelineName)
        && ((latestPipelineRun == null) || (run.isLatest && run.lastUpdated.isAfter(latestPipelineRun.lastUpdated)))) {
        latestPipelineRun = run;
      }
    }

    return latestPipelineRun;
  }

  private String today() {
    return ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT);
  }

}
