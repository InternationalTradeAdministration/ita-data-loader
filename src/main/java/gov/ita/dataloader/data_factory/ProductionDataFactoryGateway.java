package gov.ita.dataloader.data_factory;

import com.fasterxml.jackson.core.JsonProcessingException;
import gov.ita.dataloader.azure_auth.AccessTokenGateway;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Slf4j
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

  private final AccessTokenGateway accessTokenGateway;
  private final RestTemplate restTemplate;
  private final String resourceUrl = "https://management.azure.com";

  public ProductionDataFactoryGateway(AccessTokenGateway accessTokenGateway,
                                      RestTemplate restTemplate) {
    this.accessTokenGateway = accessTokenGateway;
    this.restTemplate = restTemplate;
  }

  @Override
  public PipelineRun getPipelineStatus(String pipelineName) throws JsonProcessingException {
    String apiUrl = getBaseApiUrl() + "/queryPipelineRuns?api-version=2018-06-01";
    HttpEntity<QueryPipelineRequest> request = new HttpEntity<>(new QueryPipelineRequest(pipelineName), buildHeaders());
    ResponseEntity<QueryPipelineResponse> response = restTemplate.exchange(apiUrl, HttpMethod.POST, request, QueryPipelineResponse.class);
    return (response.getBody() != null && response.getBody().getValue().size() > 0)
      ? response.getBody().getValue().get(0)
      : null;
  }

  @Override
  public void runPipeline(String pipelineName, String fileName) {
    log.info("Starting pipeline run: {}", pipelineName);
    String apiUrl = getBaseApiUrl() + String.format("/pipelines/%s/createRun?api-version=2018-06-01", pipelineName);

    String param = "{\"fileName\": \"" + fileName + "\" }";
    HttpEntity<String> request = new HttpEntity<>(param, buildHeaders());

    restTemplate.postForLocation(apiUrl, request, String.class);
  }

  private HttpHeaders buildHeaders() {
    String accessToken = accessTokenGateway.getAccessToken(clientId, clientSecret, resourceUrl);
    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    headers.setBearerAuth(accessToken);
    return headers;
  }

  private String getBaseApiUrl() {
    return String.format(
      "%s/subscriptions/%s/resourceGroups/%s/providers/Microsoft.DataFactory/factories/%s",
      resourceUrl, subscriptionId, resourceGroup, datafactoryName);
  }
}
