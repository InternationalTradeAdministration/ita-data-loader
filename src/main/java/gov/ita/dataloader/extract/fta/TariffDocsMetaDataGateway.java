package gov.ita.dataloader.extract.fta;

import gov.ita.dataloader.azure_auth.AccessTokenGateway;
import gov.ita.dataloader.extract.fta.tariff.TariffDocsMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.List;

@Service
public class TariffDocsMetaDataGateway {

  @Value("${tariff-docs.client-id}")
  private String clientId;

  @Value("${tariff-docs.client-secret}")
  private String clientSecret;

  @Value("${tariff-docs.metadata-url}")
  private String tariffDocsMetadataUrl;

  private AccessTokenGateway accessTokenGateway;
  private final RestTemplate restTemplate;

  public TariffDocsMetaDataGateway(AccessTokenGateway accessTokenGateway, RestTemplate restTemplate) {
    this.accessTokenGateway = accessTokenGateway;
    this.restTemplate = restTemplate;
  }

  public List<TariffDocsMetadata> getMetadata() {
    String accessToken = accessTokenGateway.getAccessToken(clientId, clientSecret, clientId);

    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    headers.setBearerAuth(accessToken);

    String body = "{\"query\": \"$filter=Publication eq 'FTA Publication'\"}";

    HttpEntity<String> requestEntity = new HttpEntity<>(body, headers);
    return restTemplate.exchange(
      tariffDocsMetadataUrl,
      HttpMethod.POST,
      requestEntity,
      new ParameterizedTypeReference<List<TariffDocsMetadata>>() {
      }).getBody();
  }

}
