package com.hotels.road.towtruck;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.github.tomakehurst.wiremock.junit.WireMockRule;

public class TowtruckStartupTest {

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().httpsPort(8443));
  private final TowtruckStartup underTest = new TowtruckStartup();

  @Before
  public void before() {
    // Disable TLS certificate checks
    System.setProperty("com.amazonaws.sdk.disableCertChecking", "true");
  }

  @Test(expected = AmazonS3Exception.class)
  public void testS3WithForbidden() {
    stubFor(get(urlPathMatching("/.*"))
        .willReturn(aResponse()
            .withStatus(403)));

    final AmazonS3 s3 = new TowtruckApp().s3("localhost:8443",
        "");

    underTest.testS3(s3);
  }

  @Test(expected = AmazonS3Exception.class)
  public void testS3WithNotFound() {
    stubFor(get(urlPathMatching("/.*"))
        .willReturn(aResponse()
            .withStatus(404)));

    final AmazonS3 s3 = new TowtruckApp().s3("localhost:8443",
        "");

    underTest.testS3(s3);
  }
}
