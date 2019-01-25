/**
 * Copyright (C) 2016-2019 Expedia Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hotels.road.tool.cli;

import java.io.InputStreamReader;
import java.io.PrintStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import com.hotels.road.offramp.client.Committer;
import com.hotels.road.offramp.client.OfframpClient;
import com.hotels.road.offramp.client.OfframpOptions;
import com.hotels.road.offramp.model.DefaultOffset;
import com.hotels.road.offramp.model.Message;
import com.hotels.road.tls.TLSConfig;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.core.OutputStreamAppender;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.IVersionProvider;
import picocli.CommandLine.Option;

import reactor.core.publisher.Flux;


/**
 * Main class of `data-highway-console-offramp` cli tool.
 */
@Command(
    description = "Data Highway Offramp CLI client",
    name = "data-highway-console-offramp",
    sortOptions = false,
    versionProvider = OfframpConsole.ManifestVersionProvider.class
)
public class OfframpConsole implements Callable<Void> {

  // Configure the default message and CLI output
  PrintStream msgout = System.out;
  PrintStream cliout = System.err;
  ObjectMapper mapper = new ObjectMapper();

  // Required options

  @Option(
      names = { "-h", "--host"}, required = true,
      description = "Cluster of Data Highway that Offramp CLI client will connect (required).")
  String host;

  @Option(
      names = { "-r", "--roadName"}, required = true,
      description = "Road of which to consume messages (required).")
  String roadName;

  @Option(
      names = { "-s", "--streamName"}, required = true,
      description = "Stream under which road to consume messages (required).")
  String streamName;

  // Optional options

  @Option(
      names = { "-u", "--username"},
      description = "User name of the account to consume messages.")
  String username = null;

  @Option(
      names = { "-p", "--password"},
      description = "Password of the provided user.")
  String password = null;

  @Option(
      names = { "-o", "--defaultOffset"},
      description = "Option between the latest and earliest available message to Offramp service. "
          + "Enum values: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})",
      completionCandidates = DefaultOffsetCandidates.class)
  DefaultOffset defaultOffset = DefaultOffset.LATEST;

  @Option(
      names = "--initialRequest",
      description = "Defines how many messages will be requested on the first call to Offramp Server "
          + "(default: ${DEFAULT-VALUE}).")
  Integer initialRequestAmount = 200;

  @Option(
      names = "--replenishingRequest",
      description = "Defines how many messages will be requested on subsequent requests. "
          + "The subsequent requests will only happen after that many messages are consumed downstream "
          + "(default: ${DEFAULT-VALUE}).")
  Integer replenishingRequestAmount = 120;

  @Option(
      names = "--commitIntervalMs",
      description = "Interval that messages will be committed in milliseconds "
          + "(default: ${DEFAULT-VALUE}).")
  Long commitIntervalMs = 500L;

  @Option(
      names = "--numToConsume",
      description = "Total number of messages to be consumed before termination "
          + "(default: ${DEFAULT-VALUE}).")
  Long numToConsume = Long.MAX_VALUE;

  @Option(
      names = "--format",
      description = "Choose the output format of the logged messages. "
          + "Enum values: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).",
      completionCandidates = FormatCandidates.class)
  Format format = Format.JSON;

  @Option(
      names = "--flipOutput",
      description = "Offramp messages are streamed into stout and CLI prompts into stderr. "
          + "This option flips the output of messages to stderr and CLI prompts to stout (default: ${DEFAULT-VALUE}).")
  boolean flipOutput = false;

  @Option(
      names = "--tlsTrustAll",
      description = "Disables certificate checking and hostname verification. "
          + "This is intended for testing only (default: ${DEFAULT-VALUE}).")
  boolean tlsTrustAll = false;

  @Option(
      names = "--debug",
      description = "Debug print (default: ${DEFAULT-VALUE}).")
  boolean debug = false;

  @Option(names = {"-v", "--version"},
      versionHelp = true,
      description = "Print version info and exit")
  boolean versionRequested;

  @Option(names = "--help",
      usageHelp = true,
      description = "Print help info and exit")
  boolean helpRequested;


  public static void main(String[] args) throws Exception {
    CommandLine.call(new OfframpConsole(), args);
  }

  @Override
  public Void call() throws Exception {
    configureOutput();         // this function should be called first
    validateRequiredOptions(); // ensure that required options are provided

    if (debug) {
      final String hidden = String.join("",
          Collections.nCopies(this.password != null ? this.password.length() : 0, "*"));
      cliout.print(this.getClass() + " was configured with:\n" +
          "\thost:                      " + this.host                      + "\n" +
          "\tusername:                  " + this.username                  + "\n" +
          "\tpassword:                  " + hidden                         + "\n" +
          "\troadName:                  " + this.roadName                  + "\n" +
          "\tstreamName:                " + this.streamName                + "\n" +
          "\tdefaultOffset:             " + this.defaultOffset             + "\n" +
          "\tinitialRequestAmount:      " + this.initialRequestAmount      + "\n" +
          "\treplenishingRequestAmount: " + this.replenishingRequestAmount + "\n" +
          "\tcommitIntervalMs:          " + this.commitIntervalMs          + "\n" +
          "\tnumToConsume:              " + this.numToConsume              + "\n" +
          "\ttlsTrustAll:               " + this.tlsTrustAll               + "\n");
    }

    OfframpOptions<JsonNode> options = getOptions();

    runClient(options);

    return null;
  }


  //
  // Helper functions
  //

  /**
   * Helper function to configure where message and CLI output is directed as well as configure logging
   * This should be called first to ensure correct output of message and cli logging into the desired stream.
   */
  private void configureOutput() {
    try {
      if (flipOutput) {
        msgout = System.err;
        cliout = System.out;
      }

      // change from json to yaml
      if (format == Format.YAML) {
          mapper = new YAMLMapper();
      }


      // retrieve the ch.qos.logback.classic.LoggerContext
      LoggerContext logCtx = (LoggerContext) org.slf4j.LoggerFactory.getILoggerFactory();

      PatternLayoutEncoder logEncoder = new PatternLayoutEncoder();
      logEncoder.setContext(logCtx);
      logEncoder.setPattern("%d{HH:mm:ss.SSS} - %-5level %logger{35} - %msg%n");
      logEncoder.start();

      OutputStreamAppender logConsoleAppender = new OutputStreamAppender();
      logConsoleAppender.setContext(logCtx);
      logConsoleAppender.setOutputStream(cliout);
      logConsoleAppender.setName("console");
      logConsoleAppender.setEncoder(logEncoder);
      logConsoleAppender.start();

      Logger rootLogger = logCtx.getLogger("root");
      rootLogger.detachAndStopAllAppenders();
      rootLogger.setLevel(Level.WARN);
      rootLogger.addAppender(logConsoleAppender);

      Logger roadLogger = logCtx.getLogger("com.hotels.road");
      roadLogger.setLevel(Level.WARN);

      Logger clientLogger = logCtx.getLogger("com.hotels.road.offramp.client");
      clientLogger.setLevel(debug ? Level.DEBUG : Level.INFO);

      Logger cliLogger = logCtx.getLogger("com.hotels.road.tool.cli");
      cliLogger.setLevel(debug ? Level.DEBUG : Level.INFO);
    } catch (Exception e) {
      System.err.println("Error configuring the message and cli output:");
      e.printStackTrace();
      System.exit(Error.OUTPUT_CONFIGURATION.code);
    }
  }

  /**
   * Helper function to ensure that required options are provided from picocli
   */
  private void validateRequiredOptions(){
    if (this.host == null || this.roadName == null || this.streamName == null) {
      cliout.println("Error acquiring necessary options. (host, roadName or streamName)");
      System.exit(Error.CONSOLE_OPTIONS_CONFIGURATION.code);
    }
  }

  /**
   * Helper function to construct OfframpOptions
   */
  @SuppressWarnings("unchecked")
  private OfframpOptions<JsonNode> getOptions() {

    OfframpOptions<JsonNode> options = null;

    try {
      TLSConfig.Factory tlsFactory = tlsTrustAll ? TLSConfig.trustAllFactory() : null;

      OfframpOptions.Builder optionsBuilder = OfframpOptions
          .builder(JsonNode.class)
          .host(host)
          .roadName(roadName)
          .streamName(streamName)
          .defaultOffset(defaultOffset)
          .requestBuffer(initialRequestAmount, replenishingRequestAmount)
          .tlsConfigFactory(tlsFactory);

      if (username != null) {
          optionsBuilder.username(username);
      }
      if (password != null) {
          optionsBuilder.password(password);
      }

      options = optionsBuilder.build();

    } catch (Exception e) {
      cliout.println("Error creating OfframpOptions: ");
      e.printStackTrace();
      System.exit(Error.OFFRAMP_OPTIONS_CONFIGURATION.code);
    }

    return options;
  }

  /**
   * Helper function to run OfframpClient
   */
  void runClient(OfframpOptions<JsonNode> options) {

    try (OfframpClient<JsonNode> client = OfframpClient.create(options)) {
      Committer committer = Committer.create(client, Duration.ofMillis(commitIntervalMs));
      Flux.from(client.messages())
          .doOnNext(this::msgPrint)
          .doOnError((e) -> cliout.println(e.getMessage()))
          .doOnNext(committer::commit)
          .limitRequest(numToConsume)
          .then()
          .block();
    } catch (Exception e) {
      // handle the exception that has been thrown by OfframpClient.create() OR by close()
      cliout.println("Error creating or closing Offramp client: ");
      e.printStackTrace();
      System.exit(Error.RUN_CLIENT.code);
    }
  }

  /**
   * Format message and print is to message output.
   */
  private void msgPrint(Message<JsonNode> msg) {
    try {
      switch (format) {
        case OBJECT:
          // stringify java object
          msgout.println(msg);
          break;
        default:
          // format into json or yaml
          msgout.println(mapper.writeValueAsString(msg));
          break;
      }
    } catch (JsonProcessingException e) {
      cliout.println(String.format("Error serialising to %s the message: %s",format, msg));
      cliout.println(getStackTrace(e.getStackTrace()));
    }
  }

  private String getStackTrace(StackTraceElement[] stes) {
    return String.join("\n",
        Arrays.stream(stes)
            .map((m) -> "\t" + m.toString())
            .collect(Collectors.toList())
    );
  }

  /**
   * Class that returns a list of {@link DefaultOffset} enumerations.
   */
  private static class DefaultOffsetCandidates extends ArrayList<String> {
    DefaultOffsetCandidates() {
      super(
          Arrays.stream(DefaultOffset.values())
              .map(DefaultOffset::name)
              .collect(Collectors.toList())
      );
    }
  }

  /**
   * Class that returns a list of {@link DefaultOffset} enumerations.
   */
  private static class FormatCandidates extends ArrayList<String> {
    FormatCandidates() {
      super(
          Arrays.stream(Format.values())
              .map(Format::name)
              .collect(Collectors.toList())
      );
    }
  }

  /**
   * {@link IVersionProvider} implementation that returns version information from the jar file's {@code pom.xml} file.
   */
  static class ManifestVersionProvider implements IVersionProvider {

    public String[] getVersion() throws Exception {
      String pomVersion = getVersionFromPom();
      if (pomVersion != null ) { return new String[] { pomVersion }; }

      String propertiesVersion = getVersionFromProperties();
      if (propertiesVersion != null ) { return new String[] { propertiesVersion }; }

      String packageVersion = getVersionFromPackage();
      if (packageVersion != null ) { return new String[] { packageVersion }; }

      return new String[] { "" };
    }

    private String getVersionFromPom() throws Exception {
      MavenXpp3Reader reader = new MavenXpp3Reader();
      Model model = reader.read(
          new InputStreamReader(
              OfframpConsole.class
                  .getResourceAsStream("/META-INF/maven/com.hotels.road/road-tool-cli/pom.xml")
          )
      );
      return model.getVersion();
    }

    private String getVersionFromProperties() throws Exception {
      Properties p = new Properties();
      p.load(
          OfframpConsole.class
              .getResourceAsStream("/META-INF/maven/com.hotels.road/road-tool-cli/pom.properties")
      );
      return p.getProperty("version", null);
    }

    private String getVersionFromPackage() throws Exception {
      Package aPackage = OfframpConsole.class.getPackage();
      String version = null;
      if (aPackage != null) {
        version = aPackage.getImplementationVersion();
        if (version == null) {
          version = aPackage.getSpecificationVersion();
        }
      }
      return version;
    }
  }

  /**
   * Enum to list the exit error codes of this CLI tool
   */
  enum Error {
    OUTPUT_CONFIGURATION(1),
    CONSOLE_OPTIONS_CONFIGURATION(2),
    OFFRAMP_OPTIONS_CONFIGURATION(3),
    RUN_CLIENT(4);

    private int code;

    Error(int num) {
      this.code = num;
    }
  }

  enum Format {
    JSON,
    YAML,
    OBJECT
  }
}
