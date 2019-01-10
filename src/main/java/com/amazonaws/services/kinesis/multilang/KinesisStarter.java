package com.amazonaws.services.kinesis.multilang;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;

/**
 * This file is derived mostly from Waldemar Hummer's custom extensions here:
 * https://github.com/localstack/localstack/blob/master/localstack/utils/kinesis/java/com/atlassian/KinesisStarter.java
 * 
 * With modifications to allow it to work with the latest 1.x version of Amazon Kinesis Client, specifically with LocalStack in mind
 * 
 * Custom extensions to <code>MultiLangDaemon</code> class from
 * amazon-kinesis-client project, introducing the following additional
 * configuration properties:
 *
 * - kinesisEndpoint: endpoint host (hostname:port) for Kinesis API
 * - kinesisProtocol: protocol for Kinesis API (http or https)
 * - dynamodbEndpoint: endpoint host (hostname:port) for DynamoDB API
 * - dynamodbProtocol: protocol for DynamoDB API (http or https)
 * - metricsLevel: level of CloudWatch metrics to report (e.g., SUMMARY or NONE)
 *
 * @author Kerey Watters
 */
public class KinesisStarter {

	private static final String PROP_DYNAMODB_ENDPOINT = "dynamodbEndpoint";
	private static final String PROP_DYNAMODB_PROTOCOL = "dynamodbProtocol";
	private static final String PROP_KINESIS_ENDPOINT = "kinesisEndpoint";
	private static final String PROP_KINESIS_PROTOCOL = "kinesisProtocol";
	private static final String PROP_METRICS_LEVEL = "metricsLevel";

	public static void printUsage(PrintStream stream, String messageToPrepend) {
		StringBuilder builder = new StringBuilder();
		if (messageToPrepend != null) {
			builder.append(messageToPrepend);
		}
		stream.println(builder.toString());
	}

	public static void main(String[] args) throws Exception {

		if (args.length == 0) {
			printUsage(System.err, "You must provide a properties file");
			System.exit(1);
		}
		
		System.setProperty("com.amazonaws.sdk.disableCbor", "true");

		printUsage(System.out, "Reading properties from " + args[0]);
		Properties props = loadProperties(args[0]);

		if (props.containsKey("disableCertChecking")) {
			System.setProperty("com.amazonaws.sdk.disableCertChecking", "true");
		}

		MultiLangDaemonConfig config = null;
		try {
			config = new MultiLangDaemonConfig(args[0]);
		} catch (IOException e) {
			printUsage(System.err, "You must provide a properties file");
			System.exit(1);
		} catch (IllegalArgumentException e) {
			printUsage(System.err, e.getMessage());
			System.exit(1);
		}

		try {
			ExecutorService executorService = config.getExecutorService();
			KinesisClientLibConfiguration kinesisConfig = config.getKinesisClientLibConfiguration();
	
			if (props.containsKey(PROP_METRICS_LEVEL)) {
				String level = props.getProperty(PROP_METRICS_LEVEL);
				kinesisConfig = kinesisConfig.withMetricsLevel(level);
			}
			if (props.containsKey(PROP_DYNAMODB_ENDPOINT)) {
				String protocol = "http";
				if (props.containsKey(PROP_DYNAMODB_PROTOCOL)) {
					protocol = props.getProperty(PROP_DYNAMODB_PROTOCOL);
				}
				String endpoint = protocol + "://" + props.getProperty(PROP_DYNAMODB_ENDPOINT);
				kinesisConfig.withDynamoDBEndpoint(endpoint);
			}
			if (props.containsKey(PROP_KINESIS_ENDPOINT)) {
				String protocol = "http";
				if (props.containsKey(PROP_KINESIS_PROTOCOL)) {
					protocol = props.getProperty(PROP_KINESIS_PROTOCOL);
				}
				String endpoint = protocol + "://" + props.getProperty(PROP_KINESIS_ENDPOINT);
				kinesisConfig.withKinesisEndpoint(endpoint);
			}
	
			MultiLangDaemon daemon = new MultiLangDaemon(kinesisConfig, config.getRecordProcessorFactory(),
					executorService);
			
			final long shutdownGraceMillis = config.getKinesisClientLibConfiguration().getShutdownGraceMillis();
			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					printUsage(System.out, "Process terminated, initiating shutdown.");
					try {
						Future<Void> fut = daemon.worker.requestShutdown();
						fut.get(shutdownGraceMillis, TimeUnit.MILLISECONDS);
						printUsage(System.out, "Process shutdown is complete.");
					} catch (InterruptedException | ExecutionException | TimeoutException e) {
						printUsage(System.err, "Encountered an error during shutdown.");
					}
				}
			});
	
			Future<Integer> future = executorService.submit(daemon);
			try {
				System.exit(future.get());
			} catch (InterruptedException | ExecutionException e) {
				printUsage(System.err, "Encountered an error while running daemon" + e);
				printUsage(System.err, e.getMessage());
				printUsage(System.err, "" + e.getStackTrace());
			}
		} catch (Exception e) {
			printUsage(System.err, e.getMessage());
			System.exit(1);
		}
		printUsage(System.out, "Finished!");
		System.exit(1);
	}

	private static Properties loadProperties(String propertiesFileName) throws IOException {
		Properties properties = new Properties();
		InputStream propertyStream = null;
		try {
			propertyStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(propertiesFileName);
			if (propertyStream == null) {
				File propertyFile = new File(propertiesFileName);
				if (propertyFile.exists()) {
					propertyStream = new FileInputStream(propertyFile);
				}
			}

			if (propertyStream == null) {
				throw new FileNotFoundException(
						"Unable to find property file in classpath, or file system: '" + propertiesFileName + "'");
			}

			properties.load(propertyStream);
			return properties;
		} finally {
			if (propertyStream != null) {
				propertyStream.close();
			}
		}

	}
}