package com.amazonaws.auth;

/**
 * These are the credentials you are looking for
 */
public class AlwaysSucceedCredentialsProvider implements AWSCredentialsProvider {

    @Override
    public AWSCredentials getCredentials() {
        return new BasicAWSCredentials("123", "456");
    }

	@Override
	public void refresh() {
	}

}
