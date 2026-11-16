package com.exemplo.pagamentos.core;

import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;

import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.UnknownHostException;
import java.net.http.HttpConnectTimeoutException;
import java.util.Set;

public final class CoreOutcomeClassifier {

    private static final Set<Integer> CLIENT_ERRORS_NOT_EXECUTED = Set.of(429);
    private static final Set<Integer> CLIENT_ERRORS_AMBIGUOUS = Set.of(408, 409);

    private CoreOutcomeClassifier() {}

    public static CoreResult classify(Throwable error) {
        if (error instanceof HttpClientErrorException http) {
            int status = http.getStatusCode().value();
            String detail = status + " " + http.getResponseBodyAsString();
            if (CLIENT_ERRORS_NOT_EXECUTED.contains(status)) {
                return new CoreResult.NotSent(detail);
            }
            if (CLIENT_ERRORS_AMBIGUOUS.contains(status)) {
                return new CoreResult.Ambiguous(detail);
            }
            return new CoreResult.Rejected(detail);
        }
        if (error instanceof HttpServerErrorException http) {
            return new CoreResult.Ambiguous(http.getStatusCode().value() + " " + http.getResponseBodyAsString());
        }
        if (neverLeftTheClient(error)) {
            return new CoreResult.NotSent(describe(error));
        }
        return new CoreResult.Ambiguous(describe(error));
    }

    private static boolean neverLeftTheClient(Throwable error) {
        Throwable current = error;
        while (current != null) {
            if (current instanceof ConnectException
                    || current instanceof HttpConnectTimeoutException
                    || current instanceof UnknownHostException
                    || current instanceof NoRouteToHostException) {
                return true;
            }
            current = current.getCause() == current ? null : current.getCause();
        }
        return false;
    }

    private static String describe(Throwable error) {
        Throwable root = error;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return error.getClass().getSimpleName() + ": " + error.getMessage()
                + (root != error ? " (causa: " + root.getClass().getSimpleName() + ": " + root.getMessage() + ")" : "");
    }
}
