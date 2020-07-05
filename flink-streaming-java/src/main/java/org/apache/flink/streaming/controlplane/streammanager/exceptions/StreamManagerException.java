package org.apache.flink.streaming.controlplane.streammanager.exceptions;

import org.apache.flink.util.FlinkException;

public class StreamManagerException extends FlinkException {
    private static final long serialVersionUID = -3978431963865449031L;

    public StreamManagerException(String message) {
        super(message);
    }

    public StreamManagerException(Throwable cause) {
        super(cause);
    }

    public StreamManagerException(String message, Throwable cause) {
        super(message, cause);
    }
}
