package storm.repository.com.core.errors;

public class ConnectorNotFoundException extends RuntimeException {
    public ConnectorNotFoundException(String connectorId) {
        super("Connector not found: " + connectorId);
    }
}
