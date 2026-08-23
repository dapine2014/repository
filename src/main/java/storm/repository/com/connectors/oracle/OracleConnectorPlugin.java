package storm.repository.com.connectors.oracle;

import storm.repository.com.core.api.ConnectorPlugin;
import storm.repository.com.core.api.ConnectorType;

public final class OracleConnectorPlugin implements ConnectorPlugin {
    @Override public String id()          { return "oracle"; }
    @Override public String displayName() { return "Oracle Database"; }
    @Override public String version()     { return "1.0.0"; }
    @Override public ConnectorType type() { return ConnectorType.SOURCE; }
}
