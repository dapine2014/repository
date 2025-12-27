package storm.repository.com.core.runtime;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

import storm.repository.com.core.api.ConnectorPlugin;
import storm.repository.com.core.errors.ConnectorNotFoundException;

public final class ConnectorRegistry {
    private final Map<String, ConnectorPlugin> pluginsById;

    public ConnectorRegistry() {
        Map<String, ConnectorPlugin> plugins = new HashMap<>();
        for (ConnectorPlugin plugin : ServiceLoader.load(ConnectorPlugin.class)) {
            plugins.put(plugin.id(), plugin);
        }
        this.pluginsById = Collections.unmodifiableMap(plugins);
    }

    public Collection<ConnectorPlugin> list() {
        return pluginsById.values();
    }

    public ConnectorPlugin get(String connectorId) {
        ConnectorPlugin plugin = pluginsById.get(connectorId);
        if (plugin == null) {
            throw new ConnectorNotFoundException(connectorId);
        }
        return plugin;
    }
}
