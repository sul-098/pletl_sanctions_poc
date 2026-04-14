package etl.mashreq.config;

import javax.xml.namespace.NamespaceContext;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

public class XPathNamespaceContext implements NamespaceContext {

    private final Map<String, String> namespaces;

    public XPathNamespaceContext(Map<String, String> namespaces) {
        this.namespaces = namespaces == null ? Collections.emptyMap() : namespaces;
    }

    @Override
    public String getNamespaceURI(String prefix) {
        return namespaces.getOrDefault(prefix, "");
    }

    @Override
    public String getPrefix(String namespaceURI) {
        return namespaces.entrySet().stream()
                .filter(e -> e.getValue().equals(namespaceURI))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElse(null);
    }

    @Override
    public Iterator<String> getPrefixes(String namespaceURI) {
        String prefix = getPrefix(namespaceURI);
        return prefix == null ? Collections.emptyIterator() : Collections.singleton(prefix).iterator();
    }
}
