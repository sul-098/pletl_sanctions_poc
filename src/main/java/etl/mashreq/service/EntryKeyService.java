package etl.mashreq.service;

import  etl.mashreq.config.SourceProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;
import org.w3c.dom.Node;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import java.util.Map;

@Service
@Slf4j
public class EntryKeyService {

    public String resolveBusinessKey(SourceProperties source,
                                     Node entryNode,
                                     XPath xPath,
                                     Map<String, Object> fields) {
        try {
            String type = source.getUniqueKey().getType();

            if ("xpath".equalsIgnoreCase(type)) {
                String value = evaluateToString(xPath, entryNode, source.getUniqueKey().getXpath());
                if (StringUtils.isNotBlank(value)) {
                    return value.trim();
                }
            }

            Object sourceRef = fields.get("sourceRef");
            if (sourceRef != null && StringUtils.isNotBlank(String.valueOf(sourceRef))) {
                return String.valueOf(sourceRef).trim();
            }

            Object primaryName = fields.get("primaryName");
            Object entityType = fields.get("entityType");
            return source.getId() + "|" + StringUtils.defaultString(String.valueOf(primaryName)).trim()
                    + "|" + StringUtils.defaultString(String.valueOf(entityType)).trim();

        } catch (Exception ex) {
            throw new IllegalStateException("Failed to resolve business key for source=" + source.getId(), ex);
        }
    }

    private String evaluateToString(XPath xPath, Node node, String expr) throws Exception {
        if ("local-name()".equals(expr)) {
            return node.getNodeName();
        }
        Object value = xPath.compile(expr).evaluate(node, XPathConstants.STRING);
        return value == null ? null : value.toString();
    }
}
