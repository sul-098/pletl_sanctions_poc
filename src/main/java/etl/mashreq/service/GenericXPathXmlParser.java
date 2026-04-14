package etl.mashreq.service;

import etl.mashreq.config.CollectionMappingProperties;
import etl.mashreq.config.SourceProperties;
import etl.mashreq.domain.NormalizedEntry;
import etl.mashreq.domain.NormalizedSanctionsFile;
import etl.mashreq.exception.ParsingException;
import etl.mashreq.util.XmlSecurityUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;

@Service
@RequiredArgsConstructor
@Slf4j
public class GenericXPathXmlParser {

    private final EntryKeyService entryKeyService;
    private final CanonicalizationService canonicalizationService;

    public NormalizedSanctionsFile parse(Path file, SourceProperties source) {
        try (InputStream in = Files.newInputStream(file)) {
            Document document = XmlSecurityUtils.parseSecurely(in);
            XPath xPath = XPathFactory.newInstance().newXPath();

            NodeList entryNodes = (NodeList) xPath.compile(source.getEntryXpath())
                    .evaluate(document, XPathConstants.NODESET);

            Map<String, NormalizedEntry> entries = new LinkedHashMap<>();

            for (int i = 0; i < entryNodes.getLength(); i++) {
                Node entryNode = entryNodes.item(i);

                Map<String, Object> attributes = new LinkedHashMap<>();

                // Scalar fields
                for (Map.Entry<String, String> field : source.getFields().entrySet()) {
                    attributes.put(field.getKey(), evaluateScalar(xPath, entryNode, field.getValue()));
                }

                // Collection fields
                for (Map.Entry<String, CollectionMappingProperties> collectionEntry : source.getCollections().entrySet()) {
                    List<Map<String, Object>> items = extractCollection(xPath, entryNode, collectionEntry.getValue());
                    attributes.put(collectionEntry.getKey(), items);
                }

                String businessKey = entryKeyService.resolveBusinessKey(source, entryNode, xPath, attributes);

                Map<String, Object> canonical = canonicalizationService.canonicalizeMap(attributes);
                String hash = canonicalizationService.computeHash(canonical);

                NormalizedEntry normalizedEntry = NormalizedEntry.builder()
                        .businessKey(businessKey)
                        .sourceId(source.getId())
                        .attributes(canonical)
                        .canonicalHash(hash)
                        .build();

                entries.put(businessKey, normalizedEntry);
            }

            log.info("Parsed {} entries from file={} source={}", entries.size(), file.getFileName(), source.getId());

            return NormalizedSanctionsFile.builder()
                    .sourceId(source.getId())
                    .fileName(file.getFileName().toString())
                    .loadedAt(Instant.now())
                    .entries(entries)
                    .build();

        } catch (Exception e) {
            throw new ParsingException("Failed to parse XML file=" + file, e);
        }
    }

    private Object evaluateScalar(XPath xPath, Node contextNode, String expr) throws Exception {
        if (expr.startsWith("'") && expr.endsWith("'")) {
            return expr.substring(1, expr.length() - 1);
        }
        if ("local-name()".equals(expr)) {
            return contextNode.getNodeName();
        }

        String result = xPath.compile(expr).evaluate(contextNode, XPathConstants.STRING).toString();
        return result == null || result.isBlank() ? null : result.trim();
    }

    private List<Map<String, Object>> extractCollection(XPath xPath,
                                                        Node entryNode,
                                                        CollectionMappingProperties mapping) throws Exception {
        NodeList itemNodes = (NodeList) xPath.compile(mapping.getItemXpath())
                .evaluate(entryNode, XPathConstants.NODESET);

        List<Map<String, Object>> items = new ArrayList<>();
        for (int i = 0; i < itemNodes.getLength(); i++) {
            Node itemNode = itemNodes.item(i);
            Map<String, Object> itemMap = new LinkedHashMap<>();

            for (Map.Entry<String, String> itemField : mapping.getItemFields().entrySet()) {
                String value = xPath.compile(itemField.getValue()).evaluate(itemNode, XPathConstants.STRING).toString();
                if (value != null && !value.isBlank()) {
                    itemMap.put(itemField.getKey(), value.trim());
                }
            }

            if (!itemMap.isEmpty()) {
                items.add(itemMap);
            }
        }
        return items;
    }
}
