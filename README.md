# AML Sanctions Delta POC

This project compares sanctions lists (OFAC, HMT, UN, EU, UAE) and generates delta outputs.

## Features
- Multi-source XML parsing
- Delta detection (Added, Removed, Updated)
- Config-driven mapping
- Supports large files (planned streaming upgrade)

## Tech Stack
- Java 17
- Spring Boot
- XML parsing (XPath → moving to StAX)