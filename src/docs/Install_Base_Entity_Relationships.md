# Install Base Data Taxonomy - Entity Relationships Analysis

## Executive Summary

This document provides a detailed analysis of the Install Base Data Taxonomy, which defines a target-state architecture for managing commercial sales, technical deployments, and service operations. The model separates three core domains connected through centralized entitlements and canonical enterprise identifiers.

---

## Core Domains Overview

### 1. **Commercial Install Base** - "What was sold and to whom"
- **System of Record**: CRM/ERP (Azure Fabric SQL DB / AWS Aurora)
- **Purpose**: Commercial entitlement, billing, compliance
- **Key Focus**: Capture contractual and commercial truth

### 2. **Technical Install Base** - "What is deployed, where, and how it runs"
- **System of Insight**: Install Base Platform
- **Purpose**: Runtime enforcement, lifecycle management, usage tracking
- **Key Focus**: Track actual deployed assets and their runtime state

### 3. **Service Install Base** - "What is supported and serviced"
- **System of Insight**: Service Management Systems
- **Purpose**: Support, uptime, regulatory compliance
- **Key Focus**: Manage serviceable assets and maintenance

---

## Canonical Linking Keys

These enterprise-wide identifiers connect all three domains:

| Key | Purpose | Scope |
|-----|---------|-------|
| `customer_id` | Unique customer identifier | Enterprise-wide |
| `account_id` / `legal_entity_id` | Legal/billing entity | Commercial/Legal |
| `contract_id` | Contract identifier | Commercial |
| `order_id` | Order identifier | Commercial |
| `sku_id` / `offering_id` | Product offering identifier | Commercial |
| `entitlement_id` | Runtime rights identifier | Entitlements |
| `asset_id` | Serviceable physical asset | Service |
| `instance_id` | Technical runtime instance | Technical |
| `device_id` | Hardware/edge device | Technical |
| `location_id` / `site_id` | Physical location | Service |

---

## Domain 1: Commercial Install Base

### Entity: **Customer**

**Purpose**: Represents the commercial entity purchasing products/services

| Field | Type | Description |
|-------|------|-------------|
| `customer_id` (PK) | UUID | Unique customer identifier |
| `name` | String | Customer name |
| `customer_type` | String | Enterprise, partner, etc. |
| `region` | String | Market/region |
| `industry` | String | Industry segment |
| `status` | String | Active, inactive |

**Relationships**:
- One Customer → Many Contracts
- One Customer → Many Entitlements
- One Customer → Many Product Instances
- One Customer → Many Service Assets

---

### Entity: **Contract**

**Purpose**: Legal agreement defining commercial terms

| Field | Type | Description |
|-------|------|-------------|
| `contract_id` (PK) | UUID | Unique contract |
| `customer_id` (FK) | UUID | References Customer |
| `legal_entity_id` (FK) | UUID | Billing/legal entity |
| `start_date` | Date | Contract start |
| `end_date` | Date | Contract end |
| `status` | String | Active, expired |
| `terms_ref` | String | Link to contract document |

**Relationships**:
- One Contract → Belongs to One Customer
- One Contract → Many Order Lines
- One Contract → Many Entitlements
- One Contract → Many Service Contracts

---

### Entity: **Order Line**

**Purpose**: Individual line items within an order, linking SKUs to contracts

| Field | Type | Description |
|-------|------|-------------|
| `order_line_id` (PK) | UUID | Unique order line |
| `order_id` (FK) | UUID | Parent order |
| `sku_id` (FK) | String | Product offering |
| `quantity` | Integer | Purchased quantity |
| `pricing_model` | String | Subscription / usage |
| `start_date` | Date | Effective start |
| `end_date` | Date | Effective end |

**Relationships**:
- One Order Line → Belongs to One Order
- One Order Line → References One SKU
- One Order Line → Many Entitlements

---

## Domain 2: Entitlements (Bridge Layer)

### Entity: **Entitlement**

**Purpose**: Translates commercial contracts into runtime product permissions

| Field | Type | Description |
|-------|------|-------------|
| `entitlement_id` (PK) | UUID | Unique entitlement |
| `customer_id` (FK) | UUID | Entitled customer |
| `contract_id` (FK) | UUID | Source contract |
| `order_line_id` (FK) | UUID | Source order line |
| `subject_type` | String | customer / instance / device |
| `subject_id` | UUID | Target of entitlement |
| `status` | String | Active, suspended |
| `valid_from` | Date | Start date |
| `valid_to` | Date | End date |

**Relationships**:
- One Entitlement → Belongs to One Customer
- One Entitlement → Derived from One Contract
- One Entitlement → Derived from One Order Line
- One Entitlement → Many Entitlement Items
- One Entitlement → Applies to One Subject (Customer/Instance/Device)

---

### Entity: **Entitlement Item**

**Purpose**: Specific feature rights, quotas, or limits within an entitlement

| Field | Type | Description |
|-------|------|-------------|
| `entitlement_item_id` (PK) | UUID | Unique entitlement rule |
| `entitlement_id` (FK) | UUID | Parent entitlement |
| `feature_code` | String | Feature or capability |
| `right_type` | String | Access, quota, limit |
| `value` | String/Number | Allowed value |
| `unit` | String | Calls/day, seats |
| `scope` | String | Global, region, instance |

**Relationships**:
- One Entitlement Item → Belongs to One Entitlement
- One Entitlement Item → Enforced at Product Runtime

---

## Domain 3: Technical Install Base

### Entity: **Product Instance**

**Purpose**: Represents a deployed software instance in runtime

| Field | Type | Description |
|-------|------|-------------|
| `instance_id` (PK) | UUID | Runtime instance |
| `customer_id` (FK) | UUID | Owning customer |
| `product_id` | String | Product identifier |
| `deployment_type` | String | Cloud / edge / on-prem |
| `environment` | String | Prod, test, validated |
| `region` | String | Deployment region |
| `status` | String | Active, retired |

**Lifecycle Events**: Provisioned, Activated, Upgraded, Patched, Decommissioned

**Relationships**:
- One Product Instance → Belongs to One Customer
- One Product Instance → Many Entitlements (checked at runtime)
- One Product Instance → Many Usage Records
- One Product Instance → Many Lifecycle Events

---

### Entity: **Device**

**Purpose**: Physical hardware or edge device

| Field | Type | Description |
|-------|------|-------------|
| `device_id` (PK) | UUID | Physical device |
| `customer_id` (FK) | UUID | Owning customer |
| `asset_id` (FK) | UUID | Service asset |
| `model` | String | Device model |
| `serial_number` | String | Manufacturer serial |
| `firmware_version` | String | Firmware level |
| `connectivity` | String | Online/offline |

**Relationships**:
- One Device → Belongs to One Customer
- One Device → Linked to One Service Asset
- One Device → Many Usage Records
- One Device → Many Entitlements

---

## Domain 4: Service Install Base

### Entity: **Service Asset**

**Purpose**: Serviceable asset requiring support and maintenance

| Field | Type | Description |
|-------|------|-------------|
| `asset_id` (PK) | UUID | Serviceable asset |
| `customer_id` (FK) | UUID | Asset owner |
| `device_id` (FK) | UUID | Linked device |
| `product_family` | String | Product category |
| `model` | String | Asset model |
| `serial_number` | String | Serial |
| `install_date` | Date | Installation date |
| `status` | String | In service, retired |

**Relationships**:
- One Service Asset → Belongs to One Customer
- One Service Asset → Linked to One Device
- One Service Asset → One Service Contract
- One Service Asset → Many Service Events

---

### Entity: **Service Contract**

**Purpose**: Service level agreement for support and maintenance

| Field | Type | Description |
|-------|------|-------------|
| `service_contract_id` (PK) | UUID | Service agreement |
| `customer_id` (FK) | UUID | Customer |
| `contract_id` (FK) | UUID | Commercial contract |
| `sla_id` (FK) | UUID | SLA definition |
| `start_date` | Date | Coverage start |
| `end_date` | Date | Coverage end |

**Relationships**:
- One Service Contract → Belongs to One Customer
- One Service Contract → Derived from One Commercial Contract
- One Service Contract → References One SLA
- One Service Contract → Covers Many Service Assets

---

### Entity: **SLA (Service Level Agreement)**

**Purpose**: Defines service level commitments

| Field | Type | Description |
|-------|------|-------------|
| `sla_id` (PK) | UUID | SLA identifier |
| `name` | String | SLA tier name |
| `response_time` | String | Response time commitment |
| `resolution_time` | String | Resolution time commitment |
| `coverage_hours` | String | Support hours |
| `penalties` | JSON | Penalty terms |

**Relationships**:
- One SLA → Many Service Contracts

---

### Entity: **Service Event**

**Purpose**: Tracks maintenance, incidents, and service activities

| Field | Type | Description |
|-------|------|-------------|
| `service_event_id` (PK) | UUID | Event identifier |
| `asset_id` (FK) | UUID | Affected asset |
| `event_type` | String | incident, maintenance, repair, calibration, inspection |
| `opened_at` | Timestamp | Event start |
| `closed_at` | Timestamp | Event end |
| `severity` | String | Severity level |
| `root_cause` | String | Root cause (optional) |
| `outcome` | String | resolved, replaced, escalated |
| `notes_ref` | String | Additional notes (optional) |

**Relationships**:
- One Service Event → Affects One Service Asset
- One Service Event → Tracked under One Service Contract

---

## Domain 5: Usage & Telemetry (Feedback Loop)

### Entity: **Usage Record**

**Purpose**: Captures actual product usage to feed billing and entitlements

| Field | Type | Description |
|-------|------|-------------|
| `usage_id` (PK) | UUID | Usage event |
| `customer_id` (FK) | UUID | Customer |
| `instance_id` (FK) | UUID | Product instance |
| `device_id` (FK) | UUID | Device (optional) |
| `feature_code` | String | Feature used |
| `quantity` | Number | Usage amount |
| `unit` | String | Calls, GB, hours |
| `window_start` | Timestamp | Usage window start |
| `window_end` | Timestamp | Usage window end |

**Relationships**:
- One Usage Record → Belongs to One Customer
- One Usage Record → Generated by One Product Instance (or Device)
- One Usage Record → Feeds Billing Systems
- One Usage Record → Informs Entitlement Evaluations

---

### Entity: **Entitlement Evaluation Log** (Optional but Valuable)

**Purpose**: Audit trail of entitlement checks for compliance and debugging

| Field | Type | Description |
|-------|------|-------------|
| `eval_id` (PK) | UUID | Evaluation identifier |
| `timestamp` | Timestamp | When check occurred |
| `subject_type` | String | customer / instance / device |
| `subject_id` | UUID | Who/what was checked |
| `entitlement_id` | UUID | Entitlement checked (nullable) |
| `decision` | String | allow / deny |
| `reason_code` | String | Why decision was made |
| `limits_remaining` | JSON | Remaining quotas |
| `policy_version` | String | Policy version used |
| `correlation_id` | UUID | Request correlation |

**Relationships**:
- One Evaluation Log → References One Entitlement (if found)
- One Evaluation Log → References One Subject (Instance/Device/Customer)

---

## Cross-Domain Relationship Patterns

### Pattern 1: **Commercial → Entitlements → Technical**

```
Customer → Contract → Order Line → SKU
                ↓
           Entitlement → Entitlement Items
                ↓
        Product Instance (Runtime Enforcement)
```

**Flow**: 
1. Customer purchases SKU via Contract/Order
2. Order Line generates Entitlements
3. Entitlements grant runtime permissions to Product Instances
4. Product Instance checks entitlements at runtime

---

### Pattern 2: **Commercial → Service → Technical**

```
Customer → Contract → Service Contract → SLA
                ↓                          ↓
         Service Asset ← Device ← Product Instance
                ↓
         Service Events
```

**Flow**:
1. Customer purchases service via Contract
2. Service Contract covers Service Assets
3. Service Assets linked to physical Devices
4. Devices run Product Instances
5. Service Events track maintenance activities

---

### Pattern 3: **Usage Feedback Loop**

```
Product Instance/Device → Usage Records
         ↓                        ↓
    Entitlement Check      Billing System
         ↓                        ↓
   Allow/Deny Decision       Invoice Generation
         ↓
   Entitlement Evaluation Log
```

**Flow**:
1. Product Instance generates Usage Records
2. Usage feeds back to Entitlement checks (quota consumption)
3. Usage also feeds Billing for invoicing
4. Evaluation Logs track all entitlement decisions

---

### Pattern 4: **AI & Optimization Layer**

```
Usage Records + Telemetry → AI/ML Models
                ↓
    Anomaly Detection + Utilization Insights
                ↓
    Automated Entitlement Recommendations
                ↓
    Optimize Customer Experience
```

**Flow**:
1. Usage and telemetry data collected
2. AI models analyze patterns
3. Detect anomalies (fraud, overuse, underuse)
4. Recommend entitlement adjustments
5. Drive product-led growth

---

## Complete Entity Relationship Diagram (Textual)

```
COMMERCIAL DOMAIN
├── Customer (1) ──────────┬───────────┬──────────┐
│   └── customer_id        │           │          │
│                           │           │          │
├── Contract (M) ──────────┤           │          │
│   ├── contract_id        │           │          │
│   └── customer_id (FK)───┘           │          │
│                                       │          │
├── Order (M)                           │          │
│   ├── order_id                        │          │
│   └── contract_id (FK)────────────────┘          │
│                                                   │
└── Order Line (M)                                  │
    ├── order_line_id                               │
    ├── order_id (FK)                               │
    └── sku_id                                      │
                                                    │
ENTITLEMENTS DOMAIN (BRIDGE)                        │
├── Entitlement (M) ────────────────────────────────┤
│   ├── entitlement_id                              │
│   ├── customer_id (FK)────────────────────────────┘
│   ├── contract_id (FK)
│   ├── order_line_id (FK)
│   ├── subject_type (customer/instance/device)
│   └── subject_id
│
└── Entitlement Item (M)
    ├── entitlement_item_id
    ├── entitlement_id (FK)
    ├── feature_code
    └── right_type

TECHNICAL DOMAIN
├── Product Instance (M) ───────────┬───────────────┐
│   ├── instance_id                 │               │
│   └── customer_id (FK)────────────┤               │
│                                    │               │
└── Device (M) ────────────────────┤               │
    ├── device_id                   │               │
    ├── customer_id (FK)────────────┤               │
    └── asset_id (FK)               │               │
                                     │               │
SERVICE DOMAIN                       │               │
├── Service Asset (M) ───────────────┤               │
│   ├── asset_id                     │               │
│   ├── customer_id (FK)─────────────┤               │
│   └── device_id (FK)               │               │
│                                     │               │
├── Service Contract (M) ────────────┤               │
│   ├── service_contract_id          │               │
│   ├── customer_id (FK)─────────────┘               │
│   ├── contract_id (FK)                             │
│   └── sla_id (FK)                                  │
│                                                     │
├── SLA (M)                                          │
│   └── sla_id                                       │
│                                                     │
└── Service Event (M)                                │
    ├── service_event_id                             │
    └── asset_id (FK)                                │
                                                      │
USAGE & TELEMETRY DOMAIN                             │
├── Usage Record (M) ─────────────────────────────────┤
│   ├── usage_id                                      │
│   ├── customer_id (FK)──────────────────────────────┘
│   ├── instance_id (FK)
│   ├── device_id (FK)
│   └── feature_code
│
└── Entitlement Evaluation Log (M)
    ├── eval_id
    ├── subject_id
    ├── entitlement_id (FK)
    └── decision
```

Legend:
- (1) = One
- (M) = Many
- (FK) = Foreign Key
- (PK) = Primary Key

---

## Key Insights

### Separation of Concerns
1. **Commercial** owns "what was sold" – source of truth for contracts
2. **Technical** owns "what is deployed" – runtime state
3. **Service** owns "what is supported" – maintenance and SLA
4. **Entitlements** bridges commercial to technical – translates contracts into runtime permissions

### Data Flow
1. **Downstream**: Commercial → Entitlements → Technical
2. **Upstream**: Usage/Telemetry → Billing/Entitlements → AI Optimization
3. **Lateral**: Service ↔ Technical (devices/assets linked)

### Strategic Benefits
- **Revenue Protection**: Entitlements enforce what was sold
- **Reduced Duplication**: Canonical IDs link domains without copying data
- **Auditability**: Evaluation logs provide compliance trail
- **AI-Ready**: Structured usage data enables ML-driven insights
- **Scalability**: Stateless, API-first entitlements layer

### Big Tech Patterns
- **Amazon**: Commercial facts platform + usage ledger
- **Netflix**: Entitlements + plans DB (no licenses)
- **Stripe**: Event-sourced commercial ledger
- **Uber**: Product-centric commerce services
- **Microsoft**: Hybrid ERP + internal commerce platforms

---

## Implementation Considerations

### Database Design
- Use UUID primary keys for global uniqueness
- Implement foreign key constraints for referential integrity
- Create indexes on: customer_id, contract_id, instance_id, device_id, asset_id
- Consider partitioning Usage Records by time window
- Use JSON/JSONB for flexible fields (penalties, limits_remaining)

### API Design
- RESTful APIs for CRUD operations
- GraphQL for complex relationship queries
- Webhook/Event-driven for real-time updates
- Caching strategy for entitlement checks (latency-sensitive)

### Data Governance
- Clear ownership: Commercial (Sales), Technical (Engineering), Service (Operations)
- Data quality rules at each domain boundary
- Master Data Management for Customer entity
- Regular reconciliation between domains

### Security & Compliance
- Encryption at rest and in transit
- Role-based access control per domain
- Audit logging for all entitlement decisions
- GDPR/privacy compliance for customer data
- SOC2/ISO compliance for service records

---

## Conclusion

This Install Base Data Taxonomy provides a comprehensive, scalable framework for managing the complete lifecycle of commercial sales, technical deployments, and service operations. By separating concerns while maintaining strong linkages through canonical identifiers, organizations can achieve:

- Clear data ownership and accountability
- Scalable runtime enforcement of commercial terms
- AI-driven insights and optimization
- Auditability and compliance
- Product-led growth enablement

The model is designed to replace legacy licensing with modern, cloud-native entitlements while providing flexibility for hybrid and edge deployments.
