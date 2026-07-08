# Install Base Entity Relationship Diagrams

This document contains visual representations of the Install Base Data Taxonomy using Mermaid diagrams.

---

## High-Level Architecture Overview

```mermaid
graph TB
    subgraph Commercial["Commercial Install Base (CRM/ERP)"]
        Customer[Customer]
        Contract[Contract]
        Order[Order]
        OrderLine[Order Line]
        SKU[SKU/Offering]
    end
    
    subgraph Entitlements["Entitlements Service (Bridge)"]
        Entitlement[Entitlement]
        EntItem[Entitlement Item]
        Policy[Policy Engine]
    end
    
    subgraph Technical["Technical Install Base (Runtime)"]
        Instance[Product Instance]
        Device[Device]
        Deployment[Deployment]
    end
    
    subgraph Service["Service Install Base (Support)"]
        Asset[Service Asset]
        SvcContract[Service Contract]
        SLA[SLA]
        SvcEvent[Service Event]
    end
    
    subgraph Usage["Usage & Telemetry"]
        UsageRec[Usage Record]
        EvalLog[Evaluation Log]
        Telemetry[Telemetry Data]
    end
    
    subgraph AI["AI & Optimization"]
        Analytics[Analytics Engine]
        Anomaly[Anomaly Detection]
        Recommendations[Entitlement Recommendations]
    end
    
    Customer --> Contract
    Contract --> OrderLine
    OrderLine --> SKU
    OrderLine --> Entitlement
    Contract --> Entitlement
    Entitlement --> EntItem
    Entitlement --> Policy
    
    Customer --> Instance
    Customer --> Device
    Customer --> Asset
    
    Instance --> UsageRec
    Device --> UsageRec
    Instance -.enforces.-> EntItem
    Device -.enforces.-> EntItem
    
    Asset --> Device
    SvcContract --> Asset
    SLA --> SvcContract
    Contract --> SvcContract
    Asset --> SvcEvent
    
    UsageRec --> Analytics
    Telemetry --> Analytics
    EvalLog --> Analytics
    Analytics --> Anomaly
    Analytics --> Recommendations
    Recommendations -.optimizes.-> Entitlement
    
    Policy -.checks.-> EvalLog
    
    style Commercial fill:#e1f5ff
    style Entitlements fill:#fff4e1
    style Technical fill:#e8f5e9
    style Service fill:#f3e5f5
    style Usage fill:#fce4ec
    style AI fill:#fff9c4
```

---

## Detailed Entity Relationship Diagram

```mermaid
erDiagram
    CUSTOMER ||--o{ CONTRACT : "has"
    CUSTOMER ||--o{ PRODUCT_INSTANCE : "owns"
    CUSTOMER ||--o{ DEVICE : "owns"
    CUSTOMER ||--o{ SERVICE_ASSET : "owns"
    CUSTOMER ||--o{ USAGE_RECORD : "generates"
    CUSTOMER ||--o{ ENTITLEMENT : "receives"
    
    CONTRACT ||--o{ ORDER_LINE : "contains"
    CONTRACT ||--o{ ENTITLEMENT : "generates"
    CONTRACT ||--o{ SERVICE_CONTRACT : "spawns"
    
    ORDER_LINE ||--o{ ENTITLEMENT : "creates"
    
    ENTITLEMENT ||--o{ ENTITLEMENT_ITEM : "contains"
    ENTITLEMENT ||--o{ EVAL_LOG : "tracked_in"
    
    PRODUCT_INSTANCE ||--o{ USAGE_RECORD : "produces"
    PRODUCT_INSTANCE ||--o{ EVAL_LOG : "checked_against"
    
    DEVICE ||--o{ USAGE_RECORD : "produces"
    DEVICE ||--o{ EVAL_LOG : "checked_against"
    DEVICE ||--|| SERVICE_ASSET : "linked_to"
    
    SERVICE_ASSET ||--o{ SERVICE_EVENT : "experiences"
    SERVICE_ASSET ||--|| SERVICE_CONTRACT : "covered_by"
    
    SERVICE_CONTRACT ||--|| SLA : "references"
    
    SLA ||--o{ SERVICE_CONTRACT : "defines"
    
    CUSTOMER {
        uuid customer_id PK
        string name
        string customer_type
        string region
        string industry
        string status
    }
    
    CONTRACT {
        uuid contract_id PK
        uuid customer_id FK
        uuid legal_entity_id FK
        date start_date
        date end_date
        string status
        string terms_ref
    }
    
    ORDER_LINE {
        uuid order_line_id PK
        uuid order_id FK
        string sku_id FK
        integer quantity
        string pricing_model
        date start_date
        date end_date
    }
    
    ENTITLEMENT {
        uuid entitlement_id PK
        uuid customer_id FK
        uuid contract_id FK
        uuid order_line_id FK
        string subject_type
        uuid subject_id
        string status
        date valid_from
        date valid_to
    }
    
    ENTITLEMENT_ITEM {
        uuid entitlement_item_id PK
        uuid entitlement_id FK
        string feature_code
        string right_type
        string value
        string unit
        string scope
    }
    
    PRODUCT_INSTANCE {
        uuid instance_id PK
        uuid customer_id FK
        string product_id
        string deployment_type
        string environment
        string region
        string status
    }
    
    DEVICE {
        uuid device_id PK
        uuid customer_id FK
        uuid asset_id FK
        string model
        string serial_number
        string firmware_version
        string connectivity
    }
    
    SERVICE_ASSET {
        uuid asset_id PK
        uuid customer_id FK
        uuid device_id FK
        string product_family
        string model
        string serial_number
        date install_date
        string status
    }
    
    SERVICE_CONTRACT {
        uuid service_contract_id PK
        uuid customer_id FK
        uuid contract_id FK
        uuid sla_id FK
        date start_date
        date end_date
    }
    
    SLA {
        uuid sla_id PK
        string name
        string response_time
        string resolution_time
        string coverage_hours
        json penalties
    }
    
    SERVICE_EVENT {
        uuid service_event_id PK
        uuid asset_id FK
        string event_type
        timestamp opened_at
        timestamp closed_at
        string severity
        string root_cause
        string outcome
        string notes_ref
    }
    
    USAGE_RECORD {
        uuid usage_id PK
        uuid customer_id FK
        uuid instance_id FK
        uuid device_id FK
        string feature_code
        number quantity
        string unit
        timestamp window_start
        timestamp window_end
    }
    
    EVAL_LOG {
        uuid eval_id PK
        timestamp timestamp
        string subject_type
        uuid subject_id
        uuid entitlement_id FK
        string decision
        string reason_code
        json limits_remaining
        string policy_version
        uuid correlation_id
    }
```

---

## Commercial Domain Detail

```mermaid
graph LR
    subgraph Commercial Domain
        C[Customer]
        LE[Legal Entity]
        CT[Contract]
        O[Order]
        OL[Order Line]
        SKU[SKU/Offering]
        SUB[Subscription]
    end
    
    C -->|1:M| CT
    LE -->|1:M| CT
    CT -->|1:M| O
    O -->|1:M| OL
    OL -->|M:1| SKU
    OL -->|1:1| SUB
    
    C -->|has| LE
    
    style C fill:#4285f4,color:#fff
    style CT fill:#34a853,color:#fff
    style OL fill:#fbbc04,color:#000
    style SKU fill:#ea4335,color:#fff
```

### Commercial to Entitlements Flow

```mermaid
sequenceDiagram
    participant CRM as CRM/ERP
    participant Contract
    participant OrderLine
    participant EntSvc as Entitlements Service
    participant Entitlement
    participant EntItem as Entitlement Items
    
    CRM->>Contract: Create Contract
    Contract->>OrderLine: Add Order Lines
    OrderLine->>EntSvc: Trigger Entitlement Generation
    EntSvc->>Entitlement: Create Entitlement
    Entitlement->>EntItem: Generate Entitlement Items
    EntItem-->>EntSvc: Return Feature Rights
    EntSvc-->>OrderLine: Confirm Entitlement Created
```

---

## Entitlements Domain Detail

```mermaid
graph TB
    subgraph "Entitlements Service (Runtime Bridge)"
        ENT[Entitlement]
        ITEM[Entitlement Item]
        POL[Policy Engine]
        CACHE[Entitlement Cache]
    end
    
    subgraph "Inputs"
        CUST[Customer ID]
        INST[Instance ID]
        DEV[Device ID]
        FEAT[Feature Code]
    end
    
    subgraph "Outputs"
        ALLOW[Allow/Deny]
        QUOTA[Quota Remaining]
        REASON[Reason Code]
    end
    
    CUST --> ENT
    INST --> ENT
    DEV --> ENT
    ENT --> ITEM
    ITEM --> POL
    FEAT --> POL
    
    POL --> ALLOW
    POL --> QUOTA
    POL --> REASON
    
    ENT -.caches.-> CACHE
    CACHE -.fast_check.-> POL
    
    style ENT fill:#ff9800,color:#fff
    style POL fill:#9c27b0,color:#fff
    style CACHE fill:#00bcd4,color:#fff
```

### Runtime Entitlement Check Flow

```mermaid
sequenceDiagram
    participant App as Product Runtime
    participant Cache as Entitlement Cache
    participant EntAPI as Entitlement API
    participant Policy as Policy Engine
    participant DB as Entitlement DB
    participant Log as Evaluation Log
    
    App->>Cache: Check Feature Access
    alt Cache Hit
        Cache-->>App: Return Cached Decision
    else Cache Miss
        Cache->>EntAPI: Request Entitlement
        EntAPI->>DB: Query Entitlements
        DB-->>EntAPI: Return Entitlement Items
        EntAPI->>Policy: Evaluate Policy
        Policy-->>EntAPI: Decision (Allow/Deny)
        EntAPI->>Log: Record Evaluation
        EntAPI->>Cache: Update Cache
        EntAPI-->>App: Return Decision
    end
    
    App->>App: Execute or Block Feature
```

---

## Technical Domain Detail

```mermaid
graph TB
    subgraph "Technical Install Base"
        INST[Product Instance]
        DEV[Device]
        DEPLOY[Deployment]
        ENV[Environment]
        VER[Version]
    end
    
    subgraph "Lifecycle"
        PROV[Provisioned]
        ACT[Activated]
        UPG[Upgraded]
        PATCH[Patched]
        DECOM[Decommissioned]
    end
    
    INST -->|deployed_as| DEPLOY
    INST -->|runs_in| ENV
    INST -->|has| VER
    DEV -->|hosts| INST
    
    INST -.lifecycle.-> PROV
    PROV --> ACT
    ACT --> UPG
    UPG --> PATCH
    PATCH --> DECOM
    
    style INST fill:#4caf50,color:#fff
    style DEV fill:#8bc34a,color:#000
    style DEPLOY fill:#cddc39,color:#000
```

### Technical Instance Lifecycle

```mermaid
stateDiagram-v2
    [*] --> Provisioned: Deploy Request
    Provisioned --> Activated: Activation
    Activated --> Running: Start
    Running --> Upgraded: Version Update
    Upgraded --> Running: Restart
    Running --> Patched: Security Patch
    Patched --> Running: Restart
    Running --> Suspended: Entitlement Expired
    Suspended --> Running: Entitlement Renewed
    Running --> Decommissioned: Retire
    Suspended --> Decommissioned: Retire
    Decommissioned --> [*]
    
    Running --> Failed: Error
    Failed --> Running: Recovery
    Failed --> Decommissioned: Unrecoverable
```

---

## Service Domain Detail

```mermaid
graph TB
    subgraph "Service Install Base"
        ASSET[Service Asset]
        SVCC[Service Contract]
        SLA[SLA]
        EVENT[Service Event]
        TECH[Technician]
        LOC[Location/Site]
    end
    
    subgraph "Event Types"
        INC[Incident]
        MAINT[Maintenance]
        REP[Repair]
        CAL[Calibration]
        INSP[Inspection]
    end
    
    ASSET -->|covered_by| SVCC
    SVCC -->|references| SLA
    ASSET -->|generates| EVENT
    EVENT -->|assigned_to| TECH
    ASSET -->|located_at| LOC
    
    EVENT -.type.-> INC
    EVENT -.type.-> MAINT
    EVENT -.type.-> REP
    EVENT -.type.-> CAL
    EVENT -.type.-> INSP
    
    style ASSET fill:#9c27b0,color:#fff
    style SVCC fill:#673ab7,color:#fff
    style SLA fill:#3f51b5,color:#fff
    style EVENT fill:#2196f3,color:#fff
```

### Service Event Flow

```mermaid
sequenceDiagram
    participant Asset as Service Asset
    participant Monitor as Monitoring System
    participant Incident as Incident Mgmt
    participant Tech as Field Technician
    participant SLA as SLA Tracker
    participant Log as Service Event Log
    
    Asset->>Monitor: Health Check Alert
    Monitor->>Incident: Create Incident
    Incident->>SLA: Check SLA Timer
    SLA-->>Incident: SLA Response Time
    Incident->>Tech: Assign Ticket
    Tech->>Asset: Perform Service
    Tech->>Incident: Update Status
    Incident->>Log: Record Service Event
    Log-->>Asset: Update Asset Status
    Asset->>Monitor: Health Restored
```

---

## Usage & Telemetry Flow

```mermaid
graph TB
    subgraph "Data Sources"
        INST[Product Instance]
        DEV[Device]
        APP[Application]
    end
    
    subgraph "Collection Layer"
        TELE[Telemetry Agent]
        METER[Usage Metering]
        LOG[Event Logs]
    end
    
    subgraph "Processing"
        AGGR[Aggregation]
        RATE[Rating Engine]
        ENR[Enrichment]
    end
    
    subgraph "Consumers"
        BILL[Billing]
        ENT[Entitlements]
        AI[AI/Analytics]
        COMP[Compliance]
    end
    
    INST --> TELE
    DEV --> TELE
    APP --> METER
    APP --> LOG
    
    TELE --> AGGR
    METER --> AGGR
    LOG --> AGGR
    
    AGGR --> RATE
    RATE --> ENR
    
    ENR --> BILL
    ENR --> ENT
    ENR --> AI
    ENR --> COMP
    
    style TELE fill:#ff5722,color:#fff
    style AGGR fill:#ff9800,color:#fff
    style RATE fill:#ffc107,color:#000
```

### Usage to Billing Flow

```mermaid
sequenceDiagram
    participant Instance as Product Instance
    participant Telemetry
    participant UsageDB as Usage Database
    participant Rating as Rating Engine
    participant Entitlement
    participant Billing
    participant Invoice
    
    Instance->>Telemetry: Emit Usage Event
    Telemetry->>UsageDB: Store Usage Record
    UsageDB->>Rating: Aggregate & Rate
    Rating->>Entitlement: Check Quota
    Entitlement-->>Rating: Quota Status
    Rating->>Billing: Generate Charge
    Billing->>Invoice: Create Invoice Line
    Invoice-->>Billing: Invoice Sent
```

---

## AI & Optimization Layer

```mermaid
graph TB
    subgraph "Data Inputs"
        USAGE[Usage Records]
        EVAL[Evaluation Logs]
        TELE[Telemetry]
        SVC[Service Events]
    end
    
    subgraph "AI/ML Models"
        ANOM[Anomaly Detection]
        PRED[Predictive Analytics]
        OPTIM[Optimization Engine]
        REC[Recommendation Engine]
    end
    
    subgraph "Outputs"
        ALERT[Alerts]
        INSIGHT[Insights]
        ENTOPT[Entitlement Optimization]
        PROD[Product Recommendations]
    end
    
    USAGE --> ANOM
    EVAL --> ANOM
    TELE --> PRED
    SVC --> PRED
    
    ANOM --> OPTIM
    PRED --> OPTIM
    OPTIM --> REC
    
    ANOM --> ALERT
    PRED --> INSIGHT
    REC --> ENTOPT
    REC --> PROD
    
    style ANOM fill:#e91e63,color:#fff
    style PRED fill:#9c27b0,color:#fff
    style OPTIM fill:#673ab7,color:#fff
    style REC fill:#3f51b5,color:#fff
```

### AI-Driven Optimization Flow

```mermaid
sequenceDiagram
    participant Usage as Usage Platform
    participant ML as ML Model
    participant Analytics as Analytics Engine
    participant Recommender as Recommendation Engine
    participant Entitlement as Entitlement Service
    participant CSM as Customer Success
    
    Usage->>ML: Stream Usage Data
    ML->>Analytics: Detect Pattern
    Analytics->>Recommender: Identify Optimization Opportunity
    
    alt Under-Utilization
        Recommender->>CSM: Suggest Downgrade/Optimize
        CSM->>Entitlement: Adjust Entitlement
    else Over-Utilization
        Recommender->>CSM: Suggest Upgrade
        CSM->>Entitlement: Expand Entitlement
    else Anomaly Detected
        Recommender->>CSM: Alert Fraud/Misuse
        CSM->>Entitlement: Suspend or Investigate
    end
    
    Entitlement-->>Usage: Updated Limits Applied
```

---

## Complete Data Flow: End-to-End

```mermaid
graph TB
    Start[Customer Purchase] --> Contract[Create Contract]
    Contract --> Order[Create Order]
    Order --> OrderLine[Add Order Lines]
    OrderLine --> EntGen[Generate Entitlements]
    
    EntGen --> Deploy[Deploy Instance]
    Deploy --> Runtime[Runtime Check]
    Runtime --> EntCheck{Entitlement Valid?}
    
    EntCheck -->|Yes| Allow[Allow Access]
    EntCheck -->|No| Deny[Deny Access]
    
    Allow --> Usage[Generate Usage]
    Usage --> Meter[Meter Usage]
    Meter --> Bill[Create Bill]
    
    Usage --> AI[AI Analysis]
    AI --> Optimize[Optimize Entitlements]
    Optimize --> EntGen
    
    Deploy --> Service[Service Asset]
    Service --> Monitor[Monitor Health]
    Monitor --> Event{Issue Detected?}
    Event -->|Yes| Incident[Create Incident]
    Event -->|No| Monitor
    
    Incident --> Resolve[Resolve Issue]
    Resolve --> Service
    
    Bill --> Invoice[Generate Invoice]
    Invoice --> End[Customer Pays]
    
    style Start fill:#4285f4,color:#fff
    style EntGen fill:#fbbc04,color:#000
    style Runtime fill:#34a853,color:#fff
    style Bill fill:#ea4335,color:#fff
    style End fill:#4285f4,color:#fff
```

---

## Canonical Identifier Flow

```mermaid
graph LR
    subgraph "Identifiers"
        CUST_ID[customer_id]
        CONTRACT_ID[contract_id]
        INSTANCE_ID[instance_id]
        DEVICE_ID[device_id]
        ASSET_ID[asset_id]
        ENT_ID[entitlement_id]
    end
    
    subgraph "Commercial"
        C[Customer]
        CT[Contract]
    end
    
    subgraph "Entitlements"
        E[Entitlement]
    end
    
    subgraph "Technical"
        I[Instance]
        D[Device]
    end
    
    subgraph "Service"
        A[Asset]
    end
    
    CUST_ID --> C
    CUST_ID --> CT
    CUST_ID --> E
    CUST_ID --> I
    CUST_ID --> D
    CUST_ID --> A
    
    CONTRACT_ID --> CT
    CONTRACT_ID --> E
    
    ENT_ID --> E
    
    INSTANCE_ID --> I
    DEVICE_ID --> D
    ASSET_ID --> A
    
    D -.links.-> A
    I -.belongs.-> C
    
    style CUST_ID fill:#4285f4,color:#fff,stroke:#000,stroke-width:3px
    style CONTRACT_ID fill:#34a853,color:#fff,stroke:#000,stroke-width:3px
    style ENT_ID fill:#fbbc04,color:#000,stroke:#000,stroke-width:3px
```

---

## Summary: Key Relationship Patterns

### 1. One-to-Many Relationships
- Customer → Contracts (1:M)
- Customer → Entitlements (1:M)
- Customer → Product Instances (1:M)
- Customer → Service Assets (1:M)
- Contract → Order Lines (1:M)
- Entitlement → Entitlement Items (1:M)
- Product Instance → Usage Records (1:M)
- Service Asset → Service Events (1:M)

### 2. Many-to-One Relationships
- Contracts → Customer (M:1)
- Entitlements → Contract (M:1)
- Product Instances → Customer (M:1)
- Usage Records → Product Instance (M:1)
- Service Events → Service Asset (M:1)

### 3. One-to-One Relationships
- Device ↔ Service Asset (1:1)
- Service Contract ↔ SLA (1:1)

### 4. Many-to-Many Relationships (via Junction Tables)
- Customers ↔ Products (via Contracts/Orders)
- Entitlements ↔ Features (via Entitlement Items)
- Service Assets ↔ Technicians (via Service Events)

---

## Diagram Legend

### Node Colors
- **Blue**: Commercial Domain
- **Orange/Yellow**: Entitlements Domain
- **Green**: Technical Domain
- **Purple**: Service Domain
- **Pink**: Usage & Telemetry
- **Light Yellow**: AI & Analytics

### Relationship Types
- **Solid Arrow** (→): Direct foreign key relationship
- **Dotted Arrow** (-.->): Logical/reference relationship
- **Bold Arrow** (==>): Primary data flow
- **Dashed Line** (--): Async/event-driven relationship

---

## How to Use These Diagrams

1. **For Developers**: Use the detailed ERD to understand database schema and foreign key relationships
2. **For Architects**: Use the high-level architecture to understand system boundaries and data flows
3. **For Product Managers**: Use the domain-specific diagrams to understand business processes
4. **For Operations**: Use the lifecycle and flow diagrams to understand runtime behavior
5. **For Security/Compliance**: Use the evaluation log and audit trail diagrams

---

## Next Steps

1. Implement database schema based on ERD
2. Design APIs following the data flow patterns
3. Set up event streaming for real-time updates
4. Implement caching strategy for entitlement checks
5. Build monitoring dashboards for each domain
6. Create data governance policies
7. Set up automated reconciliation between domains
