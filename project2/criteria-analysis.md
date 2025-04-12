# Criteria Analysis for Project 2
## Overview

This document evaluates our data collection against the **10 validation criteria** required for Project 2. Each dataset has been analyzed to determine its adherence to these requirements.

***

## Criteria Evaluation

### 1. Multiple Data Sources
✅ **Met** – Our dataset consists of multiple independently produced sources:

* Crime Data
* Demographics Data
* Health (Mortality & Natality) Data
* Labor Market Data
* State Government Budgets
* US Geographic Codes
These datasets originate from different domains, fulfilling this requirement.

### 2. At Least One Unstructured Source
✅ **Met** – We satisfy this requirement through additional unstructured data .

### 3. Multiple Logical Entities
✅ **Met** – Our dataset contains distinct logical entities:

* **Crime Data:** Overall crime statistics, broken down by type.
* **Health Data:** Mortality and natality indicators by state and year.
* **State Budgets:** Financial breakdowns into multiple expenditure categories.
* **US Geographic Codes:** Location mapping for states.

### 4. Functional Dependency Consistency
✅ **Likely Met** – Most datasets maintain logical consistency:

* **Crime Data:** Crime statistics align with state-year pairs.
* **US Geographic Codes:** Latitude/longitude correspond to unique states.
* **State Budgets:** Each year has a unique expenditure record. Further validation needed across datasets to confirm no inconsistencies.

### 5. Incorrect Data Types Present
✅ **Met (Anomaly Present)** – Incorrect data types exist:

* **Labor Market Data:** Many unnamed columns, indicating possible type mismatches.
* **Demographics Data:** Percentage values stored as text instead of numbers.

### 6. Null Values Stored as Empty Strings or "\N"
✅ **Met (Anomaly Present)** – Null values are improperly stored:

* **State Government Budgets Key:** Contains many "Unnamed" columns.
* **Labor Market Data:** Empty placeholders exist in employment-related fields.

### 7. Multiple Attributes in a Single Field
✅ **Met (Anomaly Present)** – Some fields store multiple values:

* **Demographics Data:** State-level estimates and errors combined in one field.
* **Labor Market Data:** State names combined with employment statistics.

### 8. Lists Stored in a Single Cell
✅ **Met (Anomaly Present)** – Some fields store lists of values:

* **State Government Budgets Key:** Column explanations stored in single fields.
* **Health Data:** Some Month/Period fields contain multiple elements.

### 9. Duplicate Data from Different Sources Using Different Identifiers
✅ **Met (Anomaly Present)** – Different datasets use varying identifiers for the same entity:

* **US Geographic Codes vs. State Budgets:** State names appear in different formats.
* **Demographics vs. Labor Market Data:** Population metrics labeled inconsistently.

### 10. Multiple Logical Entities in One Table
✅ **Met (Anomaly Present)** – Some tables mix multiple entity types:

* **State Budgets:** Budget categories and metadata mixed in one table.
* **Crime Data:** Crime categories stored within the same table rather than separate logical entities.

