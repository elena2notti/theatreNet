# TheatreNet — Knowledge Graph for Performing Arts Archives

TheatreNet is a knowledge graph infrastructure for modelling and exploring performing arts archives.

The project integrates heterogeneous archival datasets from Teatro Regio of Torino and Fondazione I Teatri of Reggio Emilia into a unified graph environment, enabling relational, temporal, and network-based exploration of works, productions, performances, and people.

It combines semantic modeling principles with a Neo4j property graph implementation optimized for interactive querying and visualization.

---

## Project Overview

Performing arts archives present specific structural challenges:

- Layered entities (work → production → performance)
- Event-centric documentation
- Multiple authorship roles
- Hybrid administrative/editorial data
- Identifier inconsistencies across systems

TheatreNet addresses these challenges through:

- Internal data normalization
- Identifier harmonization
- Entity reconciliation across sources
- Semantic modeling inspired by CIDOC CRM and FRBRoo
- Operational implementation in Neo4j

The result is a graph that preserves provenance while enabling efficient exploration and querying.

---

## Repository Structure

The repository is organized into the following main directories:

### `website/`
Web interface for graph exploration.

Includes:
- Home page
- Entity detail pages
- Visualizations
- Timeline navigation
- Live Cypher query interface

This layer focuses on usability and non-technical access to the graph.

---

### `property_graph/`
Neo4j-oriented implementation layer.

Contains:
- Cypher-based ingestion logic
- Graph construction scripts
- Vector embedding integration
- Node reconciliation and merge strategies

This is the operational layer of the project.

---

### `semantic_graph/`
RDF and ontology-oriented layer.

Contains:
- RDF generation scripts
- Ontological modeling logic
- Neosemantics upload configuration

This layer represents the conceptual semantic foundation of the graph.

---

### `regio/`
Dataset preparation and modeling workflows specific to Teatro Regio.

---

### `fondazione/`
Dataset preparation and modeling workflows specific to Fondazione I Teatri.

---

### `normalization/`
Data cleaning and preprocessing layer.

Includes:
- Normalization routines
- Identifier corrections
- Reconciliation logic
- Data consistency validation

---

## Technical Stack

- Neo4j (Property Graph)
- Cypher
- RDF generation layer
- Neosemantics (RDF → Neo4j bridge)
- Vector embeddings for similarity-based reconciliation
- Static HTML/JS front-end interface

---

## Methodological Approach

The project follows a layered architecture:

1. Data Cleaning & Normalization  
2. Semantic Modeling (RDF layer)  
3. Graph Upload & Configuration  
4. Property Graph Optimization  
5. Interactive Exploration Interface  

This separation allows the system to maintain ontological rigor while supporting performant and user-friendly querying.

---

## Author
Elena Binotti  
LM Digital Humanities and Digital Knowledge  
University of Bologna  
In collaboration with Promemoria Group

---

## Data Disclaimer

Archival datasets referenced in this project are not included in the repository.
They remain subject to the policies and intellectual property of their respective institutions (Teatro Regio and Fondazione I Teatri).
