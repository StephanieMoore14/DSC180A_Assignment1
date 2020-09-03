# DSC180A_Assignment1

This assignment includes:
- A survey of the data and the context in which it was created (report).
- A description and justification of the data ingestion process in part 3 (report).
- Development of code for ingesting and storing the data for later use (code).

[Here](https://github.com/StephanieMoore14/DSC180A_Assignment1/blob/master/report.pdf) is a link to the report.

Part 1

Introduction to the problem being investigated and description the data being used to approach the problem. A description the investigation into racial discrepancies in traffic stops by the SDSU investigation into the San Diego Police department (referring to the investigation in Suspect Citizens for context).

Addresses the appropriateness of the data design and collection:
- Why is the data appropriate to address the problem?
- What are the potential shortcomings of the data for addressing the problem?
- What data have been used to address this problem in the past? (Historical context).
- Summarize relevant details of the data generating process, describing the population that the data represents, whether that population is relevant to the question at hand, while addressing possible questions of data reliability.

Part 2
Description of the data ingestion process you designed.
- Specifications of where the data originates, addressing legal issues pertaining to access.
- Addresses any data privacy concerns and how your data pipeline handles them.
- Lays out the schema and justify the decisions.
- Addresses the applicability of the pipeline to similar data sources you might anticipate using in your future work on the subject.

Part 3
Creation of a data ingestion pipeline for the result-replication project. The pipeline:
- Ingests Traffic Stops data from the San Diego Open Data Portal into local file(s) on disk, according to best practices.
- All files have the same schema, the ingestion pipeline takes in the year (between 2014 and 2019) as a parameter. Note that data post-2018 is structured differently according to RIPA (Racial and Identity Profiling Act).
