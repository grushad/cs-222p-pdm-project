## Project 1 Report


### 1. Basic information
 - Team #:
 - Github Repo Link: https://github.com/UCI-Chenli-teaching/cs222p-winter23-grushad
 - Student 1 UCI NetID: gdharod
 - Student 1 Name: Grusha Jayesh Dharod
 

### 2. Internal Record Format
- Show your record format design.



- Describe how you store a null field.



- Describe how you store a VarChar field.



- Describe how your record design satisfies O(1) field access.



### 3. Page Format
- Show your page format design.



- Explain your slot directory design if applicable.



### 4. Page Management
- Show your algorithm of finding next available-space page when inserting a record.



- How many hidden pages are utilized in your design?
  -  One hidden page is utilized in my design


- Show your hidden page(s) format design if applicable
  - Hidden page consists of 4 fields each of 4 bytes, i.e. of integer type.
  - The fields that are stored in the hidden page are:
  1. Number of Pages
  2. Read Count
  3. Write Count
  4. Append Count

### 5. Implementation Detail
- Other implementation details goes here.



### 6. Member contribution (for team of two)
- Explain how you distribute the workload in team.



### 7. Other (optional)
- Freely use this section to tell us about things that are related to the project 1, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)