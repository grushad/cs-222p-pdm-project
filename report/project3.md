## Project 3 Report


### 1. Basic information
- Team #: N/A
- Github Repo Link: https://github.com/UCI-Chenli-teaching/cs222p-winter23-grushad
- Student 1 UCI NetID: gdharod
- Student 1 Name: Grusha Jayesh Dharod


### 2. Meta-data page in an index file
- Show your meta-data page of an index design if you have any. 
The Hidden page in an index file stores the read, write, and append page counters. Apart from that, the first page of the index file is the dummy root of the tree. This has a pointer to the actual root of the tree.


### 3. Index Entry Format
- Show your index entry design (structure). 
The index entry has basic metadata information. 
  - entries on internal nodes:  
  For internal nodes, we store the key and the pointer to the right child for each node. Just that the first entry would have the pointer to the left child.
  - entries on leaf nodes:
  For leaf nodes, we would have the key, number of RIDs matching the key, followed by list of RIDs. In case of varchar keys, we would also have the len of the field at the start to indicate the length of the key. The other fields would remain same.
  For length field - 4 bytes, for key field - 4 bytes in case of int and float and for varchar it would be equal to the length field, for number of RIDs it would be 4 bytes. Each RID would take 6 bytes (4 bytes for page number and 2 for slot number).


### 4. Page Format
- Show your internal-page (non-leaf node) design.
For the page design for internal nodes, we would store the metadata namely an indicator of 1 byte that shows if it is a leaf node or an internal node. Next we have the indicator that shows the number of free bytes which would be of 2 bytes. Then we have the 2 bytes for number of entries stored on that page. Finally we are storing a pointer 4 bytes to the left child of the first node since only the first entry has a pointer to the left node.


- Show your leaf-page (leaf node) design.
For the page design for internal nodes, we would store the metadata namely an indicator of 1 byte that shows if it is a leaf node or an internal node. Next we have the indicator that shows the number of free bytes which would be of 2 bytes. Then we have the 2 bytes for number of entries stored on that page. Finally we are storing a pointer 4 bytes to the right sibling of the leaf node.


### 5. Describe the following operation logic.
- Split
For split, we would find the middle key on the page by checking the number of entries field on the page. Copying the second half of the data on the old page to a new page. Updating metadata on both the pages and creating an entry with the middle key which would be the split key. This would be inserted in the parent node of the old node. This would have its right child set to the new node created.


- Rotation (if applicable)



- Merge/non-lazy deletion (if applicable)



- Duplicate key span in a page
For duplicate keys that span in a page, I have added a number of RIDs field corresponding to a key. Approach #3 as per notes and this would be followed by this list of RIDs. 


- Duplicate key span multiple pages (if applicable)



### 6. Implementation Detail
- Have you added your own module or source file (.cc or .h)? 
  Clearly list the changes on files and CMakeLists.txt, if any.



- Other implementation details:



### 7. Member contribution (for team of two)
- Explain how you distribute the workload in team.



### 8. Other (optional)
- Freely use this section to tell us about things that are related to the project 3, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)
