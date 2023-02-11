## Project 2 Report


### 1. Basic information
- Team #: N/A
- Github Repo Link: https://github.com/UCI-Chenli-teaching/cs222p-winter23-grushad
- Student 1 UCI NetID: gdharod
- Student 1 Name: Grusha Jayesh Dharod

### 2. Meta-data
- Show your meta-data design (Tables and Columns table) and information about each column.
Tables table:
- table-id: used for mapping between tables table and columns table
- table-name: uniquely identifies a table
- file-name: indicates file name where the data is physically stored on the disk
- table-type: indicates if the table is of System or User type; this field is used to avoid modifications to the catalog tables by the user

Columns table:
- table-id: used to indicate which table the columns belong to
- column-name: name of the column
- column-type: data type stored in the column
- column-length: length of the data that would be stored in the column
- column-position: position of the attribute in the table

### 3. Internal Record Format (in case you have changed from P1, please re-enter here)
- Show your record format design.
  - The format is divided into 4 parts: tombstone data, metadata, offset data, and actual data
  - The first byte indicates whether the record is a tombstone or not. Followed by this is the RID field which stores the pointer to the next record if the current record is a tombstone. RID uses 8 bytes: 4 bytes for pagenumber and 4 for slot number
  - Next for the metadata of the record, we store the null indicator bytes after the tombstone data. After the null indicator, the number of fields in the record are stored.
  - Next the offset for each field is stored. We store the byte where each field ends as we can calculate the start.
  - So the offset for each field will be stored as an integer and thus will occupy 4 bytes each. Thus total size for offset will 4 * numFields.
  - After storing all the offsets we store the actual data.


- Describe how you store a null field.
  - The format is same as that of in P1, it hasn't changed


- Describe how you store a VarChar field.
  - For varchar the format still remains the same from P1.



- Describe how your record design satisfies O(1) field access.
  - The record design still satisfies O(1) access time as we are storing the offset for each field which allows us to directly jump to any field instead of going through all the bytes.
  - The only change is the addition of the tombstone indicator and RID to the record format
  - This doesn't affect the access time as we know the bytes each field occupies thus we can skip those conveniently.

    
### 4. Page Format (in case you have changed from P1, please re-enter here)
- Show your page format design.
  - The page format hasn't changed from P1. The earlier design still continues.


- Explain your slot directory design if applicable.
  - slot directory structure still remains the same from P1



### 5. Page Management (in case you have changed from P1, please re-enter here)
- How many hidden pages are utilized in your design?
  - the design is same as P1


- Show your hidden page(s) format design if applicable
  - this design is same as P1


### 6. Describe the following operation logic.
- Delete a record
  - To delete a record, first the record is located using the RID and first we check if this RID was earlier deleted, if we are trying to delete a deleted record, error is returned. 
  - Then it is checked if the record is a tombstone or an actual record.
  - If it is a tombstone then the RID is calculated using the RID field in the record and delete operation is called on that RID.
  - If it is not a tombstone then, calculate the offset and length of the last record stored on that page
  - Using that calculate the number of bytes that need to be shifted to the left.
  - Use memmove to copy all the records to the right of the current record and update all their offsets in the slot directory.
  - The offsets will reduce by the length of the current record.
  - For the current record's slot directory, set the length as 0 to indicate that the record is deleted and the slot is unused so that it can be used if new record is inserted.
  - Update the page metadata i.e. number of free bytes on the page and increase it by the length of the deleted record.

- Update a record
  - For update record, we check using the RID if this points to a deleted record i.e. length = 0; if yes then return error.
  - Then we create the record in the format that we follow using the new data that is passed to the method.
  - we calculate the length and offset of the last record stored on the page which would help in calculating the number of bytes that need to be shifted
  - Next we check if the current record is a tombstone or an actual record.
  - If it is an actual record, we need to consider 3 cases:
    - the length of new record <= length of old record
      - in this case, we simply move the records to the right of the current record to the left to avoid holes and copy the updated records
      - we update the page metadata i.e. free bytes
      - we update slot directory i.e. offsets for records that have been moved to the left 
    - length of new record > length of old record and the current page holds enough free space to adjust new record
      - In this case, we move the records to the right of the current record towards the right to make space for the updated record
      - Then we copy the updated record.
      - then we update the metadata and slot directory of the records that are shifted.
    - length of new record > length of old record and there's not enough free space on the current page to update new record
      - in this case we need to create the record at the current page as a tombstone to indicate that the actual data is not stored here.
      - we update the tombstone field in the record.
      - we find a new page with enough free space to insert the updated record. 
      - The record is written to that page and the slot number is returned.
      - once we find the page number and slot number to insert the updated record, we update the RID on the current page.
      - finally, we update the page metadata.

    
- Scan on normal records
  - we start scanning from the start of the file i.e. pagenum = 0 and slotnum = 1
  - we read the attribute on which condition is applied, if the value matches, then read the required attrbutes 
  - we continue reading records until we reach end of the file.
  - we keep incrementing slotnum until the all the records on the page are read.
  - then we increment pagenum; pagenum is incremented until all the pages are read.
  - we keep only 1 page in memory at a time.

- Scan on deleted records
  - Deleted records are indicated as having length of 0 in the slot directory.
  - thus if we come across RID which is a deleted record, then the length in the slot directory would be 0 and thus we would ignore it.

- Scan on updated records
  - while scanning records, we would check if the record is a tombstone or not, if it is a tombstone, we would ignore it.
  - In a sequential scan to fetch all the records, we would only read records that are not tombstones.
  - This would ensure we are not reading same records more than once. 
  - Since we only need to read the data and not the RID, we would ignore the records which are marked as tombstone.


### 7. Implementation Detail
- Other implementation details goes here.



### 8. Member contribution (for team of two)
- Explain how you distribute the workload in team.



### 9. Other (optional)
- Freely use this section to tell us about things that are related to the project 1, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)