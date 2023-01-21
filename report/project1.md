## Project 1 Report


### 1. Basic information
 - Team #: N/A
 - Github Repo Link: https://github.com/UCI-Chenli-teaching/cs222p-winter23-grushad
 - Student 1 UCI NetID: gdharod
 - Student 1 Name: Grusha Jayesh Dharod
 

### 2. Internal Record Format
- Show your record format design.
    - The format can be divided into 3 parts, 1st part is the metadata of the record. 2nd part has the offset values for each field and then is the actual data.
    - For storing the record, the first 4 bytes are using for indicating the number of fields in the records.
    - The next data that is stored is regarding the null values in the records. The number of bytes required for storing this is calculated as ceil(numFields / 8). Each bit is need for 1 field. If 1st bit is 1, this indicates that 1st field is NULL.
    - Next the offset for each field is stored. We store the byte where each field ends as we can calculate the start.
    - So the offset for each field will be stored as an integer and thus will occupy 4 bytes each. Thus total size for offset will 4 * numFields.
    - After storing all the offsets we store the actual data.

- Describe how you store a null field.
  - For null fields, starting 5th byte we store information regarding if the field is null.
  - In the offset value of the null field we store -1 instead of the actual offset to indicate that it is null
  - we don't actually store the null value in the data portion of the record.


- Describe how you store a VarChar field.
  - For varchar, we are given the length of the data in the record descriptor.
  - The length can be calculated as the number of characters as 1 character is 1 byte.
  - Using the length value in the record descriptor field, we use it to store it in our record. We need not store the length of varchar in our record as we already store the offset for each field at the start.


- Describe how your record design satisfies O(1) field access.
  - This is done with the help of offsets that we are storing after the metadata of the record.
  - We know the bytes from where the actual data starts, we can calculate it as follows: 4 bytes for storing the number of fields at the start, ceil(numFields/8) bytes for storing null value information, offset data for each field - i.e. numFields * 4 bytes.
  - We can know the order of the field using the record descriptor and thus we can calculate the offset to check for.
  - For instance, if we want the 2nd field, we can calculate it as follows: 4 + ceil(numFields / 8) + ((order of attribute - 1) * 4)
  - we also know where the previous field i.e. 1st field ends by checking its offset. Now we have both the start and end of the 2nd field.
  - Thus, we don't have to traverse through all the fields to access one field. We can do it O(1) access time.


### 3. Page Format
- Show your page format design.
  - The page format uses the start of the page to store the actual records and the end of the page to store the metadata about the page and records.
  - Each record stored on a page has a record id which has 2 attributes: page number and slot number. This indicates which page the record is stored on and the slot number it occupies on that page.
  - The records are stored in a sequential manner starting from the start of the page.
  - The metadata that is stored on the page is stored starting from the end of the page.
  - The metadata consist of number of free bytes on the page, number of records on the page. This is followed by a slot directory. 
  - The slot directory has slots equivalent to the number of records stored on the page. It stores the offset and the length of each record.
  - There is free space in the middle, i.e. between the actual records data and the metadata


- Explain your slot directory design if applicable.
  - The slot directory has 2 attributes stored, offset value and length of the record.
  - Offset value indicates the start of the record and using the length we can parse the entire record.


### 4. Page Management
- Show your algorithm of finding next available-space page when inserting a record.
  - For finding the next available page, first I have checked if the current page has free bytes >= the record size(including the metadata for the record).
  - If yes, then we insert the record on the current page.
  - If not then we start from the beginning of the file and read each page and check if each page has free bytes to fit the new record.\
  - We insert the record at the first page that we can fit the record into.
  - If all the pages are full then we append a new page to the file and insert our new record in the new page.


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