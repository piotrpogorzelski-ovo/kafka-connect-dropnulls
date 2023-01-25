This SMT removes all fields which has value of null.
Suppossed to be used with MongoDB source and BigQuery sink and a schema with compatibility set to "none", to flawlesly stream data from Mongo to BigQuery, assuming that each field have single type across all documents within a collection (and time).

We assume that we will ony add new field, and field will not change its type, so BigQuery sink connector will only add new columns to existing tables.
In destination BigQuery table, all missing fields of a document will have value of a null, but only after a first message with this field having non null value.

The process should autodescover the structure of the document and extend the final table with columns as new nonnull fields appear in document.

May reasult in a many schemas generated for the topic (combination of all possible fields).

Supports only records with schemas.

Properties:

Example on how to add to your connector:
```
transforms=dropnulls
transforms.dropnulls.type=com.github.lepoitr.kafka.connect.smt.DropNulls$Value

```


ToDO
* add recursion
* drop also fields with value of empty array 
* handle records without schemas

