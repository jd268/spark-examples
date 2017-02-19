# spark-examples
Example : s3 csv file schema validation

For each incoming csv data file, we are storing name of columns and
their data-types in json file. Everytime we get csv file on s3
landing layer,we validate its schema against predefined schema in
json file. Compare no of columns,column names,column sequence and
data-types of two data-sets

