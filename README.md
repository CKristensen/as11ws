# klavaness_case
Klavaness Digital Case

To test the script run on teminal:
- python case.py

Have the current working directory in the same directory as the case.py file.

Unittesting:
Run the following command.
- python test_case.py

CASE:
The team wants data from an application API inside of our application CargoValue. 
The data is stored in a file directory mounted on a file system. 
The file directory contains json files with data that are serialized. 
These must be flattened and aggregated into csv data.

1.	You need to write a script that reads the data 
2.	The script must extract and transform the data into a flattened csv format. One or more files

RESOLUTION:
Solved using a algorithm that tranverses each json file to get the flattened rows with all information.
Concatenates all rows into a dataframe that is saved into a csv file *BlueEnergy18*

Basic Visualization on PowerBI
