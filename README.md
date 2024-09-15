# Key-Value Database Parser

This project is a Java-based key-value database parser. It includes various classes to handle different data types and operations, such as strings, lists, sets, sorted sets, and hashes. The project also includes functionality for handling expiry times and LZF compression.

## Project Structure

- `src/main/java/com/zaaim/kv/db/parser/`
  - `CommandExecutor.java`: Implements various Redis command executions.
  - `DbParser.java`: Parses different data types from the database.
  - `KeyValuePair.java`: Represents a key-value pair with an optional expiry time.
  - `LZF.java`: Handles LZF compression and decompression.

## Requirements

- Java 21 or higher
- Maven

## Building the Project

To build the project, run the following command:

```sh
mvn clean install
```
## Running the Project
To run the project, execute the Main class. You can do this from your IDE (e.g., IntelliJ IDEA) or from the command line:

```sh
java -cp target/your-jar-file.jar com.zaaim.kv.db.Main
```

## Usage
# Parsing a Database
The DbParser class provides methods to parse different data types from the database. Example usage:
    
```java
DbParser parser = new DbParser();
parser.readEntry(0); // Reads a string entry
parser.readEntry(1); // Reads a list entry
// Add more as needed
```
# Handling Key-Value Pairs
The KeyValuePair class represents a key-value pair with an optional expiry time. Example usage:

```java
KeyValuePair kvp = new KeyValuePair();
kvp.setKey("exampleKey");
kvp.setValue("exampleValue");
kvp.setExpiryTime(new Timestamp(System.currentTimeMillis() + 60000)); // Expires in 1 minute
```

# LZF Compression
The LZF class provides methods to compress and decompress data using the LZF algorithm. Example usage:

```java
byte[] input = ...; // Your input data
byte[] output = new byte[expectedOutputLength];
LZF.expand(input, 0, output, 0, expectedOutputLength);
```
