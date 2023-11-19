package org.example.server.service;

import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.example.entity.Attribute;
import org.example.server.repository.DBMSRepository;
import org.jdom2.Element;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.example.connectionManager.DbConnectionManager.getMongoClient;

public class DBMSService {
    private static DBMSRepository dbmsRepository;
    private static final String regexCreateDatabase = "CREATE\\s+DATABASE\\s+([\\w_]+);";
    private static final String regexDropDatabase = "DROP\\s+DATABASE\\s+([\\w_]+);";
    private static final String regexUseDatabase = "USE\\s+([\\w_]+);";

    private static final String DATABASE_NAME = "mini_dbms";
    private static final String DATABASE_NODE = "DataBase";
    private static final String TABLE_NODE = "Table";
    private static final String TABLE_NAME_ATTRIBUTE = "tableName";
    private static final String DATABASE_NAME_ATTRIBUTE = "dataBaseName";
    private static final String PRIMARY_KEY_NODE = "primaryKey";
    private static final String PRIMARY_KEY_ATTRIBUTE = "pkAttribute";
    private static final String STRUCTURE_NODE = "Structure";
    private static final String ATTRIBUTE_NODE = "Attribute";

    public DBMSService(DBMSRepository dbmsRepository) {
        this.dbmsRepository = dbmsRepository;
    }

    public static void executeCommand(String sqlCommand) throws Exception {
        Pattern pattern = Pattern.compile(regexCreateDatabase);
        Matcher matcher = pattern.matcher(sqlCommand);
        if (matcher.find()) {
            String databaseName = matcher.group(1);
            createDatabase(databaseName);
            return;
        }
        pattern = Pattern.compile(regexDropDatabase);
        matcher = pattern.matcher(sqlCommand);
        if (matcher.find()) {
            String databaseName = matcher.group(1);
            dropDataBase(databaseName);
            return;
        }
        pattern = Pattern.compile(regexUseDatabase);
        matcher = pattern.matcher(sqlCommand);
        if (matcher.find()) {
            String databaseName = matcher.group(1);
            useDataBase(databaseName);
            return;
        }

        Statement statement = CCJSqlParserUtil.parse(sqlCommand);
        if (statement instanceof Insert insert) {
            insert(insert);
        } else if (statement instanceof Delete delete) {
            deleteFromTable(delete
            );
        } else if (statement instanceof CreateTable createTable) {
            createTable(createTable);
        } else if (statement instanceof Update update) {
            update(update);
        } else if (statement instanceof Drop dropTable) {
            dropTable(dropTable);
        } else if (statement instanceof CreateIndex createIndex) {
            createIndex(createIndex);
        } else
            throw new Exception("Eroare parsare comanda");
    }

    public static void update(Update update) throws Exception {
        if (dbmsRepository.getCurrentDatabase().equals(""))
            throw new Exception("No database in use!");


        String tableName = update.getTable().getName();
        Element tableElement = validateTableName(tableName);

        List<Column> updateColumns = update.getColumns();
        List<Expression> values = update.getExpressions();


        List<String> attributes = tableElement.getChild("Structure").getChildren("Attribute").stream().map(line -> line.getAttributeValue("attributeName")).toList();

        validateUpdateColumns(updateColumns, attributes);
        String primaryKey = update.getWhere().toString().split("=")[1].trim();

        String valuesList = computeUpdateKeyValue(attributes, updateColumns, values, Arrays.stream(findRecordValues(tableName, primaryKey).split("#")).toList());
        updateInMongoDb(tableName, primaryKey, valuesList);
    }

    private static void updateInMongoDb(String tableName, String key, String values) throws Exception {
        try (MongoClient client = getMongoClient()) {
            MongoDatabase mongoDatabase = client.getDatabase(DATABASE_NAME);
            MongoCollection<Document> collection = mongoDatabase.getCollection(dbmsRepository.getCurrentDatabase() + tableName);

            Document filter = new Document("_id", key); // Caută înregistrarea după cheia primară
            Document updateDoc = new Document("$set", new Document("values", values)); // Specifică coloana și noua valoare

            collection.updateOne(filter, updateDoc);

        } catch (Exception e) {
            throw new Exception("A apărut o eroare la actualizarea înregistrării.");
        }
    }

    private static String computeUpdateKeyValue(List<String> columns, List<Column> updateColumns1, List<Expression> values, List<String> oldValues) {
        List<String> updateColumns = updateColumns1.stream().map(elem -> elem.getColumnName().toString()).toList();
        int j = 0;
        List<String> othersList = new ArrayList<>();
        for (int i = 1; i < columns.size(); i++) {
            System.out.println(i);
            String column = columns.get(i);

            if (updateColumns.contains(column)) {
                String value = values.get(j).toString();
                j++;
                othersList.add(value);

            } else {
                othersList.add(oldValues.get(j));

            }
        }
        return String.join("#", othersList);
    }

    public static String findRecordValues(String tableName, String primaryKeyValue) throws Exception {
        MongoClient client = getMongoClient();
        MongoDatabase mongoDatabase = client.getDatabase(DATABASE_NAME);
        MongoCollection<Document> collection = mongoDatabase.getCollection(dbmsRepository.getCurrentDatabase() + tableName);
        Document filter = new Document("_id", primaryKeyValue); // Caută înregistrarea după cheia primară
        FindIterable<Document> result = collection.find(filter);
        return result.first().get("values").toString();

    }

    public static void insert(Insert insert) throws Exception {
        if (dbmsRepository.getCurrentDatabase().equals(""))
            throw new Exception("No database in use!");


        String tableName = insert.getTable().getName();
        Element tableElement = validateTableName(tableName);

        List<Column> insertColumns = insert.getColumns();
        List<Expression> values = ((ExpressionList) insert.getItemsList()).getExpressions();


        List<String> attributes = tableElement.getChild("Structure").getChildren("Attribute").stream().map(line -> line.getAttributeValue("attributeName")).toList();
        List<Element> primaryKeyElements = tableElement.getChild("primaryKey").getChildren("pkAttribute");

        validateColumns(insertColumns, attributes);
        List<String> keyValueList = computeInsertKeyValue(attributes, primaryKeyElements, values);
        insertInMongoDb(tableName, keyValueList.get(0), keyValueList.get(1));
    }

    private static void insertInMongoDb(String tableName, String key, String values) throws Exception {
        try (MongoClient client = getMongoClient()) {
            MongoDatabase mongoDatabase = client.getDatabase(DATABASE_NAME);
            MongoCollection<Document> collection = mongoDatabase.getCollection(dbmsRepository.getCurrentDatabase() + tableName);

            Document document = new Document("_id", key).append("values", values);
            collection.insertOne(document);
        } catch (Exception e) {
            throw new Exception("Primary key constraint violated!");
        }
    }

    private static void validateColumns(List<Column> columns, List<String> attributes) throws Exception {
        if (columns != null) {
            if (columns.size() != attributes.size())
                throw new Exception("Number of query values and destination fields are not the same.");
            for (Column column : columns) {
                if (!attributes.contains(column.toString()))
                    throw new Exception("Table has no column named " + column);
            }

        }
    }

    private static void validateUpdateColumns(List<Column> columns, List<String> attributes) throws Exception {
        if (columns != null) {
            for (Column column : columns) {
                if (!attributes.contains(column.toString()))
                    throw new Exception("Table has no column named " + column);
            }

        }
    }

    private static List<String> computeInsertKeyValue(List<String> columns, List<Element> primaryKeyElements, List<Expression> values) {
        List<String> primaryKeyList = new ArrayList<>();
        List<String> othersList = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            String column = columns.get(i);
            String value = values.get(i).toString();

            if (primaryKeyElements.stream().anyMatch(e -> e.getText().equals(column))) {
                primaryKeyList.add(value);
            } else {
                othersList.add(value);
            }
        }
        return Arrays.asList(String.join("#", primaryKeyList), String.join("#", othersList));
    }

    private static void deleteFromTable(Delete delete) throws Exception {
        if (dbmsRepository.getCurrentDatabase().equals(""))
            throw new Exception("No database in use!");
        String tableName = delete.getTable().getName();
        Element tableElement = validateTableName(tableName);
        Map<String, String> primaryKeys = extractAttributeValues(delete.getWhere());
        validatePrimaryKey(tableElement, primaryKeys);
        deleteFromMongoDb(tableName, primaryKeys);
    }

    private static Element validateTableName(String tableName) throws Exception {
        Element rootDataBases = dbmsRepository.getDoc().getRootElement();

        Optional<Element> databaseElement = getDatabaseElement(rootDataBases);
        if (databaseElement.isPresent()) {
            Optional<Element> tableElement = getTableByName(databaseElement.get(), tableName);
            if (tableElement.isEmpty()) {
                throw new Exception("Table not found!");
            }
            return tableElement.get();
        } else {
            throw new Exception("Invalid database!");
        }
    }

    private static Optional<Element> getDatabaseElement(Element databasesElement) {
        return databasesElement.getChildren(DATABASE_NODE).stream()
                .filter(db -> db.getAttributeValue(DATABASE_NAME_ATTRIBUTE).equals(dbmsRepository.getCurrentDatabase()))
                .findFirst();
    }

    private static Optional<Element> getTableByName(Element databaseElement, String tableName) {
        return databaseElement.getChildren(TABLE_NODE).stream().
                filter(table -> table.getAttributeValue(TABLE_NAME_ATTRIBUTE).equals(tableName))
                .findFirst();
    }

    private static Map<String, String> extractAttributeValues(Expression expression) {
        Map<String, String> attributeValueMap = new LinkedHashMap<>();

        if (expression instanceof AndExpression andExpression) {
            attributeValueMap.putAll(extractAttributeValues(andExpression.getLeftExpression()));
            attributeValueMap.putAll(extractAttributeValues(andExpression.getRightExpression()));
        } else if (expression instanceof EqualsTo equalsTo) {
            if (equalsTo.getLeftExpression() instanceof Column column) {
                String attributeName = column.getColumnName();
                String attributeValue = equalsTo.getRightExpression().toString();
                attributeValueMap.put(attributeName, attributeValue);
            }
        }

        return attributeValueMap;
    }

    private static void validatePrimaryKey(Element table, Map<String, String> primaryKeys) throws Exception {
        Set<String> primaryKeysFromDb = getPrimaryKeysFromDb(table);
        if (!primaryKeysFromDb.equals(primaryKeys.keySet())) {
            throw new Exception("Invalid arguments in where condition!");
        }
        validateTypeOfPrimaryKeys(table, primaryKeys);
    }

    private static Set<String> getPrimaryKeysFromDb(Element table) {
        return table.getChildren(PRIMARY_KEY_NODE).stream()
                .flatMap(pk -> pk.getChildren(PRIMARY_KEY_ATTRIBUTE).stream())
                .map(pkAttr -> pkAttr.getContent(0).getValue())
                .collect(Collectors.toSet());
    }

    private static void validateTypeOfPrimaryKeys(Element table, Map<String, String> primaryKeys) throws Exception {
        List<Attribute> primaryKeysAttribute = getAttributePkFromDb(table, primaryKeys.keySet());
        for (Map.Entry<String, String> entry : primaryKeys.entrySet()) {
            Optional<Attribute> attribute = getAttributeByName(primaryKeysAttribute, entry.getKey());
            if (attribute.isPresent()) {
                Attribute attr = attribute.get();
                String value = entry.getValue();
                if (attr.getType().equalsIgnoreCase("int")) {
                    try {
                        Integer.parseInt(value);
                    } catch (NumberFormatException e) {
                        throw new Exception("Invalid attribute type: " + entry.getValue());
                    }
                } else if (attr.getType().equalsIgnoreCase("float")) {
                    try {
                        Float.parseFloat(value);
                    } catch (NumberFormatException e) {
                        throw new Exception("Invalid attribute type: " + entry.getValue());
                    }
                } else if (attr.getType().equalsIgnoreCase("double")) {
                    try {
                        Double.parseDouble(value);
                    } catch (NumberFormatException e) {
                        throw new Exception("Invalid attribute type: " + entry.getValue());
                    }
                } else {
                    int padding = 0;
                    if (value.startsWith("'") && value.endsWith("'")) {
                        padding = 2;
                    }
                    if (entry.getValue().length() > attr.getLength() + padding) {
                        throw new Exception("Invalid attribute type: " + entry.getValue());
                    }
                }
            }
        }
    }

    private static List<Attribute> getAttributePkFromDb(Element table, Set<String> primaryKeys) {
        return table.getChildren(STRUCTURE_NODE).stream()
                .flatMap(attr -> attr.getChildren(ATTRIBUTE_NODE).stream())
                .map(DBMSService::getAttribute)
                .filter(attribute -> primaryKeys.contains(attribute.getAttributeName()))
                .collect(Collectors.toList());
    }

    private static Optional<Attribute> getAttributeByName(List<Attribute> attributes, String name) {
        return attributes.stream()
                .filter(attribute -> attribute.getAttributeName().equals(name))
                .findFirst();
    }

    private static Attribute getAttribute(Element attributeFromDb) {
        String attributeName = attributeFromDb.getAttributeValue("attributeName");
        String type = attributeFromDb.getAttributeValue("type");
        int length = Integer.parseInt(attributeFromDb.getAttributeValue("length"));
        boolean isNull = attributeFromDb.getAttributeValue("isnull").equals("1");
        return new Attribute(attributeName, type, length, isNull);
    }

    private static void deleteFromMongoDb(String tableName, Map<String, String> primaryKeys) throws Exception {
        try (MongoClient client = getMongoClient()) {
            MongoDatabase mongoDatabase = client.getDatabase(DATABASE_NAME);
            MongoCollection<Document> collection = mongoDatabase.getCollection(dbmsRepository.getCurrentDatabase() + tableName);
            String concatenatedKey = String.join("#", primaryKeys.values());
            Bson filter = Filters.eq("_id", concatenatedKey);
            DeleteResult result = collection.deleteOne(filter);
            if (result.getDeletedCount() < 1) {
                throw new Exception("No elements were deleted.");
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception(e.getMessage());
        }
    }

    public static void createIndex(CreateIndex index) throws Exception {
        if (dbmsRepository.getCurrentDatabase().equals(""))
            throw new Exception("No database in use!");
        try {

            String indexName = index.getIndex().getName();
            String tableName = index.getTable().getName();
            List<String> columnsNames = index.getIndex().getColumnsNames();
            String isUnique = index.getIndex().getType() != null ? "1" : "0";
            Element rootDataBases = dbmsRepository.getDoc().getRootElement();
            Element dataBase = rootDataBases.getChild("DataBase");
            for (Element element : rootDataBases.getChildren("DataBase")) {
                if (Objects.equals(element.getAttributeValue("dataBaseName"), dbmsRepository.getCurrentDatabase())) {
                    dataBase = element;
                    break;
                }
            }

            Element tableElement = null;

            for (Element element : dataBase.getChildren("Table")) {
                if (Objects.equals(element.getAttributeValue("tableName"), tableName.toString())) {
                    tableElement = element;
                }
            }

            if (tableElement == null) {
                throw new Exception("Table not found!");
            }

            Element indexFilesElement = tableElement.getChild("IndexFiles");
            if (indexFilesElement == null) {
                indexFilesElement = new Element("IndexFiles");
            }
            if (indexFilesElement.getChildren("IndexFile").stream().anyMatch(column -> column.getAttributeValue("indexName").equals(indexName)))
                throw new Exception("An Index with same name already exist!");
            Element indexFileElement = new Element("IndexFile");
            indexFileElement.setAttribute("indexName", indexName);

            indexFileElement.setAttribute("isUnique", isUnique);
            indexFileElement.setAttribute("indexType", "BTree");


            Element indexAttributesElement = new Element("IndexAttributes");
            for (String columnName : columnsNames) {
                if (!tableElement.getChild("Structure").getChildren("Attribute").stream().anyMatch(column -> column.getAttributeValue("attributeName").equalsIgnoreCase(columnName))) {
                    throw new Exception("Column " + columnName + " not found!");
                }

                Element iAttributeElement = new Element("IAttribute");
                iAttributeElement.setText(columnName);
                indexAttributesElement.addContent(iAttributeElement);
            }

            indexFileElement.addContent(indexAttributesElement);
            indexFilesElement.addContent(indexFileElement);

            dbmsRepository.saveToFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void useDataBase(String databaseName) throws Exception {
        Element rootElement = dbmsRepository.getDoc().getRootElement();
        for (Element element : rootElement.getChildren("DataBase")) {
            if (Objects.equals(element.getAttributeValue("dataBaseName"), databaseName)) {
                dbmsRepository.setCurrentDatabase(databaseName);
                break;
            }
        }
        if (dbmsRepository.getCurrentDatabase().equals(""))
            throw new Exception("Database not found!");

    }

    public static void createDatabase(String databaseName) throws Exception {
        try {

            Element rootElement = dbmsRepository.getDoc().getRootElement();
            for (Element element : rootElement.getChildren("DataBase")) {
                if (element.getAttributeValue("dataBaseName").equals(databaseName)) {
                    throw new Exception("Database already exist!");
                }
            }

            Element databaseElement = new Element("DataBase");
            databaseElement.setAttribute("dataBaseName", databaseName);
            rootElement.addContent(databaseElement);

            dbmsRepository.saveToFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public static void dropDataBase(String databaseName) throws Exception {
        try {

            Element rootElement = dbmsRepository.getDoc().getRootElement();
            Boolean databaseFound = false;
            for (Element element : rootElement.getChildren("DataBase")) {
                if (element.getAttributeValue("dataBaseName").equals(databaseName)) {
                    rootElement.removeContent(element);
                    databaseFound = true;
                    break;
                }
            }
            if (!databaseFound)
                throw new Exception("Database not found!");
            dbmsRepository.saveToFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void dropTable(Drop dropTable) throws Exception {
        if (dbmsRepository.getCurrentDatabase().equals(""))
            throw new Exception("No database in use!");
        try {
            Boolean tableFound = false;
            Element rootDataBases = dbmsRepository.getDoc().getRootElement();
            Element dataBase = rootDataBases.getChild("DataBase");
            for (Element element : rootDataBases.getChildren("DataBase")) {
                if (element.getAttributeValue("dataBaseName").equals(dbmsRepository.getCurrentDatabase())) {
                    dataBase = element;
                }
            }

            for (Element element : dataBase.getChildren("Table")) {
                if (element.getAttributeValue("tableName").equals(dropTable.getName().toString())) {
                    tableFound = true;
                    dataBase.removeContent(element);
                }
            }
            if (!tableFound)
                throw new Exception("Table not found!");
            dbmsRepository.saveToFile();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void createTable(CreateTable createTable) throws Exception {
        if (dbmsRepository.getCurrentDatabase().equals(""))
            throw new Exception("No database in use!");
        Element rootDataBases = dbmsRepository.getDoc().getRootElement();
        Element database = rootDataBases.getChild("DataBase");
        ;
        for (Element element : rootDataBases.getChildren("DataBase")) {
            if (Objects.equals(element.getAttributeValue("dataBaseName"), dbmsRepository.getCurrentDatabase())) {
                if (element.getChildren("Table").stream().anyMatch(column -> column.getAttributeValue("tableName").equalsIgnoreCase(createTable.getTable().getName())))
                    throw new Exception("Table already exist!");
                database = element;
            }
        }
        try {

            String tableName = createTable.getTable().getName();
            String fileName = tableName + ".csv";
            int rowLength = 0; //calculam in functie de nr coloane

            // Crearea elementului radacina "<Table>"
            Element rootElement = new Element("Table");
            rootElement.setAttribute("tableName", tableName);
            rootElement.setAttribute("fileName", fileName);
            //rootElement.setAttribute("rowLength", String.valueOf(rowLength));

            // Crearea elementului "<Structure>"
            Element structureElement = new Element("Structure");
            for (ColumnDefinition column : createTable.getColumnDefinitions()) {
                String attributeName = column.getColumnName();
                String type = column.getColDataType().getDataType();
                int isNull = column.toString().contains("NOT NULL") ? 0 : 1;
                int length = 0;
                Element attributeElement = new Element("Attribute");
                attributeElement.setAttribute("attributeName", attributeName);
                attributeElement.setAttribute("type", type);
                if (type.equalsIgnoreCase("char") || type.equalsIgnoreCase("varchar")) {
                    length = column.getColDataType().getArgumentsStringList().size() > 0 ? Integer.parseInt(column.getColDataType().getArgumentsStringList().get(0)) : 30;

                } else if (type.equalsIgnoreCase("int") || type.equalsIgnoreCase("double"))
                    length = 64;
                attributeElement.setAttribute("length", String.valueOf(length));
                attributeElement.setAttribute("isnull", String.valueOf(isNull));
                structureElement.addContent(attributeElement);
            }

            // Crearea elementului "<primaryKey>"
            Element primaryKeyElement = new Element("primaryKey");
            Element uniqueKeysElement = new Element("uniqueKeys");

            Element indexFilesElement = new Element("IndexFiles");
            // Creați elementul XML pentru constrângerea de cheie străină
            Element foreignKeysElement = rootElement.getChild("foreignKeys");
            if (foreignKeysElement == null) {
                foreignKeysElement = new Element("foreignKeys");
            }

            if (createTable.getIndexes() != null) {
                for (Index index : createTable.getIndexes()) {
                    if (indexFilesElement.getChildren("IndexFile").stream().anyMatch(column -> column.getAttributeValue("indexName").equals(index.getColumnsNames().get(0) + ".ind")))
                        throw new Exception("An Index with same name already exist!");

                    if (index.getType().equals("PRIMARY KEY")) {
                        Element indexFileElement = new Element("IndexFile");
                        System.out.println(index);
                        indexFileElement.setAttribute("indexName", index.getColumnsNames().get(0) + ".ind");
                        indexFileElement.setAttribute("keyLength", "64");
                        indexFileElement.setAttribute("isUnique", "1");
                        indexFileElement.setAttribute("indexType", "BTree");


                        Element indexAttributesElement = new Element("IndexAttributes");
                        for (String columnName : index.getColumnsNames()) {
                            Element pkAttributeElement = new Element("pkAttribute");
                            primaryKeyElement.addContent(pkAttributeElement);
                            pkAttributeElement.addContent(columnName);
                            Element iAttributeElement = new Element("IAttribute");
                            iAttributeElement.setText(columnName); // Aici trebuie să specificați numele atributului indexului

                            indexAttributesElement.addContent(iAttributeElement);
                        }
                        indexFileElement.addContent(indexAttributesElement);
                        indexFilesElement.addContent(indexFileElement);

                    }
                    if (index.getType().equals("UNIQUE")) {
                        //pentru index
                        // Adăugați elementul IndexFiles pentru index
                        Element indexFileElement = new Element("IndexFile");
                        indexFileElement.setAttribute("indexName", index.getColumnsNames().get(0) + ".ind");
                        indexFileElement.setAttribute("keyLength", "30");
                        indexFileElement.setAttribute("isUnique", "1");
                        indexFileElement.setAttribute("indexType", "BTree");


                        Element indexAttributesElement = new Element("IndexAttributes");
                        for (String columnName : index.getColumnsNames()) {
                            Element uniqueAttributeElement = new Element("UniqueAttribute");
                            uniqueAttributeElement.addContent(columnName);
                            uniqueKeysElement.addContent(uniqueAttributeElement);

                            Element iAttributeElement = new Element("IAttribute");
                            iAttributeElement.setText(columnName); // Aici trebuie să specificați numele atributului indexului

                            indexAttributesElement.addContent(iAttributeElement);

                        }
                        indexFileElement.addContent(indexAttributesElement);
                        indexFilesElement.addContent(indexFileElement);


                    }


                    if (index.getType().equals("FOREIGN KEY")) {
                        String foreignKeyPattern = "FOREIGN KEY \\(([^)]+)\\) REFERENCES ([^(]+)\\(([^)]+)\\)";
                        Pattern pattern = Pattern.compile(foreignKeyPattern);
                        Matcher matcher = pattern.matcher(index.toString());

                        Element foreignKeyElement = new Element("foreignKey");
                        foreignKeysElement.addContent(foreignKeyElement);

                        if (matcher.find()) {
                            String referencedTable = matcher.group(2);
                            String referencedColumns = matcher.group(3);

                            Element refrencedTable = null;
                            Element dataBase = rootDataBases.getChild("DataBase");
                            for (Element element : dataBase.getChildren("Table")) {
                                if (element.getAttributeValue("tableName").equals(referencedTable)) {
                                    refrencedTable = element;
                                }
                            }
                            if (refrencedTable == null)
                                throw new Exception("Referenced table not found!");

                            for (String columnName : index.getColumnsNames()) {
                                Element fkAttributeElement = new Element("fkAttribute");
                                fkAttributeElement.setText(columnName);
                                foreignKeyElement.addContent(fkAttributeElement);
                            }

                            Element referencesElement = new Element("references");
                            foreignKeyElement.addContent(referencesElement);

                            Element refTableElement = new Element("refTable");
                            refTableElement.setText(referencedTable);
                            referencesElement.addContent(refTableElement);

                            for (String refColumn : referencedColumns.split(",")) {
                                if (!refrencedTable.getChild("Structure").getChildren("Attribute").stream().anyMatch(column -> column.getAttributeValue("attributeName").equalsIgnoreCase(refColumn)))
                                    throw new Exception("Referenced column " + refColumn + " not found!");
                                Element fkAttributeElement = new Element("fkAttribute");
                                fkAttributeElement.setText(refColumn.trim());
                                foreignKeyElement.addContent(fkAttributeElement);
                            }
                        }
                    }
                }
            }


            // Adăugați elementele create la elementul radacină "<Table>"
            rootElement.addContent(structureElement);
            rootElement.addContent(primaryKeyElement);
            rootElement.addContent(foreignKeysElement);
            rootElement.addContent(uniqueKeysElement);
            rootElement.addContent(indexFilesElement);


            database.addContent(rootElement);
            dbmsRepository.saveToFile();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
