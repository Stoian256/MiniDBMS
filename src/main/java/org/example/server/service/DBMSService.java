package org.example.server.service;

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
import org.example.server.entity.Attribute;
import org.example.server.entity.ForeignKey;
import org.example.server.repository.DBMSRepository;
import org.jdom2.Element;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.example.server.connectionManager.DbConnectionManager.getMongoClient;

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
            deleteFromTable(delete);
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
        List<ForeignKey> foreignKeys = getForeignKeyFromCatalog(tableElement);
        String primaryKeyIndexName = dbmsRepository.getCurrentDatabase() + tableName + "PkInd" + String.join("", primaryKeyElements.stream().map(el -> el.getContent().get(0).getValue()).toList());
        validateColumns(insertColumns, attributes);
        List<String> keyValueList = computeInsertKeyValue(attributes, primaryKeyElements, values);

        Element indexFiles = tableElement.getChild("IndexFiles");
        Map<String, String> attributeValueMap = computeAttributeValueMap(insertColumns, values);
        validatePrimaryKeyConstraint(dbmsRepository.getCurrentDatabase() + tableName,keyValueList.get(0));
        processIndexFiles(indexFiles, attributeValueMap, keyValueList.get(0), primaryKeyIndexName);
        saveForeignKeys(foreignKeys, attributeValueMap, tableName, keyValueList.get(0));
        insertInMongoDb(dbmsRepository.getCurrentDatabase() + tableName, keyValueList.get(0), keyValueList.get(1));
    }

    private static void processIndexFiles(Element indexFiles, Map<String, String> attributeValueMap, String primaryKey, String primaryKeyIndexName) throws Exception {
        for (Element indexFile : indexFiles.getChildren("IndexFile")) {
            String indexName = indexFile.getAttributeValue("indexName").split("\\.")[0];
            if (Objects.equals(indexName, primaryKeyIndexName) || indexName.contains("FkInd"))
                continue;

            String isUnique = indexFile.getAttributeValue("isUnique");
            List<String> indexColumns = indexFile.getChild("IndexAttributes").getChildren("IAttribute").stream().map(el -> el.getContent().get(0).getValue()).toList();
            String indexSearchKey = indexColumns.stream().map(column -> attributeValueMap.get(column)).collect(Collectors.joining("$"));
            if (isUnique.equals("1")) {
                validateUniqueKeyConstraints(indexName, indexSearchKey);
            }
        }

        for (Element indexFile : indexFiles.getChildren("IndexFile")) {
            String indexName = indexFile.getAttributeValue("indexName").split("\\.")[0];
            if (Objects.equals(indexName, primaryKeyIndexName) || indexName.contains("FkInd"))
                continue;

            String isUnique = indexFile.getAttributeValue("isUnique");
            List<String> indexColumns = indexFile.getChild("IndexAttributes").getChildren("IAttribute").stream().map(el -> el.getContent().get(0).getValue()).toList();

            List<String> keyValues = new ArrayList<>();
            indexColumns.forEach(el -> {
                keyValues.add(attributeValueMap.get(el));
            });
            if (Objects.equals(isUnique, "1"))
                insertInMongoDb(indexName, String.join("$", keyValues), primaryKey);
            if (Objects.equals(isUnique, "0"))
                updateIndexInMongoDb(indexName, String.join("$", keyValues), primaryKey);

        }
    }

    private static void saveForeignKeys(List<ForeignKey> foreignKeys, Map<String, String> attributeValueMap, String tableName, String primaryKey) throws Exception {
        validateForeignKeys(foreignKeys, attributeValueMap);
        for (int index = 0; index < foreignKeys.size(); index++) {
            saveForeignKey(foreignKeys.get(index), attributeValueMap, tableName, primaryKey);
        }
    }

    private static void saveForeignKey(ForeignKey foreignKey, Map<String, String> attributeValueMap, String tableName, String primaryKey) throws Exception {
        String collectionName = dbmsRepository.getCurrentDatabase() + tableName + "FkInd" + String.join("", String.join("", foreignKey.getAttributes()) + "Ref" + foreignKey.getRefTableName());
        List<String> foreignKeyValue = getForeignKeyValue(foreignKey, attributeValueMap);
        updateIndexInMongoDb(collectionName, String.join("$", foreignKeyValue), primaryKey);
    }

    private static void validateForeignKeys(List<ForeignKey> foreignKeys, Map<String, String> attributeValueMap) throws Exception {
        for (ForeignKey foreignKey : foreignKeys) {
            List<String> foreignKeyValue = getForeignKeyValue(foreignKey, attributeValueMap);
            validateForeignKey(foreignKey.getRefTableName(), foreignKeyValue);
        }
    }

    private static List<String> getForeignKeyValue(ForeignKey foreignKey, Map<String, String> attributeValueMap) {
        return foreignKey.getAttributes().stream()
                .map(attributeValueMap::get)
                .toList();
    }

    private static void validateForeignKey(String refTable, List<String> foreignKeyValue) throws Exception {
        String collectionName = dbmsRepository.getCurrentDatabase() + refTable;
        List<String> primaryKeyFromRefTable = getValuesForKey(collectionName, String.join("#", foreignKeyValue));
        if (primaryKeyFromRefTable.isEmpty()) {
            throw new Exception("Foreign key constraint violated!");
        }
    }

    private static List<ForeignKey> getForeignKeyFromCatalog(Element tableElement) {
        List<ForeignKey> foreignKeys = new ArrayList<>();
        Element foreignKeysElement = tableElement.getChild("foreignKeys");
        for (Element foreignKeyElement : foreignKeysElement.getChildren("foreignKey")) {
            foreignKeys.add(getForeignKeyFromElement(foreignKeyElement));
        }

        return foreignKeys;
    }

    private static ForeignKey getForeignKeyFromElement(Element foreignKeyElement) {
        List<String> attributes = foreignKeyElement.getChildren("fkAttribute").stream()
                .map(Element::getText)
                .toList();
        Element referencesElement = foreignKeyElement.getChild("references");
        String refTable = referencesElement.getChild("refTable").getText();
        List<String> refAttributes = referencesElement.getChildren("refAttribute").stream()
                .map(Element::getText)
                .toList();
        return new ForeignKey(refTable, attributes, refAttributes);
    }
    private static void insertInMongoDb(String collectionName, String key, String values) throws Exception {
        try (MongoClient client = getMongoClient()) {
            MongoDatabase mongoDatabase = client.getDatabase(DATABASE_NAME);
            MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);

            Document document = new Document("_id", key).append("values", values);
            collection.insertOne(document);
        } catch (Exception e) {
            throw new Exception("Primary key constraint violated!");
        }
    }

    private static Map<String, String> computeAttributeValueMap(List<Column> attributes, List<Expression> values) {
        Map<String, String> attributeValueMap = new LinkedHashMap<>();
        for (int i = 0; i < attributes.size(); i++) {
            Column attribute = attributes.get(i);
            Expression value = values.get(i);

            attributeValueMap.put(attribute.getColumnName(), value.toString());
        }
        return attributeValueMap;
    }

    private static void validateUniqueKeyConstraints(String indexName, String indexSearchKey) throws Exception {
        try (MongoClient client = getMongoClient()) {
            MongoDatabase mongoDatabase = client.getDatabase(DATABASE_NAME);
            MongoCollection<Document> collection = mongoDatabase.getCollection(indexName);

            Document filter = new Document("_id", indexSearchKey);
            Document oldDocument = collection.find(filter).first();
            if (oldDocument != null)
                throw new Exception("Unique Key Constraint Violation");

        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
    }

    private static void validatePrimaryKeyConstraint(String tableName, String primaryKey) throws Exception {
        try (MongoClient client = getMongoClient()) {
            MongoDatabase mongoDatabase = client.getDatabase(DATABASE_NAME);
            MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);

            Document filter = new Document("_id", primaryKey);
            Document oldDocument = collection.find(filter).first();
            if (oldDocument != null)
                throw new Exception("Primary Key Constraint Violation");
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
    }

    private static void updateIndexInMongoDb(String indexName, String key, String values) throws Exception {
        try (MongoClient client = getMongoClient()) {
            MongoDatabase mongoDatabase = client.getDatabase(DATABASE_NAME);
            MongoCollection<Document> collection = mongoDatabase.getCollection(indexName);

            Document filter = new Document("_id", key); // Caută înregistrarea după cheia primară
            Document oldDocument = collection.find(filter).first();

            if (oldDocument != null && oldDocument.containsKey("values")) {
                String oldValue = oldDocument.getString("values");
                String updatedValue = oldValue + "#" + values; // Concatenează valoarea existentă cu noua valoare

                // Actualizează câmpul "values" cu valoarea actualizată
                Document updateDoc = new Document("$set", new Document("values", updatedValue));
                collection.updateOne(filter, updateDoc);
            } else {
                Document document = new Document("_id", key).append("values", values);
                collection.insertOne(document);
            }

        } catch (Exception e) {
            throw new Exception("A apărut o eroare la actualizarea înregistrării.");
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
        Map<String, List<ForeignKey>> foreignKeys = getForeignKeyFromCatalogByRefTable(tableElement.getParentElement(), tableName);
        validateForeignKeyForDelete(foreignKeys, primaryKeys.keySet(), String.join("#", primaryKeys.values()));
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

    private static Map<String, List<ForeignKey>> getForeignKeyFromCatalogByRefTable(Element databaseElement, String refTable) {
        Map<String, List<ForeignKey>> foreignKeyMap = new HashMap<>();
        for (Element tableElement : databaseElement.getChildren("Table")) {
            Element foreignKeysElement = tableElement.getChild("foreignKeys");
            String tableName = tableElement.getAttributeValue("tableName");
            for (Element foreignKeyElement : foreignKeysElement.getChildren("foreignKey")) {
                ForeignKey foreignKey = getForeignKeyFromElementByRefTable(foreignKeyElement, refTable);
                if (foreignKey != null) {
                    foreignKeyMap.computeIfAbsent(tableName, k -> new ArrayList<>()).add(foreignKey);
                }
            }
        }
        return foreignKeyMap;
    }

    private static ForeignKey getForeignKeyFromElementByRefTable(Element foreignKeyElement, String refTable) {
        List<String> attributes = foreignKeyElement.getChildren("fkAttribute").stream()
                .map(Element::getText)
                .toList();
        Element referencesElement = foreignKeyElement.getChild("references");
        if (referencesElement.getChild("refTable").getText().equals(refTable)) {
            List<String> refAttributes = referencesElement.getChildren("refAttribute").stream()
                    .map(Element::getText)
                    .toList();
            return new ForeignKey(refTable, attributes, refAttributes);
        }
        return null;
    }

    private static void validateForeignKeyForDelete(Map<String, List<ForeignKey>> foreignKeyMap, Set<String> primaryKey, String value) throws Exception {
        for (Map.Entry<String, List<ForeignKey>> entry : foreignKeyMap.entrySet()) {
            for (ForeignKey foreignKey : entry.getValue()) {
                if (new HashSet<>(foreignKey.getRefAttributeList()).containsAll(primaryKey)) {
                    String collectionName = dbmsRepository.getCurrentDatabase() + entry.getKey() + "FkInd" + String.join("", String.join("", foreignKey.getAttributes()) + "Ref" + foreignKey.getRefTableName());
                    List<String> primaryKeyFromRefTable = getValuesForKey(collectionName, value);
                    if (!primaryKeyFromRefTable.isEmpty()) {
                        throw new Exception("Foreign key constraint violated!");
                    }
                }
            }
        }
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

            String tableName = index.getTable().getName();
            String indexName = dbmsRepository.getCurrentDatabase() + tableName + "Ind" + index.getIndex().getName();

            Element tableElement = validateTableName(tableName);
            Element indexFilesElement = validateIndexFiles(tableElement, indexName);
            Element indexFileElement = createIndexFileElement(index, tableElement);

            indexFilesElement.addContent(indexFileElement);

            dbmsRepository.saveToFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Element createIndexFileElement(CreateIndex index, Element tableElement) throws Exception {
        String indexName = dbmsRepository.getCurrentDatabase() + index.getTable().getName() + "Ind" + index.getIndex().getName();
        List<String> columnsNames = index.getIndex().getColumnsNames();
        String isUnique = index.getIndex().getType() != null ? "1" : "0";
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
        return indexFileElement;
    }

    private static Element validateIndexFiles(Element tableElement, String indexName) throws Exception {
        Element indexFilesElement = tableElement.getChild("IndexFiles");
        if (indexFilesElement == null) {
            indexFilesElement = new Element("IndexFiles");
        }
        if (indexFilesElement.getChildren("IndexFile").stream().anyMatch(column -> column.getAttributeValue("indexName").equals(indexName)))
            throw new Exception("An Index with same name already exist!");
        return indexFilesElement;
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
            validateDropTable(dataBase, dropTable.getName().toString());
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

    private static void validateDropTable(Element dataBase, String refTable) throws Exception {
        for (Element tableElement : dataBase.getChildren("Table")) {
            for (Element fKeyElement : tableElement.getChild("foreignKeys").getChildren("foreignKey")) {
                if (fKeyElement.getChild("references").getChild("refTable").getText().equals(refTable)) {
                    throw new Exception("Foreign key constraint violated!");
                }
            }
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
            String fileName = dbmsRepository.getCurrentDatabase() + tableName;
            int rowLength = 0; //calculam in functie de nr coloane

            // Crearea elementului radacina "<Table>"
            Element rootElement = new Element("Table");
            rootElement.setAttribute("tableName", tableName);
            rootElement.setAttribute("fileName", fileName);
            //rootElement.setAttribute("rowLength", String.valueOf(rowLength));

            Element primaryKeyElement = new Element("primaryKey");
            Element uniqueKeysElement = new Element("uniqueKeys");

            Element indexFilesElement = new Element("IndexFiles");
            Element foreignKeysElement = rootElement.getChild("foreignKeys");

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

                if (column.getColumnSpecs() != null && column.getColumnSpecs().get(0).equalsIgnoreCase("UNIQUE")) {
                    Element indexFileElement = new Element("IndexFile");
                    String indexName = dbmsRepository.getCurrentDatabase() + tableName + "UqInd" + attributeName;
                    indexFileElement.setAttribute("indexName", indexName);
                    indexFileElement.setAttribute("keyLength", "30");
                    indexFileElement.setAttribute("isUnique", "1");
                    indexFileElement.setAttribute("indexType", "BTree");


                    Element indexAttributesElement = new Element("IndexAttributes");

                    Element uniqueAttributeElement = new Element("UniqueAttribute");
                    uniqueAttributeElement.addContent(attributeName);
                    uniqueKeysElement.addContent(uniqueAttributeElement);

                    Element iAttributeElement = new Element("IAttribute");
                    iAttributeElement.setText(attributeName);
                    indexAttributesElement.addContent(iAttributeElement);

                    indexFileElement.addContent(indexAttributesElement);
                    indexFilesElement.addContent(indexFileElement);
                    createCollection(indexName);
                }
            }
            if (foreignKeysElement == null) {
                foreignKeysElement = new Element("foreignKeys");
            }

            if (createTable.getIndexes() != null) {
                for (Index index : createTable.getIndexes()) {
                    if (indexFilesElement.getChildren("IndexFile").stream().anyMatch(column -> column.getAttributeValue("indexName").equals(index.getColumnsNames().get(0) + ".ind")))
                        throw new Exception("An Index with same name already exist!");

                    if (index.getType().equals("PRIMARY KEY")) {
                        Element indexFileElement = new Element("IndexFile");
                        String indexName = dbmsRepository.getCurrentDatabase() + tableName + "PkInd" + String.join("", index.getColumnsNames());
                        indexFileElement.setAttribute("indexName", indexName);
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
                        String indexName = dbmsRepository.getCurrentDatabase() + tableName + "UqInd" + String.join("", index.getColumnsNames());
                        indexFileElement.setAttribute("indexName", indexName);
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
                        createCollection(indexName);
                    }


                    if (index.getType().equals("FOREIGN KEY")) {
                        Element indexFileElement = new Element("IndexFile");
                        String indexName = "";
                        indexFileElement.setAttribute("keyLength", "64");
                        indexFileElement.setAttribute("isUnique", "0");
                        indexFileElement.setAttribute("indexType", "BTree");
                        Element indexAttributesElement = new Element("IndexAttributes");

                        String foreignKeyPattern = "FOREIGN KEY \\(([^)]+)\\) REFERENCES ([^(]+)\\(([^)]+)\\)";
                        Pattern pattern = Pattern.compile(foreignKeyPattern);
                        Matcher matcher = pattern.matcher(index.toString());

                        Element foreignKeyElement = new Element("foreignKey");
                        foreignKeysElement.addContent(foreignKeyElement);

                        if (matcher.find()) {
                            String referencedTable = matcher.group(2);
                            String referencedColumns = matcher.group(3);

                            indexName = dbmsRepository.getCurrentDatabase() + tableName + "FkInd" + String.join("", String.join("", index.getColumnsNames()) + "Ref" + referencedTable);
                            indexFileElement.setAttribute("indexName", indexName);

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

                                Element iAttributeElement = new Element("IAttribute");
                                iAttributeElement.setText(columnName);
                                indexAttributesElement.addContent(iAttributeElement);
                            }

                            indexFileElement.addContent(indexAttributesElement);
                            indexFilesElement.addContent(indexFileElement);

                            Element referencesElement = new Element("references");
                            foreignKeyElement.addContent(referencesElement);

                            Element refTableElement = new Element("refTable");
                            refTableElement.setText(referencedTable);
                            referencesElement.addContent(refTableElement);

                            for (String refColumn : referencedColumns.split(",")) {
                                if (!refrencedTable.getChild("Structure").getChildren("Attribute").stream().anyMatch(column -> column.getAttributeValue("attributeName").equalsIgnoreCase(refColumn.strip())))
                                    throw new Exception("Referenced column " + refColumn + " not found!");
                                Element fkAttributeElement = new Element("refAttribute");
                                fkAttributeElement.setText(refColumn.trim());
                                referencesElement.addContent(fkAttributeElement);
                            }
                        }
                        createCollection(indexName);
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
            createCollection(fileName);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void createCollection(String collectionName) throws Exception {
        try (MongoClient client = getMongoClient()) {
            MongoDatabase mongoDatabase = client.getDatabase(DATABASE_NAME);
            if (!mongoDatabase.listCollectionNames().into(new ArrayList<>()).contains(collectionName)) {
                mongoDatabase.createCollection(collectionName);
            } else {
                throw new Exception("Collection already exists");
            }
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
    }

    private static List<String> getValuesForKey(String collectionName, String key) throws Exception {
        List<String> values = new ArrayList<>();
        try (MongoClient client = getMongoClient()) {
            MongoDatabase mongoDatabase = client.getDatabase(DATABASE_NAME);
            MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
            Document query = new Document("_id", key);
            for (Document document : collection.find(query)) {
                values.add((String) document.get("values"));
            }
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
        return values;
    }

}
