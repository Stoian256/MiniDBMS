package org.example.server.service;


import com.mongodb.client.*;
import com.mongodb.client.model.Sorts;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.bson.conversions.Bson;
import org.example.server.repository.DBMSRepository;
import org.jdom2.Element;
import org.bson.Document;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.example.server.connectionManager.DbConnectionManager.getMongoClient;
import static org.example.server.service.DBMSService.getDatabaseElement;
import static org.example.server.service.DBMSService.getTableElementByName;

public class SelectService {
    private final DBMSRepository dbmsRepository;
    private static final String DATABASE_NAME = "mini_dbms";

    public SelectService(DBMSRepository dbmsRepository) {
        this.dbmsRepository = dbmsRepository;
    }

    public String processSelect(Select selectStatement) throws Exception {
        Element rootDataBases = dbmsRepository.getDoc().getRootElement();
        Optional<Element> databaseElement = getDatabaseElement(rootDataBases);
        if (databaseElement.isEmpty()) {
            throw new Exception("Invalid database!");
        }
        MongoClient client = getMongoClient();
        MongoDatabase database = client.getDatabase(DATABASE_NAME);

        PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();

        List<String> tableNames = extractTableNames(plainSelect);
        String tableName = plainSelect.getFromItem().toString(); // Obținem numele tabelei
        Element tableElement = getTableElement(tableName);
        List<String> primaryKeys = getPrimaryKeys(tableElement);
        List<String> attributes = getAttributesForTable(tableElement);
        List<String> selectedColumns = plainSelect.getSelectItems().stream().map(Objects::toString).collect(Collectors.toList());
        attributes.removeAll(primaryKeys);
        Expression where = plainSelect.getWhere(); // Obținem clauza WHERE

        Map<String, List<String>> result = new LinkedHashMap<>();
        List<String> selectedAttributes = plainSelect.getSelectItems().stream()
                .map(Object::toString)
                .toList();
        if (plainSelect.getJoins() != null && !plainSelect.getJoins().isEmpty()) {
            List<Join> joins = plainSelect.getJoins();
            validateJoins(databaseElement.get(), joins);
            // processHashJoin(database, tableNames, attributes, selectedColumns);
            processIndexedNestedJoins(result, databaseElement.get(), database, joins);
            if (where != null) {
                List<Expression> conditions = extractConditions(where);
                processConditionsForJoin(result, conditions);
            }
            projectionForJoin(result, selectedAttributes);
        } else if (where != null) {
            List<Expression> conditions = extractConditions(where);
            processConditions(rootDataBases, database, tableName, conditions, attributes, primaryKeys, selectedColumns);
        }

        return result.toString();
    }

    public static List<String> extractTableNames(PlainSelect plainSelect) {
        List<String> tableNames = new ArrayList<>();


        // Adăugați numele tabelei de bază (primare)
        tableNames.add(plainSelect.getFromItem().toString());

        // Verificați dacă există uniri (joins)
        if (plainSelect.getJoins() != null) {
            // Iterați prin lista de uniri și adăugați numele tabelelor alăturate
            for (Join join : plainSelect.getJoins()) {
                tableNames.add(join.getRightItem().toString());
            }
        }

        return tableNames;
    }

    private void validateJoins(Element databaseElement, List<Join> joins) throws Exception {
        for (Join join : joins) {
            validateJoin(databaseElement, join);
        }
    }

    private void validateJoin(Element databaseElement, Join join) throws Exception {
        Collection<Expression> expressions = join.getOnExpressions();
        for (Expression expression : expressions) {
            if (expression instanceof EqualsTo equalsTo) {
                validateRightTableFromJoin(databaseElement, join.getRightItem().toString());
                Column leftColum = (Column) equalsTo.getLeftExpression();
                validateTableNameAndColumn(databaseElement, leftColum.getTable().getName(), leftColum.getColumnName());
                Column rightColum = (Column) equalsTo.getRightExpression();
                validateTableNameAndColumn(databaseElement, rightColum.getTable().getName(), rightColum.getColumnName());
            } else {
                throw new Exception("Joins must have expressions of equal type");
            }
        }
    }

    private void validateRightTableFromJoin(Element databaseElement, String tableName) throws Exception {
        Optional<Element> optionalTable =  getTableElementByName(databaseElement, tableName);
        if (optionalTable.isEmpty()) {
            throw new Exception("Table not found!");
        }
    }

    private void validateTableNameAndColumn(Element databaseElement, String tableName, String column) throws Exception {
        Optional<Element> optionalTable =  getTableElementByName(databaseElement, tableName);
        if (optionalTable.isEmpty()) {
            throw new Exception("Table not found!");
        }
        Element attributes = optionalTable.get().getChild("Structure");
        if (attributes.getChildren().stream()
                .noneMatch(attr -> attr.getAttribute("attributeName").getValue().equals(column))) {
            throw new Exception("Invalid attributes in join!");
        }
    }

    private void processIndexedNestedJoins(Map<String, List<String>> result, Element databaseElement, MongoDatabase database, List<Join> joins) throws Exception {
        processIndexedNestedJoin(result, databaseElement, database, joins.get(0), true);
        for (int index = 1; index < joins.size(); index++) {
            processIndexedNestedJoin(result, databaseElement, database, joins.get(index), false);
        }
    }

    private void processIndexedNestedJoin(Map<String, List<String>> result, Element databaseElement, MongoDatabase database, Join join, boolean isFirstJoin) throws Exception {
        Expression onExpression = join.getOnExpression();
        String leftTable = "";
        String leftAttribute = "";
        String rightTable = "";
        String rightAttribute = "";

        if (onExpression instanceof EqualsTo equalsTo) {
            if (equalsTo.getLeftExpression() instanceof Column column) {
                leftTable = column.getTable().toString();
                leftAttribute = column.getColumnName();
            }
            if (equalsTo.getRightExpression() instanceof Column column) {
                rightTable = column.getTable().toString();
                rightAttribute = column.getColumnName();
            }
        }

        if (Objects.equals(leftTable, "") || Objects.equals(leftAttribute, "") || Objects.equals(rightTable, "") || Objects.equals(rightAttribute, "")) {
            throw new Exception("Invalid JOIN!");
        }

        String childTable = "";
        String parentTable = "";
        if (tableHasForeignKey(databaseElement, leftTable, rightTable, rightAttribute)) {
            childTable = leftTable;
            parentTable = rightTable;
        }

        if (tableHasForeignKey(databaseElement, rightTable, leftTable, leftAttribute)) {
            childTable = rightTable;
            parentTable = leftTable;
        }

        if (isFirstJoin) {
            processFirstIndexedNestedJoinForFk(result, databaseElement, database, parentTable, childTable, leftAttribute);
        } else {
            processIndexedNestedJoinForFk(result, databaseElement, database, parentTable, childTable, leftAttribute);
        }
    }

    private void processFirstIndexedNestedJoinForFk(Map<String, List<String>> result, Element databaseElement, MongoDatabase database, String parentTable, String childTable, String attribute) throws Exception {
        String collectionNameForFk = dbmsRepository.getCurrentDatabase() + childTable + "FkInd" + attribute + "Ref" + parentTable;
        MongoCollection<Document> fkCollection = database.getCollection(collectionNameForFk);

        String collectionNameForChildTable = dbmsRepository.getCurrentDatabase() + childTable;
        MongoCollection<Document> parentTableCollection = database.getCollection(dbmsRepository.getCurrentDatabase() + parentTable);

        List<String> attributesForParentTable = getAttributesForTable(databaseElement, parentTable);
        List<String> attributesForChildTable = getAttributesForTable(databaseElement, childTable);

        for (Document parentDoc : parentTableCollection.find()) {
            for (Document fkDoc : fkCollection.find()) {
                if (parentDoc.get("_id").equals(fkDoc.get("_id"))) {
                    List<String> pksForChildTable = Arrays.stream(fkDoc.get("values").toString().split("\\$")).toList();
                    List<String> valuesForParentTable = new ArrayList<>(Arrays.stream(parentDoc.get("values").toString().split("#")).toList());
                    valuesForParentTable.add(0, parentDoc.get("_id").toString());
                    for (String primaryKey : pksForChildTable) {
                        List<String> valuesForTable = new ArrayList<>(getValuesForKey(collectionNameForChildTable, primaryKey));
                        valuesForTable.add(0, primaryKey);
                        addNewValueToResult(result, parentTable, childTable, attribute, attributesForParentTable, attributesForChildTable, valuesForParentTable, valuesForTable);
                    }
                }
            }
        }
    }

    private void addNewValueToResult(Map<String, List<String>> result,
                                     String parentTable,
                                     String childTable,
                                     String attribute,
                                     List<String> attributesForParentTable,
                                     List<String> attributesForChildTable,
                                     List<String> valuesForParentTable,
                                     List<String> valuesForChildTable) {

        for (int index = 0; index < attributesForParentTable.size(); index++) {
            String attributeForParentTable = attributesForParentTable.get(index);
            if (attributeForParentTable.equals(attribute)) {
                result.computeIfAbsent(attributeForParentTable, k -> new ArrayList<>()).add(valuesForParentTable.get(index));
            } else {
                result.computeIfAbsent(parentTable + "." + attributeForParentTable, k -> new ArrayList<>()).add(valuesForParentTable.get(index));
            }
        }

        for (int index = 1; index < attributesForChildTable.size(); index++) {
            String attributeForChildTable = attributesForChildTable.get(index);
            if (!attributeForChildTable.equals(attribute)) {
                result.computeIfAbsent(childTable + "." + attributesForChildTable.get(index), k -> new ArrayList<>()).add(valuesForChildTable.get(index));
            }
        }

        result.computeIfAbsent(attributesForChildTable.get(0), k -> new ArrayList<>()).add(valuesForChildTable.get(0));
    }

    private void processIndexedNestedJoinForFk(Map<String, List<String>> result, Element databaseElement, MongoDatabase database, String parentTable, String childTable, String attribute) throws Exception {
        String collectionNameForFk = dbmsRepository.getCurrentDatabase() + childTable + "FkInd" + attribute + "Ref" + parentTable;
        MongoCollection<Document> fkCollection = database.getCollection(collectionNameForFk);

        String collectionNameForChildTable = dbmsRepository.getCurrentDatabase() + childTable;
        List<String> attributesForChildTable = getAttributesForTable(databaseElement, childTable);

        Map<String, List<String>> tempResult = new HashMap<>();
        List<String> valueForPkFromResult = result.get(attribute);

        for (int index = 0; index < valueForPkFromResult.size(); index++) {
            for (Document fkDoc : fkCollection.find()) {
                if (valueForPkFromResult.get(index).equals(fkDoc.get("_id"))) {
                    List<String> pksForChildTable = Arrays.stream(fkDoc.get("values").toString().split("\\$")).toList();
                    for (String primaryKey : pksForChildTable) {
                        List<String> valuesForTable = new ArrayList<>(getValuesForKey(collectionNameForChildTable, primaryKey));
                        valuesForTable.add(0, primaryKey);
                        addNewValueToTempResult(result, tempResult, index, childTable, attributesForChildTable, valuesForTable);
                    }
                }
            }
        }
        result.clear();
        result.putAll(tempResult);
    }

    private void addNewValueToTempResult(Map<String, List<String>> result,
                                         Map<String, List<String>> tempResult,
                                         int indexForKey,
                                         String childTable,
                                         List<String> attributesForChildTable,
                                         List<String> valuesForChildTable) {


        for (Map.Entry<String, List<String>> entry : result.entrySet()) {
            tempResult.computeIfAbsent(entry.getKey(), k -> new ArrayList<>()).add(entry.getValue().get(indexForKey));
        }
        tempResult.computeIfAbsent(attributesForChildTable.get(0), k -> new ArrayList<>()).add(valuesForChildTable.get(0));

        for (int index = 0; index < attributesForChildTable.size(); index++) {
            tempResult.computeIfAbsent(childTable + "." + attributesForChildTable.get(index), k -> new ArrayList<>()).add(valuesForChildTable.get(index));
        }
    }

    private List<String> getAttributesForTable(Element databaseElement, String tableName) throws Exception {
        Optional<Element> optionalTable =  getTableElementByName(databaseElement, tableName);
        if (optionalTable.isEmpty()) {
            throw new Exception("Table not found!");
        }

        return optionalTable.get().getChild("Structure")
                .getChildren("Attribute")
                .stream().map(attr -> attr.getAttribute("attributeName").getValue())
                .toList();
    }

    private boolean tableHasForeignKey(Element databaseElement, String childTable, String parentTable, String fkAttribute) throws Exception {
        Optional<Element> childTableElement = getTableElementByName(databaseElement, childTable);
        if (childTableElement.isEmpty()) {
            throw new Exception("Invalid table!");
        }
        for (Element fkElement : childTableElement.get().getChild("foreignKeys").getChildren("foreignKey")) {
            Element refElement = fkElement.getChild("references");
            if (refElement.getChild("refTable").getText().equals(parentTable) && refElement.getChild("refAttribute").getText().equals(fkAttribute)) {
                return true;
            }
        }
        return false;
    }

    private List<String> getValuesForKey(String collectionName, String key) throws Exception {
        String values = "";
        try (MongoClient client = getMongoClient()) {
            MongoDatabase mongoDatabase = client.getDatabase(DATABASE_NAME);
            MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
            Document query = new Document("_id", key);
            for (Document document : collection.find(query)) {
                values = (String) document.get("values");
            }
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
        return List.of(values.split("#"));
    }

    public static List<String> getAttributesFromJoin(Join join, String tableName) {
        List<String> attributes = new ArrayList<>();
        Expression onExpression = join.getOnExpression();

        if (onExpression instanceof BinaryExpression binaryExpression) {
            extractAttributesFromBinaryExpression(binaryExpression, tableName, attributes);
        }

        return attributes;
    }

    private static void extractAttributesFromBinaryExpression(BinaryExpression expr, String tableName, List<String> attributes) {
        if (expr instanceof EqualsTo equalsTo) {

            Expression leftExpression = equalsTo.getLeftExpression();
            Expression rightExpression = equalsTo.getRightExpression();

            extractAttributeFromColumnExpression(leftExpression, tableName, attributes);
            extractAttributeFromColumnExpression(rightExpression, tableName, attributes);
        } else if (expr instanceof OrExpression) {
            extractAttributesFromBinaryExpression((BinaryExpression) expr.getLeftExpression(), tableName, attributes);
            extractAttributesFromBinaryExpression((BinaryExpression) expr.getRightExpression(), tableName, attributes);
        }
    }

    private static void extractAttributeFromColumnExpression(Expression expression, String tableName, List<String> attributes) {
        if (expression instanceof Column column) {
            if (column.getTable().getName().equalsIgnoreCase(tableName)) {
                attributes.add(column.getColumnName());
            }
        }
    }

    private void processRecursiveSortMergeJoin(MongoDatabase database, List<String> tableNames, List<String> joinAttributes, List<String> selectedColumns) {
        if (tableNames.size() >= 2) {
            String leftTableName = tableNames.get(0);
            tableNames.remove(0);

            String leftJoinAttribute = "_id";
            String rightJoinAttribute = "values";
            System.out.println(rightJoinAttribute);

            MongoCollection<Document> leftCollection = database.getCollection(dbmsRepository.getCurrentDatabase() + leftTableName);

            List<String> result = new ArrayList<>();
            int resultIndex = 0;
            boolean isFirst = true;
            for (String rightTableName : tableNames) {
                List<String> appendResult = new ArrayList<>();
                MongoCollection<Document> rightCollection = database.getCollection(dbmsRepository.getCurrentDatabase() + rightTableName);

                // Sortăm tabelele după atributul de join
                Bson sortLeft = Sorts.ascending(leftJoinAttribute);
                Bson sortRight = Sorts.ascending(rightJoinAttribute);
                FindIterable<Document> leftSorted = leftCollection.find().sort(sortLeft);
                FindIterable<Document> rightSorted = rightCollection.find().sort(sortRight);

                // Inițializăm iteratorii pentru tabelele sortate
                MongoCursor<Document> leftCursor = leftSorted.iterator();
                MongoCursor<Document> rightCursor = rightSorted.iterator();

                List<Document> leftDocs = new ArrayList<>();
                List<Document> rightDocs = new ArrayList<>();

                // Adăugăm documentele sortate în liste
                while (leftCursor.hasNext()) {
                    leftDocs.add(leftCursor.next());
                }
                while (rightCursor.hasNext()) {
                    rightDocs.add(rightCursor.next());
                }

                // Realizăm Sort Merge Join între tabelele sortate
                List<Document> tuples = new ArrayList<>();
                int leftIndex = 0, rightIndex = 0;

                while (leftIndex < leftDocs.size() && rightIndex < rightDocs.size()) {
                    Document leftDoc = leftDocs.get(leftIndex);
                    Document rightDoc = rightDocs.get(rightIndex);


                    // Obținem valorile atributelor de join din documentele curente
                    Object leftJoinValue = leftDoc.get(leftJoinAttribute);
                    Object rightJoinValue = rightDoc.get(rightJoinAttribute).toString().split("#")[0];


                    // Comparăm valorile pentru a verifica condiția de join
                    int comparisonResult = ((Comparable) leftJoinValue).compareTo(rightJoinValue);

                    if (comparisonResult == 0) {
                        // Dacă valorile sunt egale, facem join și adăugăm rezultatul în lista de tuple
                    /*Document tuple = new Document();
                    for (String column : selectedColumns) {
                        if (leftDoc.containsKey(column)) {
                            tuple.append(column, leftDoc.get(column));
                        } else if (rightDoc.containsKey(column)) {
                            tuple.append(column, rightDoc.get(column));
                        }
                    }
                    tuples.add(tuple);*/
                        if (!isFirst) {
                            appendResult.add(leftDoc.toString() + rightDoc);
                            //result.set(resultIndex, result.get(resultIndex) + rightDoc);
                            resultIndex++;
                        } else {
                            appendResult.add(leftDoc.toString() + rightDoc);
                        }


                        rightIndex++;
                    } else if (comparisonResult < 0) {
                        leftIndex++;
                    } else {
                        rightIndex++;
                    }
                }
                appendResult.forEach(System.out::println);
                System.out.println();
                leftCollection = rightCollection;
                resultIndex = 0;
                isFirst = false;
            }


            // result.forEach(System.out::println);
        } else {
            System.out.println("Sunt necesare cel puțin două tabele pentru JOIN.");
        }
    }

    private void processHashJoin(MongoDatabase database, List<String> tableNames, List<String> joinAttribute, List<String> selectedColumns) {
        String leftTableName = tableNames.get(0);
        tableNames.remove(0);

        String leftJoinAttribute = "_id";
        String rightJoinAttribute = "values";

        MongoCollection<Document> leftCollection = database.getCollection(dbmsRepository.getCurrentDatabase() + leftTableName);

        List<String> result = new ArrayList<>();
        int resultIndex = 0;
        boolean isFirst = true;

        for (String rightTableName : tableNames) {
            List<String> appendResult = new ArrayList<>();
            MongoCollection<Document> rightCollection = database.getCollection(dbmsRepository.getCurrentDatabase() + rightTableName);

            // Pasul 1: Crearea unei structuri de tabelă hash în memorie pentru colecția cu mai puține elemente
            Map<Object, Document> hashTable = new HashMap<>();
            FindIterable<Document> smallerRecords = leftCollection.find();
            for (Document record : smallerRecords) {
                Object key = record.get(leftJoinAttribute);
                hashTable.put(key, record);
            }

            // Pasul 2: Iterarea colecției mai mari și găsirea înregistrărilor corespunzătoare în tabela hash
            FindIterable<Document> largerRecords = rightCollection.find();
            List<Document> tuples = new ArrayList<>();
            for (Document record : largerRecords) {
                Object joinKeyValue = record.get(rightJoinAttribute).toString().split("#")[0];
                Document matchingRecord = hashTable.get(joinKeyValue);

                if (matchingRecord != null) {
                    // Dacă găsim o corespondență, construim un tuple pentru rezultatul JOIN-ului
                    Document tuple = new Document();
                /*for (String column : selectedColumns) {
                    if (matchingRecord.containsKey(column)) {
                        tuple.append(column, matchingRecord.get(column));
                    }
                }
                tuples.add(tuple);*/
                    if (!isFirst) {
                        //System.out.println(resultIndex);
                        appendResult.add(matchingRecord.toString() + record);
                        resultIndex++;
                    } else {
                        appendResult.add(matchingRecord.toString() + record);
                    }
                    //System.out.println(record);
                    //System.out.println(matchingRecord);
                    //System.out.println();
                }
            }
            appendResult.forEach(System.out::println);
            System.out.println();
            result = appendResult;
            //result.forEach(System.out::println);
            leftCollection = rightCollection;
            resultIndex = 0;
            isFirst = false;
        }
        //result.forEach(System.out::println);

        // Afișăm rezultatul Hash Join
        /*for (Document tuple : tuples) {
            System.out.println(tuple.toJson());
        }*/
    }


    private List<Expression> extractConditions(Expression where) {
        List<Expression> conditions = new ArrayList<>();

        if (where instanceof AndExpression) {
            AndExpression andExpression = (AndExpression) where;
            conditions.addAll(extractConditions(andExpression.getLeftExpression()));
            conditions.addAll(extractConditions(andExpression.getRightExpression()));
        } else {
            conditions.add(where);
        }

        return conditions;
    }

    private void processConditionsForJoin(Map<String, List<String>> result, List<Expression> conditions) throws Exception {
        Column column;
        String conditionValue;
        String attribute;

        for (Expression condition : conditions) {
            if (condition instanceof ComparisonOperator comparisonOperator) {
                column = (Column) comparisonOperator.getLeftExpression();
                attribute = column.getTable() + "." + column.getColumnName();
                conditionValue = comparisonOperator.getRightExpression().toString();
                filterResult(result, attribute, conditionValue);
            }
        }
    }

    private void filterResult(Map<String, List<String>> result, String attribute, String conditionValue) {
        for (Map.Entry<String, List<String>> entry : result.entrySet()) {
            if (entry.getKey().equals(attribute)) {
                List<Integer> indexes = IntStream.range(0, entry.getValue().size())
                        .filter(i -> !entry.getValue().get(i).equals(conditionValue))
                        .boxed()
                        .sorted(Comparator.reverseOrder())
                        .toList();

                for (Map.Entry<String, List<String>> entry2 : result.entrySet()) {
                    indexes.forEach(index -> entry2.getValue().remove(index.intValue()));
                }
            }
        }
    }

    private void projectionForJoin(Map<String, List<String>> result, List<String> attributes) {
        result.entrySet().removeIf(entry -> !attributes.contains(entry.getKey()));
    }

    private void processConditions(Element rootDataBases, MongoDatabase database, String tableName, List<Expression> conditions, List<String> attributes, List<String> primaryKeys, List<String> selectedColumns) throws Exception {
        List<String> allMatchingIDs = new ArrayList<>();
        List<Expression> whereNoIndex = new ArrayList<>();

        for (Expression condition : conditions) {
            if (condition instanceof ComparisonOperator || condition instanceof LikeExpression) {
                List<String> matchingIDs = processSingleCondition(rootDataBases, database, tableName, condition, whereNoIndex);
                if (allMatchingIDs.isEmpty()) {
                    allMatchingIDs.addAll(matchingIDs);
                } else {
                    if (!Objects.equals(matchingIDs.get(0), "-1"))
                        allMatchingIDs.retainAll(matchingIDs);
                }
            }
        }

        if (!allMatchingIDs.isEmpty()) {
            MongoCollection<Document> studentsCollection = database.getCollection(dbmsRepository.getCurrentDatabase() + tableName);
            List<Document> result = retrieveEntities(studentsCollection, allMatchingIDs);
            result = computeInMemoryFilters(result, attributes, whereNoIndex);

            if (selectedColumns.contains("*")) {
                selectedColumns.clear();
                selectedColumns.addAll(primaryKeys);
                selectedColumns.addAll(attributes);
            }

            selectedColumns.forEach(column -> System.out.print(column + " "));
            System.out.println();

            result.forEach(document -> {
                selectedColumns.forEach(
                        selectedColumn -> {
                            Integer keyPosition = primaryKeys.indexOf(selectedColumn);
                            Integer valuePosition = attributes.indexOf(selectedColumn);
                            if (keyPosition != -1) {
                                System.out.print(document.getString("_id").split("$")[keyPosition] + " ");
                            } else if (valuePosition != -1) {
                                System.out.print(document.getString("values").split("#")[valuePosition] + " ");
                            }
                        }
                );
                System.out.println();
            });
        } else {
            System.out.println("Nu au fost găsite înregistrări care să satisfacă toate condițiile.");
        }
    }

    private List<Document> computeInMemoryFilters(List<Document> result, List<String> attributes, List<Expression> whereNoIndex) {
        for (Expression expression : whereNoIndex) {
            if (expression instanceof LikeExpression) {
                LikeExpression likeExpression = (LikeExpression) expression;
                Column column = (Column) likeExpression.getLeftExpression();
                String conditionValue = likeExpression.getRightExpression().toString().replace("'", "");
                String operator = likeExpression.getStringExpression();


                int columnIndex = attributes.indexOf(column.getColumnName());


                if (columnIndex != -1)
                    return result.stream()
                            .filter(doc -> {
                                Pattern pattern = Pattern.compile(conditionValue);
                                String columnValue = doc.getString("values").split("#")[columnIndex];
                                Matcher matcher = pattern.matcher(columnValue);
                                return matcher.find();
                            }).toList();
            }
            if (expression instanceof ComparisonOperator) {
                ComparisonOperator comparisonOperator = (ComparisonOperator) expression;
                Column column = (Column) comparisonOperator.getLeftExpression();
                Integer conditionValue = Integer.parseInt(comparisonOperator.getRightExpression().toString());
                String operator = comparisonOperator.getStringExpression();

                int columnIndex = attributes.indexOf(column.getColumnName());

                Stream<Document> filteredStream = result.stream();

                switch (operator) {
                    case ">":
                        filteredStream = filteredStream.filter(doc -> {
                            int columnValue = Integer.parseInt(doc.getString("values").split("#")[columnIndex]);
                            return columnValue > conditionValue;
                        });
                        break;
                    case ">=":
                        filteredStream = filteredStream.filter(doc -> {
                            int columnValue = Integer.parseInt(doc.getString("values").split("#")[columnIndex]);
                            return columnValue >= conditionValue;
                        });
                        break;
                    case "<":
                        filteredStream = filteredStream.filter(doc -> {
                            int columnValue = Integer.parseInt(doc.getString("values").split("#")[columnIndex]);
                            return columnValue < conditionValue;
                        });
                        break;
                    case "<=":
                        filteredStream = filteredStream.filter(doc -> {
                            int columnValue = Integer.parseInt(doc.getString("values").split("#")[columnIndex]);
                            return columnValue <= conditionValue;
                        });
                        break;
                    case "=":
                        filteredStream = filteredStream.filter(doc -> {
                            int columnValue = Integer.parseInt(doc.getString("values").split("#")[columnIndex]);
                            return columnValue == conditionValue;
                        });
                        break;
                    default:
                        break;
                }

                return filteredStream.toList();
            }
        }
        return result;
    }

    List<String> getAttributesForTable(Element tableElement) {
        List<String> attributeNames = new ArrayList<>();
        List<Element> attributeElements = tableElement.getChild("Structure").getChildren("Attribute");
        for (Element attribute : attributeElements) {
            String attributeName = attribute.getAttributeValue("attributeName");
            attributeNames.add(attributeName);
        }
        return attributeNames;
    }

    private static List<String> getPrimaryKeys(Element tableElement) {
        return tableElement.getChild("primaryKey").getChildren("pkAttribute").stream()
                .map(pk -> pk.getContent().get(0).getValue())
                .toList();
    }

    private Element getTableElement(String tableName) {
        Element rootDataBases = dbmsRepository.getDoc().getRootElement().getChild("DataBase");
        List<Element> tables = rootDataBases.getChildren("Table");
        for (Element table : tables) {
            String currentTableName = table.getAttributeValue("tableName");
            if (currentTableName.equals(tableName)) {
                return table;
            }
        }
        return null;
    }

    private List<String> processSingleCondition(Element rootDataBases, MongoDatabase database, String tableName, Expression where, List<Expression> whereNoIndex) throws Exception {
        List<String> matchingIDs = new ArrayList<>();
        matchingIDs.add("-1");

        if (where instanceof ComparisonOperator || where instanceof LikeExpression) {
            Column column;
            String conditionValue;
            String operator;

            if (where instanceof ComparisonOperator) {
                ComparisonOperator comparisonOperator = (ComparisonOperator) where;
                column = (Column) comparisonOperator.getLeftExpression();
                conditionValue = comparisonOperator.getRightExpression().toString();
                operator = comparisonOperator.getStringExpression();
            } else {
                LikeExpression likeExpression = (LikeExpression) where;
                column = (Column) likeExpression.getLeftExpression();
                conditionValue = likeExpression.getRightExpression().toString();
                operator = likeExpression.getStringExpression();
            }

            String columnName = column.getColumnName();
            List<String> tempMatchingIDs = new ArrayList<>();

            if (existIndexForColumn(rootDataBases, tableName, columnName)) {
                MongoCollection<Document> indexCollection = database.getCollection(dbmsRepository.getCurrentDatabase() + tableName + "Ind" + columnName);
                tempMatchingIDs = findMatchingIDs(indexCollection, columnName, conditionValue, operator);

                if (!tempMatchingIDs.isEmpty()) {
                    if (Objects.equals(matchingIDs.get(0), "-1")) {
                        matchingIDs.clear();
                        matchingIDs.addAll(tempMatchingIDs);
                    } else {
                        matchingIDs.retainAll(tempMatchingIDs);
                    }
                }
            } else
                whereNoIndex.add(where);


        }

        return matchingIDs;
    }

    private static boolean existIndexForColumn(Element rootDataBases, String tableName, String columnName) {
        Element table = rootDataBases.getChild("DataBase").getChild("Table");
        String tableAttrName = table.getAttributeValue("tableName");
        if (tableAttrName.equals(tableName)) {
            List<Element> indexFiles = table.getChild("IndexFiles").getChildren();
            for (Element indexFile : indexFiles) {
                String indexAttrName = indexFile.getChild("IndexAttributes").getChildText("IAttribute");
                if (indexAttrName.equals(columnName))
                    return true;
            }
        }
        return false;
    }

    private static List<String> findMatchingIDs(
            MongoCollection<Document> indexCollection,
            String columnName,
            String conditionValue,
            String operator
    ) {
        List<String> matchingIDs = new ArrayList<>();
        Document query = new Document();

        switch (operator) {
            case ">":
                query.append("$expr", new Document("$gt", Arrays.asList(new Document("$toInt", "$_id"), Integer.parseInt(conditionValue))));
                break;
            case ">=":
                query.append("$expr", new Document("$gte", Arrays.asList(new Document("$toInt", "$_id"), Integer.parseInt(conditionValue))));
                break;
            case "<":
                query.append("$expr", new Document("$lt", Arrays.asList(new Document("$toInt", "$_id"), Integer.parseInt(conditionValue))));
                break;
            case "<=":
                query.append("$expr", new Document("$lte", Arrays.asList(new Document("$toInt", "$_id"), Integer.parseInt(conditionValue))));
                break;
            case "=":
                query.append("_id", new Document("$eq", conditionValue));
                break;
            case "LIKE":
                String patternForMongoDB = conditionValue
                        .replace("'", "");
                Pattern regexPattern = Pattern.compile(patternForMongoDB);
                query.append("_id", new Document("$regex", regexPattern));
                break;
            default:
                break;
        }
        for (Document document : indexCollection.find(query)) {
            matchingIDs.addAll(Arrays.asList(document.get("values").toString().split("\\$")));
        }

        return matchingIDs;
    }

    private static List<Document> retrieveEntities(MongoCollection<Document> studentsCollection, List<String> matchingIDs) {
        List<Document> entities = new ArrayList<>();

        for (String id : matchingIDs) {
            Document document = studentsCollection.find(new Document("_id", id)).first();
            if (document != null) {
                entities.add(document);
            }
        }

        return entities;
    }
}
