package org.example;

import net.sf.jsqlparser.statement.create.table.Index;
import org.jdom2.*;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class SQLToXMLConverter {
    private static void executeCommand(String sqlCommand){
        try {
            Statement statement = CCJSqlParserUtil.parse(sqlCommand);
            System.out.println(statement);
            if (statement instanceof CreateTable) {
                CreateTable createTable = (CreateTable) statement;
                String tableName = createTable.getTable().getName();
                String fileName = "Spec.bin"; // Fișierul poate fi hardcodat sau specificat de utilizator
                int rowLength = 0; // Calculați rowLength în funcție de coloanele tabelului

                // Crearea elementului radacina "<Table>"
                Element rootElement = new Element("Table");
                rootElement.setAttribute("tableName", tableName);
                rootElement.setAttribute("fileName", fileName);
                rootElement.setAttribute("rowLength", String.valueOf(rowLength));

                // Crearea elementului "<Structure>"
                Element structureElement = new Element("Structure");
                for (ColumnDefinition column : createTable.getColumnDefinitions()) {
                    String attributeName = column.getColumnName();
                    String type = column.getColDataType().getDataType();
                    int isNull = column.toString().contains("NOT NULL") ? 0 : 1;

                    Element attributeElement = new Element("Attribute");
                    attributeElement.setAttribute("attributeName", attributeName);
                    attributeElement.setAttribute("type", type);
                    if(type.equalsIgnoreCase("char")||type.equalsIgnoreCase("varchar")){
                        int length = column.getColDataType().getArgumentsStringList().size() > 0 ? Integer.parseInt(column.getColDataType().getArgumentsStringList().get(0)) : 30;
                        attributeElement.setAttribute("length", String.valueOf(length));
                    }
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



                for (Index index : createTable.getIndexes()) {
                    if (index.getType().equals("PRIMARY KEY")) {
                        Element indexFileElement = new Element("IndexFile");
                        indexFileElement.setAttribute("indexName", index.getColumnsNames().get(0) + ".ind");
                        indexFileElement.setAttribute("keyLength", "12");
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
                                // Extrageți informațiile relevante din constrângere
                                //String foreignKeyColumns = matcher.group(1);
                                String referencedTable = matcher.group(2);
                                String referencedColumns = matcher.group(3);


                                for(String columnName:index.getColumnsNames()){
                                    Element fkAttributeElement = new Element("fkAttribute");
                                    fkAttributeElement.setText(columnName);
                                    foreignKeyElement.addContent(fkAttributeElement);
                                }

                                Element referencesElement = new Element("references");
                                foreignKeyElement.addContent(referencesElement);

                                Element refTableElement = new Element("refTable");
                                refTableElement.setText(referencedTable);
                                referencesElement.addContent(refTableElement);

                                for(String refColumn:referencedColumns.split(",")){
                                    Element fkAttributeElement = new Element("fkAttribute");
                                    fkAttributeElement.setText(refColumn.trim());
                                    foreignKeyElement.addContent(fkAttributeElement);
                                }
                            }}
                }


                // Adăugați elementele create la elementul radacină "<Table>"
                rootElement.addContent(structureElement);
                rootElement.addContent(primaryKeyElement);
                rootElement.addContent(foreignKeysElement);
                rootElement.addContent(uniqueKeysElement);
                rootElement.addContent(indexFilesElement);

                // Crearea documentului XML
                Document doc = new Document(rootElement);

                // Setarea versiunii și codificării XML
                Format format = Format.getPrettyFormat();
                format.setEncoding("UTF-8");
                format.setLineSeparator("\n");

                // Crearea unui fișier XML
                XMLOutputter xmlOutput = new XMLOutputter(format);
                xmlOutput.output(doc, new FileWriter("output.xml"));

            } else {
                System.out.println("Comanda SQL nu este o comandă de creare a tabelului.");
            }
        } catch (JSQLParserException | IOException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        System.out.print("Introduceți ceva: ");
        String input = scanner.nextLine(); // Citirea unei linii de la intrarea standard

        System.out.println("Ați introdus: " + input);
        executeCommand(input);
        // Închideți scanner-ul când ați terminat de citit
        scanner.close();
        String sqlCommand = "CREATE TABLE Specialization (" +
                "    Specid CHAR(3) NOT NULL," +
                "    SpecName VARCHAR(30) NOT NULL," +
                "    Language VARCHAR(20) NOT NULL," +
                "    PRIMARY KEY (Specid, Language)," +
                "    CONSTRAINT UC_SpecName UNIQUE (SpecName, Language)" +
                ");";


    }

}
