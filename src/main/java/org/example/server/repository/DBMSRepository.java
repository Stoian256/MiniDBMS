package org.example.server.repository;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class DBMSRepository {
    public String catalogPath;
    public Document doc;
    public String currentDatabase;
    public DBMSRepository(String catalogPath){
        this.catalogPath = catalogPath;
        this.currentDatabase = "";
        try {
            File xmlFile = new File(catalogPath);
            if (xmlFile.exists()) {
                SAXBuilder saxBuilder = new SAXBuilder();
                doc = saxBuilder.build(xmlFile);
            } else {
                doc = new Document(new Element("Databases"));
            }

            Format format = Format.getPrettyFormat();
            format.setEncoding("UTF-8");
            format.setExpandEmptyElements(true);
            format.setLineSeparator("\n");

            XMLOutputter xmlOutput = new XMLOutputter(format);
            xmlOutput.output(doc, new FileWriter(catalogPath));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (JDOMException e) {
            throw new RuntimeException(e);
        }
    }
}
