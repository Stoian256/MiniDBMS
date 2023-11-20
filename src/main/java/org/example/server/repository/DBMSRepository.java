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
    private String catalogPath;
    private Document doc;
    private String currentDatabase;
    private Format format;
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

            format = Format.getPrettyFormat();
            format.setEncoding("UTF-8");
            //format.setExpandEmptyElements(true);
            format.setLineSeparator("\n");

            XMLOutputter xmlOutput = new XMLOutputter(format);
            xmlOutput.output(doc, new FileWriter(catalogPath));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (JDOMException e) {
            throw new RuntimeException(e);
        }
    }

    public void saveToFile() throws IOException {
        XMLOutputter xmlOutput = new XMLOutputter(format);
        xmlOutput.output(doc, new FileWriter(catalogPath));
    }
    public String getCatalogPath() {
        return catalogPath;
    }

    public void setCatalogPath(String catalogPath) {
        this.catalogPath = catalogPath;
    }

    public Document getDoc() {
        File xmlFile = new File(catalogPath);
        if (xmlFile.exists()) {
            SAXBuilder saxBuilder = new SAXBuilder();
            try {
                doc = saxBuilder.build(xmlFile);
            } catch (JDOMException | IOException e) {
                throw new RuntimeException(e);
            }
        }
        return doc;
    }

    public void setDoc(Document doc) {
        this.doc = doc;
    }

    public String getCurrentDatabase() {
        return currentDatabase;
    }

    public void setCurrentDatabase(String currentDatabase) {
        this.currentDatabase = currentDatabase;
    }

    public Format getFormat() {
        return format;
    }

    public void setFormat(Format format) {
        this.format = format;
    }
}
