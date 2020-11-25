package com.jerome.utils;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;

/**
 * This is Description
 *
 * @author Jerome丶子木
 * @date 2020/01/02
 */
public class XMLUtil {

    public static Object getBean(String tagName,String xmlName){
        DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        try {
            DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
            Document document = documentBuilder.parse(XMLUtil.class.getResourceAsStream(xmlName));
            NodeList classNameList = document.getElementsByTagName(tagName);
            Node firstChild = classNameList.item(0).getFirstChild();
            System.out.println("<" + tagName + "> : " + firstChild.getNodeValue() );

            Class<?> forName = Class.forName(firstChild.getNodeValue());

            return forName.newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }
}
