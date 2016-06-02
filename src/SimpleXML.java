
    import org.w3c.dom.Document;
    import org.w3c.dom.Element;
    import org.w3c.dom.Node;
    import org.w3c.dom.NodeList;
    import org.xml.sax.SAXException;

    import javax.xml.parsers.DocumentBuilder;
    import javax.xml.parsers.DocumentBuilderFactory;
    import javax.xml.parsers.ParserConfigurationException;
    import javax.xml.transform.OutputKeys;
    import javax.xml.transform.Transformer;
    import javax.xml.transform.TransformerException;
    import javax.xml.transform.TransformerFactory;
    import javax.xml.transform.dom.DOMSource;
    import javax.xml.transform.stream.StreamResult;
    import java.io.IOException;
    import java.io.InputStream;
    import java.io.OutputStream;
    import java.util.ArrayList;
    import java.util.List;

    /**
     * Created by pnegre on 30/05/16.
     *
     * Classe que serveix per simplificar el tractament dels fitxers XML
     *
     * <p>Llegir document:</p>
     *
     * <pre>
     * SimpleXML xml = new SimpleXML(new FileInputStream(FILENAME));
     * Document doc = xml.getDoc();
     * Element root = doc.getDocumentElement();
     * </pre>
     *
     * <p>Obtenir elements "fill": </p>
     *
     * <pre>
     * List<Element> list = xml.getChildElements(element)
     * </pre>
     *
     * <p>Obtenir els attributs i el text d'un element:</p>
     *
     * <pre>
     * String text = element.getTextContent();
     * String att = element.getAttribute("nomAtribut").getValue();
     * </pre>
     *
     */
    public class SimpleXML {
        private DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        private Document doc;

        /**
         * Construeix un objecte de tipus SimpleXML, sense par&agrave;metres
         *
         * @throws ParserConfigurationException
         */
        public SimpleXML() throws ParserConfigurationException {
            doc = db.newDocument();
        }

        /**
         * Construeix un objecte de tipus SimpleXML i llegeix des d'un InputStream.
         * Fa el parseig de l'estructura XML
         *
         * @param is InputStream d'on es llegir&agrave; la informaci&oacute;
         * @throws ParserConfigurationException
         * @throws IOException
         * @throws SAXException
         */
        public SimpleXML(InputStream is) throws ParserConfigurationException, IOException, SAXException {
            doc = db.parse(is);
        }

        /**
         * Torna el Document
         *
         * @return l'objecte de tipus Document (org.w3c.dom.Document)
         */
        public Document getDoc() {
            return doc;
        }

        /**
         * Escriu la informaci&oacute; en forma de XML a un OutputStream
         *
         * @param os OutputStream on s'escriur&agrave; l'XML
         * @throws TransformerException
         */
        public void write(OutputStream os) throws TransformerException {
            Transformer tf = TransformerFactory.newInstance().newTransformer();
            tf.setOutputProperty(OutputKeys.INDENT, "yes");
            StreamResult sr = new StreamResult(os);
            tf.transform(new DOMSource(doc), sr);
        }

        /**
         * Torna una llista dels elements "fill" de l'element que es passa per par&agrave;metre
         *
         * @param parent Element "pare"
         * @return Els elements "fills" de l'element que es passa per par&agrave;metre
         */
        public List<Element> getChildElements(Element parent) {
            return getElements(parent.getChildNodes());
        }

        /**
         * Torna una llista dels elements "fill" de l'element que es passa per par&agrave;metre,
         * per&ograve; filtrem per nom.
         *
         * @param parent Element "pare"
         * @param search Nom dels nodes que volem cercar
         * @return Llista d'elements fills, filtrat per nom
         */
        public List<Element> getChildElements(Element parent, String search) {
            return getElements(parent.getElementsByTagName(search));
        }

        /**
         * Torna un sol element, fill de l'element que es passa per par&agrave;metre, i que tingui
         * el nom igual al par&agrave;metre "search". Nom&eacute;s tornar&agrave; el primer element.
         *
         * @param parent Element "pare"
         * @param search Nom del node a cercar
         * @return Element que compleix els criteris de cerca
         */
        public Element getElement(Element parent, String search) {
            return this.getChildElements(parent, search).get(0);
        }


        private List<Element> getElements(NodeList list) {
            List<Element> result = new ArrayList<Element>();
            for (int i = 0; i < list.getLength(); i++) {
                Node n = list.item(i);
                if (n.getNodeType() != Node.ELEMENT_NODE) continue;
                result.add((Element) n);
            }
            return result;
        }

    }


