import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

/**
 * Created by fjcambilr on 01/06/16.
 */
public class PracticaXML {

    public static void main(String[] args) throws IOException, SAXException, ParserConfigurationException, SQLException, ClassNotFoundException {

        SimpleXML xml = new SimpleXML(new FileInputStream("/tmp/dataset.xml"));
        Document doc = xml.getDoc();
        Element raiz = doc.getDocumentElement();
        Database d = new Database();

        List<Element> list = xml.getChildElements(raiz);
        for (Element e : list) {
            int id_persona = Integer.parseInt(xml.getElement(e, "id").getTextContent());
            String gender = xml.getElement(e, "gender").getTextContent();
            String first_name = xml.getElement(e, "first_name").getTextContent();
            String last_name = xml.getElement(e, "last_name").getTextContent();
            String email = xml.getElement(e, "email").getTextContent();
            String ip = xml.getElement(e, "ip_address").getTextContent();
            String city = xml.getElement(e, "city").getTextContent();
            String country = xml.getElement(e, "country").getTextContent();
            int number = Integer.parseInt(xml.getElement(e, "number").getTextContent());
            Country coun = new Country(country);
            d.insertCountry(coun);
            City ci = new City(city, coun);
            d.insertCity(ci);
            Persona p = new Persona(id_persona,gender, first_name, last_name, email, ip, number, ci);
            d.insertPersona(p);
        }
        d.connection.close();
    }
}

class Persona {
    public int getId_persona() {
        return id_persona;
    }

    int id_persona;
    String gender;
    String first_name;
    String last_name;
    String email;
    String ip;
    City city;
    int number;


    public int getNumber() {
        return number;
    }


    public String getGender() {
        return gender;
    }

    public String getFirst_name() {
        return first_name;
    }

    public String getLast_name() {
        return last_name;
    }

    public String getEmail() {
        return email;
    }

    public String getIp() {
        return ip;
    }

    public City getCity() {
        return city;
    }

    public Persona(int id_persona,String gender, String first_name, String last_name, String email, String ip, int number, City city) {
       this.id_persona = id_persona;
        this.gender = gender;
        this.first_name = first_name;
        this.last_name = last_name;
        this.email = email;
        this.ip = ip;
        this.city = city;
        this.number = number;
    }

}

class City {
    public int getId_ciutat() {
        return id_ciutat;
    }

    public String getCiutat() {
        return ciutat;
    }

    int id_ciutat;
    String ciutat;
    Country country;

    public City(String ciutat, Country country) {
        this.ciutat = ciutat;
        this.country = country;
    }


}

class Country {
    private int id_country;
    private String nameCountry;

    public int getId_country() {
        return id_country;
    }

    public String getNameCountry() {
        return nameCountry;
    }

    public Country(String country) {
        this.nameCountry = country;
    }

}

class Database {
    private final String user = "root";
    private final String password = "terremoto11";
    private final String dbClassName = "com.mysql.jdbc.Driver";
    private final String CONNECTION = "jdbc:mysql://172.16.10.156/xml";
    public java.sql.Connection connection;
    private String sql = "";

    public Database() throws ClassNotFoundException, SQLException {
        // creates a drivermanager class factory
        Class.forName(dbClassName);
        // Properties for user and password. Here the user and password are both 'paulr'
        Properties p = new Properties();
        p.put("user", user);
        p.put("password", password);
        // Now try to connect
        connection = DriverManager.getConnection(CONNECTION, p);
    }

    void insertCountry(Country country) throws SQLException {
        sql = "SELECT name FROM country WHERE name=?";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setString(1, country.getNameCountry());
        ResultSet rs = ps.executeQuery();
        if (!rs.next()) {
            sql = "INSERT INTO country (name) VALUES(?)";
            PreparedStatement ps2 = connection.prepareStatement(sql);
            ps2.setString(1, country.getNameCountry());
            ps2.execute();
        }
    }

    void insertCity(City city) throws SQLException {
        sql = "SELECT name,country FROM city WHERE name=? and country=?";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setString(1, city.getCiutat());
        ps.setInt(2, idCountry(city.country.getNameCountry()));
        ResultSet rs = ps.executeQuery();
        if (!rs.next()) {
            sql = "INSERT INTO city (name,country) VALUES(?,?)";
            PreparedStatement ps2 = connection.prepareStatement(sql);
            ps2.setString(1, city.getCiutat());
            ps2.setInt(2, idCountry(city.country.getNameCountry()));
            ps2.execute();
        }
    }

    public int idCity(String name) throws SQLException {
        sql = "SELECT id_city FROM city WHERE name=?";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setString(1, name);
        ResultSet rs = ps.executeQuery();
        rs.next();
        return rs.getInt("id_city");
    }

    public int idCountry(String name) throws SQLException {
        sql = "SELECT id_country FROM country WHERE name=?";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setString(1, name);
        ResultSet rs = ps.executeQuery();
        rs.next();
        return rs.getInt("id_country");
    }

    public void insertPersona(Persona p) throws SQLException {
        sql = "SELECT * FROM persona WHERE email=?";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setString(1, p.getEmail());
        ResultSet rs = ps.executeQuery();
        if (!rs.next()) {
            sql = "INSERT INTO persona VALUES(?,?,?,?,?,?,?,?)";
            PreparedStatement ps2 = connection.prepareStatement(sql);
            ps2.setInt(1, p.getId_persona());
            ps2.setString(2, p.getGender());
            ps2.setString(3, p.getFirst_name());
            ps2.setString(4, p.getLast_name());
            ps2.setString(5, p.getEmail());
            ps2.setString(6, p.getIp());
            ps2.setInt(7, idCity(p.city.getCiutat()));
            ps2.setInt(8, p.getNumber());
            ps2.execute();
        }
    }

}




