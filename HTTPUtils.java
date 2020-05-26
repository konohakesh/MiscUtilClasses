package com.lokesh.core.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import org.jsoup.Jsoup;

/**
 * This class has APIs for general HTTP work
 */
public class HTTPUtils
{
    // Log4j logger object
    private static final Logger logger = Logger.getLogger( HTTPUtils.class );

    static
    {
        // initialize log4j
        init();
    }

    private HTTPUtils()
    {
    }

    /**
     * Gets URL Connection object
     * @param urlString URL whose URLConnection object to return
     * @throws IOException
     * @return URLConnection object
     */
    public static URLConnection makeConnection( String urlString ) throws IOException
    {
        logger.debug( "Entering HTTPUtils.makeConnection() with: [" + urlString + "]" );

        URL url = new URL( urlString );

        URLConnection connection = url.openConnection();
        connection.setRequestProperty( "user-agent",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36" );
        connection.setRequestProperty( "accept",
                "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9" );
        connection.connect();

        logger.debug( "Exiting HTTPUtils.makeConnection() with: []" );
        return connection;
    }

    /**
     * Gets HTML markup from a URL
     * @param url URL whose HTML markup is needed
     * @throws IOException
     * @return StringBuilder object of HTML mark up
     */
    public static StringBuilder getHTMLFromURL( String url ) throws IOException
    {
        logger.debug( "Entering HTTPUtils.getHTMLFromURL() with: [" + url + "]" );

        StringBuilder htmlMarkUp = new StringBuilder();
        URLConnection connection = makeConnection( url );

        // Read html mark up
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader( connection.getInputStream(), StandardCharsets.UTF_8 ) );)
        {
            String line = reader.readLine();

            while ( line != null )
            {
                htmlMarkUp.append( line );
                line = reader.readLine();
            }
        }

        logger.debug( "Exiting HTTPUtils.getHTMLFromURL() with: []" );
        return htmlMarkUp;
    }

    /**
     * Scans a string and gets rid off HTML like characters
     * @param rawString Raw string which needs to be cleaned
     * @param isUnicode Are Unicode characters present?
     * @param isXML Are XML characters present?
     * @param isHTML Are HTML characters present?
     * @return String with no HTML elements in it
     */
    public static String convertHTMLToString( String rawString, boolean isUnicode, boolean isXML, boolean isHTML )
    {
        logger.debug( "Entering HTTPUtils.convertHTMLToString() with: [" + rawString + ", " + isUnicode + ", " + isXML + ", " + isHTML
                + "]" );

        if ( isUnicode )
        {
            // detects stuff like unicode characters
            rawString = StringEscapeUtils.unescapeJava( rawString.replace( "\\x", "\\u00" ) );
        }

        if ( isXML )
        {
            // detects stuff like %gt;
            rawString = StringEscapeUtils.unescapeHtml4( rawString );
        }

        if ( isHTML )
        {
            // detects stuff like <b>
            rawString = Jsoup.parse( rawString ).text();
        }

        logger.debug( "Exiting HTTPUtils.convertHTMLToString() with: [" + rawString + "]" );
        return rawString;
    }

    /**
     * Initializes log4j configurations
     */
    private static void init()
    {
        DOMConfigurator.configure( "log4j.xml" );
    }
}
